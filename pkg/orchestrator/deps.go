package orchestrator

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"minisky/pkg/config"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Dependency defines a system tool required by a MiniSky service.
type Dependency struct {
	ID          string
	Name        string
	Description string
	DownloadURL string
}

const (
	KindVersion = "v0.22.0"
	PackVersion = "v0.34.2"

	dependencyInstallTimeout = 2 * time.Minute
	maxDependencySize        = 256 << 20
	maxChecksumSize          = 4 << 10
)

type archiveFormat uint8

const (
	archiveNone archiveFormat = iota
	archiveTarGzip
	archiveZip
)

type dependencySpec struct {
	id          string
	name        string
	downloadURL string
	checksumURL string
	archive     archiveFormat
}

type dependencyInstaller struct {
	client *http.Client
	binDir string
	goos   string
	goarch string
}

// InstallToolDependency securely installs kind or pack without initializing Docker.
func InstallToolDependency(ctx context.Context, id string) error {
	installer := dependencyInstaller{
		client: &http.Client{Timeout: dependencyInstallTimeout},
		binDir: GetLocalBinPath(),
		goos:   runtime.GOOS,
		goarch: runtime.GOARCH,
	}
	spec, err := installer.resolve(id)
	if err != nil {
		return err
	}
	return installer.install(ctx, spec)
}

// InstallDependency installs a tool dependency or pulls a requested Docker image.
func (sm *ServiceManager) InstallDependency(id string) error {
	if strings.HasPrefix(id, "docker-image:") {
		image := strings.TrimPrefix(id, "docker-image:")
		log.Printf("[Deps] Pulling docker image: %s", image)
		return sm.pullImageInternal(image)
	}
	ctx, cancel := context.WithTimeout(context.Background(), dependencyInstallTimeout)
	defer cancel()
	return InstallToolDependency(ctx, id)
}

func (installer dependencyInstaller) resolve(id string) (dependencySpec, error) {
	goos := installer.goos
	if goos == "" {
		goos = runtime.GOOS
	}
	arch := installer.goarch
	if arch == "" {
		arch = runtime.GOARCH
	}
	switch arch {
	case "x86_64":
		arch = "amd64"
	case "aarch64":
		arch = "arm64"
	}

	switch id {
	case "kind":
		supported := (goos == "darwin" || goos == "linux") && (arch == "amd64" || arch == "arm64")
		supported = supported || (goos == "windows" && arch == "amd64")
		if !supported {
			return dependencySpec{}, fmt.Errorf("kind %s does not provide a %s/%s release asset", KindVersion, goos, arch)
		}
		asset := fmt.Sprintf("kind-%s-%s", goos, arch)
		base := fmt.Sprintf("https://github.com/kubernetes-sigs/kind/releases/download/%s/%s", KindVersion, asset)
		return dependencySpec{
			id: id, name: executableName(id, goos), downloadURL: base, checksumURL: base + ".sha256sum",
		}, nil
	case "pack":
		var platform, extension string
		switch {
		case goos == "linux" && arch == "amd64":
			platform, extension = "linux", "tgz"
		case goos == "linux" && (arch == "arm64" || arch == "ppc64le" || arch == "s390x"):
			platform, extension = "linux-"+arch, "tgz"
		case goos == "darwin" && arch == "amd64":
			platform, extension = "macos", "tgz"
		case goos == "darwin" && arch == "arm64":
			platform, extension = "macos-arm64", "tgz"
		case goos == "windows" && arch == "amd64":
			platform, extension = "windows", "zip"
		default:
			return dependencySpec{}, fmt.Errorf("pack %s does not provide a %s/%s release asset", PackVersion, goos, arch)
		}
		asset := fmt.Sprintf("pack-%s-%s.%s", PackVersion, platform, extension)
		base := fmt.Sprintf("https://github.com/buildpacks/pack/releases/download/%s/%s", PackVersion, asset)
		format := archiveTarGzip
		if extension == "zip" {
			format = archiveZip
		}
		return dependencySpec{
			id: id, name: executableName(id, goos), downloadURL: base, checksumURL: base + ".sha256", archive: format,
		}, nil
	default:
		return dependencySpec{}, fmt.Errorf("unsupported tool dependency: %s", id)
	}
}

func (installer dependencyInstaller) install(ctx context.Context, spec dependencySpec) error {
	if installer.client == nil {
		return fmt.Errorf("dependency installer requires an HTTP client")
	}
	if installer.binDir == "" {
		return fmt.Errorf("dependency installer requires an install directory")
	}

	log.Printf("[Deps] Downloading %s from %s...", spec.id, spec.downloadURL)
	download, err := installer.download(ctx, spec.downloadURL, maxDependencySize)
	if err != nil {
		return fmt.Errorf("download %s: %w", spec.id, err)
	}
	checksum, err := installer.download(ctx, spec.checksumURL, maxChecksumSize)
	if err != nil {
		return fmt.Errorf("download %s checksum: %w", spec.id, err)
	}
	if err := verifyChecksum(download, checksum); err != nil {
		return fmt.Errorf("verify %s checksum: %w", spec.id, err)
	}

	executable := download
	switch spec.archive {
	case archiveTarGzip:
		executable, err = extractTarGzipExecutable(download, spec.name)
	case archiveZip:
		executable, err = extractZipExecutable(download, spec.name)
	}
	if err != nil {
		return fmt.Errorf("extract %s: %w", spec.id, err)
	}
	if len(executable) == 0 {
		return fmt.Errorf("extract %s: executable is empty", spec.id)
	}

	if err := atomicInstallExecutable(installer.binDir, spec.name, executable); err != nil {
		return fmt.Errorf("install %s: %w", spec.id, err)
	}
	log.Printf("[Deps] Successfully installed %s to %s", spec.id, filepath.Join(installer.binDir, spec.name))
	return nil
}

func (installer dependencyInstaller) download(ctx context.Context, url string, maxSize int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := installer.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %s", resp.Status)
	}
	if resp.ContentLength > maxSize {
		return nil, fmt.Errorf("response is larger than %d bytes", maxSize)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxSize {
		return nil, fmt.Errorf("response is larger than %d bytes", maxSize)
	}
	return data, nil
}

func verifyChecksum(data, checksumFile []byte) error {
	fields := strings.Fields(string(checksumFile))
	if len(fields) == 0 {
		return fmt.Errorf("checksum file is empty")
	}
	expected, err := hex.DecodeString(fields[0])
	if err != nil || len(expected) != sha256.Size {
		return fmt.Errorf("checksum file does not contain a valid SHA-256 digest")
	}
	actual := sha256.Sum256(data)
	if !bytes.Equal(actual[:], expected) {
		return fmt.Errorf("checksum mismatch")
	}
	return nil
}

func extractTarGzipExecutable(data []byte, executableName string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var executable []byte
	var totalSize int64
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if err := validateArchivePath(header.Name); err != nil {
			return nil, err
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return nil, fmt.Errorf("unsafe archive entry %q is not a regular file", header.Name)
		}
		if header.Size < 0 || header.Size > maxDependencySize || totalSize > maxDependencySize-header.Size {
			return nil, fmt.Errorf("archive contents are too large")
		}
		totalSize += header.Size
		if filepath.Base(filepath.FromSlash(header.Name)) != executableName {
			continue
		}
		if executable != nil {
			return nil, fmt.Errorf("archive contains multiple %q executables", executableName)
		}
		executable, err = io.ReadAll(io.LimitReader(tr, maxDependencySize+1))
		if err != nil {
			return nil, err
		}
		if int64(len(executable)) != header.Size {
			return nil, fmt.Errorf("archive executable size mismatch")
		}
	}
	if executable == nil {
		return nil, fmt.Errorf("archive does not contain %q", executableName)
	}
	return executable, nil
}

func extractZipExecutable(data []byte, executableName string) ([]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	var executable []byte
	var totalSize uint64
	for _, file := range zr.File {
		if err := validateArchivePath(file.Name); err != nil {
			return nil, err
		}
		if file.FileInfo().IsDir() {
			continue
		}
		if !file.Mode().IsRegular() {
			return nil, fmt.Errorf("unsafe archive entry %q is not a regular file", file.Name)
		}
		if file.UncompressedSize64 > maxDependencySize || totalSize > maxDependencySize-file.UncompressedSize64 {
			return nil, fmt.Errorf("archive contents are too large")
		}
		totalSize += file.UncompressedSize64
		if filepath.Base(filepath.FromSlash(file.Name)) != executableName {
			continue
		}
		if executable != nil {
			return nil, fmt.Errorf("archive contains multiple %q executables", executableName)
		}
		reader, err := file.Open()
		if err != nil {
			return nil, err
		}
		executable, err = io.ReadAll(io.LimitReader(reader, maxDependencySize+1))
		closeErr := reader.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if uint64(len(executable)) != file.UncompressedSize64 {
			return nil, fmt.Errorf("archive executable size mismatch")
		}
	}
	if executable == nil {
		return nil, fmt.Errorf("archive does not contain %q", executableName)
	}
	return executable, nil
}

func validateArchivePath(name string) error {
	clean := filepath.Clean(filepath.FromSlash(name))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return fmt.Errorf("unsafe archive path %q", name)
	}
	return nil
}

func atomicInstallExecutable(binDir, name string, data []byte) error {
	if filepath.Base(name) != name || name == "." {
		return fmt.Errorf("invalid executable name %q", name)
	}
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return fmt.Errorf("create bin directory: %w", err)
	}
	temp, err := os.CreateTemp(binDir, "."+name+".tmp-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Chmod(0755); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, filepath.Join(binDir, name)); err != nil {
		return err
	}
	return nil
}

func executableName(id, goos string) string {
	if goos == "windows" {
		return id + ".exe"
	}
	return id
}

// GetLocalBinPath returns the absolute path to the local .minisky/bin folder.
func GetLocalBinPath() string {
	return filepath.Join(config.GetMiniskyDir(), "bin")
}

// GetKindBinaryName returns "kind" or "kind.exe" depending on the OS.
func GetKindBinaryName() string {
	if runtime.GOOS == "windows" {
		return "kind.exe"
	}
	return "kind"
}

// GetPackBinaryName returns "pack" or "pack.exe" depending on the OS.
func GetPackBinaryName() string {
	if runtime.GOOS == "windows" {
		return "pack.exe"
	}
	return "pack"
}
