package orchestrator

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestDependencyInstallerResolvesVerifiedReleaseAssets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		id, goos, goarch string
		wantName         string
		wantURL          string
		wantChecksumURL  string
	}{
		{
			id: "kind", goos: "darwin", goarch: "arm64", wantName: "kind",
			wantURL:         "https://github.com/kubernetes-sigs/kind/releases/download/v0.22.0/kind-darwin-arm64",
			wantChecksumURL: "https://github.com/kubernetes-sigs/kind/releases/download/v0.22.0/kind-darwin-arm64.sha256sum",
		},
		{
			id: "pack", goos: "linux", goarch: "amd64", wantName: "pack",
			wantURL:         "https://github.com/buildpacks/pack/releases/download/v0.40.8/pack-v0.40.8-linux.tgz",
			wantChecksumURL: "https://github.com/buildpacks/pack/releases/download/v0.40.8/pack-v0.40.8-linux.tgz.sha256",
		},
		{
			id: "pack", goos: "windows", goarch: "amd64", wantName: "pack.exe",
			wantURL:         "https://github.com/buildpacks/pack/releases/download/v0.40.8/pack-v0.40.8-windows.zip",
			wantChecksumURL: "https://github.com/buildpacks/pack/releases/download/v0.40.8/pack-v0.40.8-windows.zip.sha256",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.id+"-"+test.goos+"-"+test.goarch, func(t *testing.T) {
			t.Parallel()
			spec, err := (dependencyInstaller{goos: test.goos, goarch: test.goarch}).resolve(test.id)
			if err != nil {
				t.Fatalf("resolve returned an error: %v", err)
			}
			if spec.name != test.wantName || spec.downloadURL != test.wantURL || spec.checksumURL != test.wantChecksumURL {
				t.Fatalf("resolved spec = %#v", spec)
			}
		})
	}
}

func TestDependencyInstallerVerifiesAndAtomicallyInstallsKind(t *testing.T) {
	t.Parallel()

	binary := []byte("verified kind")
	sum := sha256.Sum256(binary)
	client := fixtureClient(t, map[string][]byte{
		"/kind":        binary,
		"/kind.sha256": []byte(fmt.Sprintf("%x  kind\n", sum)),
	})
	binDir := t.TempDir()
	target := filepath.Join(binDir, executableName("kind", runtime.GOOS))
	if err := os.WriteFile(target, []byte("old kind"), 0755); err != nil {
		t.Fatal(err)
	}

	installer := dependencyInstaller{client: client, binDir: binDir}
	spec := dependencySpec{
		id: "kind", name: filepath.Base(target), downloadURL: "https://fixtures.test/kind",
		checksumURL: "https://fixtures.test/kind.sha256",
	}
	if err := installer.install(context.Background(), spec); err != nil {
		t.Fatalf("install returned an error: %v", err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(binary) {
		t.Fatalf("installed content = %q, want %q", got, binary)
	}
	if info, err := os.Stat(target); err != nil || info.Mode()&0100 == 0 {
		t.Fatalf("installed executable mode = %v, err = %v", info.Mode(), err)
	}
}

func TestDependencyInstallerRejectsChecksumMismatchWithoutReplacingTarget(t *testing.T) {
	t.Parallel()

	binDir := t.TempDir()
	target := filepath.Join(binDir, executableName("kind", runtime.GOOS))
	if err := os.WriteFile(target, []byte("existing"), 0755); err != nil {
		t.Fatal(err)
	}
	client := fixtureClient(t, map[string][]byte{
		"/kind":        []byte("tampered"),
		"/kind.sha256": []byte(strings.Repeat("0", 64) + "  kind\n"),
	})
	installer := dependencyInstaller{client: client, binDir: binDir}
	err := installer.install(context.Background(), dependencySpec{
		id: "kind", name: filepath.Base(target), downloadURL: "https://fixtures.test/kind",
		checksumURL: "https://fixtures.test/kind.sha256",
	})
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("error = %v, want checksum failure", err)
	}
	got, readErr := os.ReadFile(target)
	if readErr != nil || string(got) != "existing" {
		t.Fatalf("target changed after failed verification: %q, %v", got, readErr)
	}
}

func TestDependencyInstallerExtractsPackWithoutShellingOut(t *testing.T) {
	t.Parallel()

	archive := tarGzipFixture(t, map[string][]byte{"pack": []byte("pack binary")})
	sum := sha256.Sum256(archive)
	client := fixtureClient(t, map[string][]byte{
		"/pack.tgz":        archive,
		"/pack.tgz.sha256": []byte(fmt.Sprintf("%x  pack.tgz\n", sum)),
	})
	binDir := t.TempDir()
	installer := dependencyInstaller{client: client, binDir: binDir}
	if err := installer.install(context.Background(), dependencySpec{
		id: "pack", name: "pack", downloadURL: "https://fixtures.test/pack.tgz",
		checksumURL: "https://fixtures.test/pack.tgz.sha256", archive: archiveTarGzip,
	}); err != nil {
		t.Fatalf("install returned an error: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(binDir, "pack"))
	if err != nil || string(got) != "pack binary" {
		t.Fatalf("installed pack = %q, %v", got, err)
	}
}

func TestDependencyInstallerRejectsUnsafeArchiveEntry(t *testing.T) {
	t.Parallel()

	archive := tarGzipFixture(t, map[string][]byte{
		"pack":           []byte("pack binary"),
		"../outside.txt": []byte("escape"),
	})
	sum := sha256.Sum256(archive)
	client := fixtureClient(t, map[string][]byte{
		"/pack.tgz":        archive,
		"/pack.tgz.sha256": []byte(fmt.Sprintf("%x", sum)),
	})
	root := t.TempDir()
	binDir := filepath.Join(root, "bin")
	installer := dependencyInstaller{client: client, binDir: binDir}
	err := installer.install(context.Background(), dependencySpec{
		id: "pack", name: "pack", downloadURL: "https://fixtures.test/pack.tgz",
		checksumURL: "https://fixtures.test/pack.tgz.sha256", archive: archiveTarGzip,
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe archive") {
		t.Fatalf("error = %v, want unsafe archive failure", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "outside.txt")); !os.IsNotExist(statErr) {
		t.Fatalf("archive escaped install directory: %v", statErr)
	}
}

func TestDependencyInstallerSupportsZipArchives(t *testing.T) {
	t.Parallel()

	archive := zipFixture(t, "pack.exe", []byte("windows pack"))
	sum := sha256.Sum256(archive)
	client := fixtureClient(t, map[string][]byte{
		"/pack.zip":        archive,
		"/pack.zip.sha256": []byte(fmt.Sprintf("%x", sum)),
	})
	binDir := t.TempDir()
	installer := dependencyInstaller{client: client, binDir: binDir}
	if err := installer.install(context.Background(), dependencySpec{
		id: "pack", name: "pack.exe", downloadURL: "https://fixtures.test/pack.zip",
		checksumURL: "https://fixtures.test/pack.zip.sha256", archive: archiveZip,
	}); err != nil {
		t.Fatalf("install returned an error: %v", err)
	}
}

func TestDependencyInstallerHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := (dependencyInstaller{client: client, binDir: t.TempDir()}).install(ctx, dependencySpec{
		id: "kind", name: "kind", downloadURL: "https://fixtures.test/kind",
		checksumURL: "https://fixtures.test/kind.sha256",
	})
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("error = %v, want context deadline", err)
	}
}

func fixtureClient(t *testing.T, fixtures map[string][]byte) *http.Client {
	t.Helper()
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, ok := fixtures[req.URL.Path]
		if !ok {
			t.Fatalf("unexpected network request: %s", req.URL)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(bytes.NewReader(body)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})}
}

func tarGzipFixture(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, content := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0755, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func zipFixture(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	entry, err := zw.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
