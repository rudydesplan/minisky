package serverless

// ─────────────────────────────────────────────────────────────────────────────
// Phase 9b — Google Cloud Buildpacks Integration (Cloud Functions & Cloud Run)
//
// This file wires the Serverless shim to real local container builds using
// Google Cloud Buildpacks (https://buildpacks.io / cloud.google.com/buildpacks).
//
// When enabled via MINISKY_SERVERLESS_BACKEND=buildpacks, function/service
// creation will:
//   1. Download source code (from GCS stub or local path).
//   2. Run `pack build` to create a Docker image using Google Buildpacks.
//   3. Start the resulting container and wire its URL into the function/service.
//
// Prerequisites:
//   - pack CLI: https://buildpacks.io/docs/tools/pack/
//   - Docker daemon running
//   - Google Buildpacks builder: gcr.io/buildpacks/builder:google-22
//
// Enable with: export MINISKY_SERVERLESS_BACKEND=buildpacks
// ─────────────────────────────────────────────────────────────────────────────

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
)

// BuildpacksBackend manages local container builds via Google Cloud Buildpacks.
type BuildpacksBackend struct {
	enabled   bool
	requested bool
	builder   string // Buildpacks builder image to use
	logStore  map[string]*bytes.Buffer
	logMu     sync.RWMutex
	status    config.BackendState
}

// DefaultBuilder is the Google-24 stack builder that mirrors GCP's latest build environment.
const DefaultBuilder = "gcr.io/buildpacks/builder:google-24"

// NewBuildpacksBackend returns a backend selected by the runtime profile or an
// explicit MINISKY_SERVERLESS_BACKEND override.
func NewBuildpacksBackend() *BuildpacksBackend {
	selection := config.ResolveBackend("MINISKY_SERVERLESS_BACKEND", "buildpacks")
	builder := os.Getenv("MINISKY_BUILDPACKS_BUILDER")
	if builder == "" {
		builder = DefaultBuilder
	}

	b := &BuildpacksBackend{
		enabled:   selection.Requested,
		requested: selection.Requested,
		builder:   builder,
		logStore:  make(map[string]*bytes.Buffer),
		status:    selection.Effective(selection.Requested, ""),
	}

	if selection.Requested {
		if missing := missingBuildpacksDependencies(); len(missing) > 0 {
			diagnostic := fmt.Sprintf("Buildpacks dependencies missing (%s); using simulation", strings.Join(missing, ", "))
			log.Printf("[Buildpacks] WARNING: %s", diagnostic)
			b.enabled = false
			b.status = selection.Effective(false, diagnostic)
		}

		if b.enabled {
			log.Printf("[Buildpacks] ✅ Buildpacks integration ENABLED (builder: %s)", builder)
		}
	} else if b.status.Diagnostic != "" {
		log.Printf("[Buildpacks] WARNING: %s", b.status.Diagnostic)
	}
	return b
}

// Enabled reports whether Buildpacks backend is active.
func (b *BuildpacksBackend) Enabled() bool { return b.enabled }

// Requested reports whether the runtime profile or explicit override requires
// executable Buildpacks behavior, even if a dependency check prevented startup.
func (b *BuildpacksBackend) Requested() bool { return b.requested }

// Status reports the effective backend selected after dependency checks.
func (b *BuildpacksBackend) Status() config.BackendState { return b.status }

// SetEnabled toggles the Buildpacks backend dynamically.
func (b *BuildpacksBackend) SetEnabled(enabled bool) error {
	if enabled {
		if missing := missingBuildpacksDependencies(); len(missing) > 0 {
			b.enabled = false
			b.status = config.BackendState{
				Profile: config.GetRuntimeProfile().Name, Backend: config.RuntimeProfileSimulation,
				Source: "dashboard", Diagnostic: fmt.Sprintf("Buildpacks dependencies missing (%s); using simulation", strings.Join(missing, ", ")),
			}
			return fmt.Errorf("missing Buildpacks dependencies: %s", strings.Join(missing, ", "))
		}
		b.enabled = true
		b.requested = true
		b.status = config.BackendState{
			Profile: config.GetRuntimeProfile().Name, Backend: "buildpacks", Enabled: true, Source: "dashboard",
		}
		log.Printf("[Buildpacks] dynamically ENABLED via UI")
	} else {
		b.enabled = false
		b.requested = false
		b.status = config.BackendState{
			Profile: config.GetRuntimeProfile().Name, Backend: config.RuntimeProfileSimulation, Source: "dashboard",
		}
		log.Printf("[Buildpacks] dynamically DISABLED via UI")
	}
	return nil
}

func missingBuildpacksDependencies() []string {
	var missing []string
	packName := orchestrator.GetPackBinaryName()
	localPack := filepath.Join(orchestrator.GetLocalBinPath(), packName)
	if _, err := os.Stat(localPack); err != nil {
		if _, err := exec.LookPath(packName); err != nil {
			missing = append(missing, "pack")
		}
	}
	if _, err := exec.LookPath("docker"); err != nil {
		missing = append(missing, "docker")
	}
	return missing
}

func (b *BuildpacksBackend) GetLogs(name string) string {
	b.logMu.RLock()
	defer b.logMu.RUnlock()
	if buf, ok := b.logStore[name]; ok {
		return buf.String()
	}
	return ""
}

// BuildFunction builds a Docker image for a Cloud Function source directory.
func (b *BuildpacksBackend) BuildFunction(functionName, sourcePath, entryPoint string) (imageRef string, err error) {
	if !b.enabled {
		return "", fmt.Errorf("buildpacks backend not enabled")
	}

	imageRef = fmt.Sprintf("minisky-fn-%s:local", sanitizeImageName(functionName))
	log.Printf("[Buildpacks] Building image %s from %s using builder %s", imageRef, sourcePath, b.builder)

	binPath := orchestrator.GetPackBinaryName()
	localPack := filepath.Join(orchestrator.GetLocalBinPath(), binPath)
	if _, err := os.Stat(localPack); err == nil {
		binPath = localPack
	}

	// Force shell to see the version and allow internet access for dependencies
	// Crucially, we pass GOOGLE_FUNCTION_TARGET at build time so the buildpacks
	// can generate the correct entrypoint metadata.
	cmd := exec.Command(binPath, "build", imageRef, "--path", sourcePath, "--builder", b.builder,
		"--trust-builder", "--network", "host", "--env", "GOOGLE_FUNCTION_TARGET="+entryPoint,
		"--env", "GOOGLE_FUNCTION_SIGNATURE_TYPE=http")
	cmd.Env = os.Environ()
	if os.Getenv("DOCKER_API_VERSION") == "" {
		cmd.Env = append(cmd.Env, "DOCKER_API_VERSION=1.44")
	}

	buf := new(bytes.Buffer)
	b.logMu.Lock()
	b.logStore[functionName] = buf
	b.logMu.Unlock()
	cmd.Stdout = io.MultiWriter(os.Stdout, buf)
	cmd.Stderr = io.MultiWriter(os.Stderr, buf)

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("pack build failed for '%s': %w", functionName, err)
	}

	log.Printf("[Buildpacks] ✅ Image built: %s", imageRef)
	return imageRef, nil
}

// BuildService builds a Docker image for a Cloud Run service.
func (b *BuildpacksBackend) BuildService(serviceName, sourcePath string) (imageRef string, err error) {
	if !b.enabled {
		return "", fmt.Errorf("buildpacks backend not enabled")
	}

	imageRef = fmt.Sprintf("minisky-svc-%s:local", sanitizeImageName(serviceName))
	log.Printf("[Buildpacks] Building image %s from %s", imageRef, sourcePath)

	binPath := orchestrator.GetPackBinaryName()
	localPack := filepath.Join(orchestrator.GetLocalBinPath(), binPath)
	if _, err := os.Stat(localPack); err == nil {
		binPath = localPack
	}

	cmd := exec.Command(binPath, "build", imageRef, "--path", sourcePath, "--builder", b.builder,
		"--trust-builder", "--network", "host", "--env", "PORT=8080")
	cmd.Env = os.Environ()
	if os.Getenv("DOCKER_API_VERSION") == "" {
		cmd.Env = append(cmd.Env, "DOCKER_API_VERSION=1.44")
	}

	buf := new(bytes.Buffer)
	b.logMu.Lock()
	b.logStore[serviceName] = buf
	b.logMu.Unlock()
	cmd.Stdout = io.MultiWriter(os.Stdout, buf)
	cmd.Stderr = io.MultiWriter(os.Stderr, buf)

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("pack build failed for service '%s': %w", serviceName, err)
	}

	log.Printf("[Buildpacks] ✅ Service image built: %s", imageRef)
	return imageRef, nil
}

// DownloadSourceFromGCS retrieves a zip archive from the storage emulator and extracts it.
func (b *BuildpacksBackend) DownloadSourceFromGCS(bucket, object string) (string, error) {
	tmpDir, err := os.MkdirTemp("", "minisky-source-")
	if err != nil {
		return "", err
	}

	// Hit the GCS emulator (linked to host port 4443)
	url := fmt.Sprintf("http://localhost:4443/storage/v1/b/%s/o/%s?alt=media", bucket, object)
	log.Printf("[Buildpacks] Downloading source from GCS: %s", url)

	cmd := exec.Command("curl", "-L", "-o", tmpDir+"/source.zip", url)
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to download source from GCS: %w", err)
	}

	// Extract
	unzipCmd := exec.Command("unzip", "-q", "-d", tmpDir, tmpDir+"/source.zip")
	unzipCmd.Run() // ignore errors if it wasn't a zip (might be a raw dir if we were fancy)

	return tmpDir, nil
}

func sanitizeImageName(name string) string {
	result := strings.ToLower(name)
	result = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, result)
	return strings.Trim(result, "-")
}
