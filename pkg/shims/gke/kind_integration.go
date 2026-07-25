package gke

// ─────────────────────────────────────────────────────────────────────────────
// Phase 6b — Kind Integration
//
// This file wires the GKE shim to a real local Kind (Kubernetes-in-Docker)
// cluster. When enabled via MINISKY_GKE_BACKEND=kind, cluster creation calls
// `kind create cluster` instead of only updating in-memory state.
//
// Prerequisites:
//   - kind CLI in PATH: https://kind.sigs.k8s.io/docs/user/quick-start/
//   - Docker daemon running
//
// Enable with: export MINISKY_GKE_BACKEND=kind
// ─────────────────────────────────────────────────────────────────────────────

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
)

// KindBackend drives real Kind cluster lifecycle.
type KindBackend struct {
	enabled         bool
	pendingClusters sync.Map
	status          config.BackendState
}

// NewKindBackend returns a backend selected by the runtime profile or an
// explicit MINISKY_GKE_BACKEND override.
func NewKindBackend() *KindBackend {
	selection := config.ResolveBackend("MINISKY_GKE_BACKEND", "kind")
	backend := &KindBackend{
		enabled: selection.Requested,
		status:  selection.Effective(selection.Requested, ""),
	}
	if selection.Requested {
		if missing := missingKindDependencies(); len(missing) > 0 {
			diagnostic := fmt.Sprintf("Kind dependencies missing (%s); using simulation", strings.Join(missing, ", "))
			log.Printf("[KindBackend] WARNING: %s", diagnostic)
			backend.enabled = false
			backend.status = selection.Effective(false, diagnostic)
		} else {
			log.Printf("[KindBackend] ✅ Kind integration ENABLED")
		}
	} else if backend.status.Diagnostic != "" {
		log.Printf("[KindBackend] WARNING: %s", backend.status.Diagnostic)
	}
	return backend
}

// Enabled reports whether Kind backend is active.
func (k *KindBackend) Enabled() bool { return k.enabled }

// Status reports the effective backend selected after dependency checks.
func (k *KindBackend) Status() config.BackendState { return k.status }

// SetEnabled toggles the Kind backend dynamically.
func (k *KindBackend) SetEnabled(enabled bool) error {
	if enabled {
		if missing := missingKindDependencies(); len(missing) > 0 {
			k.status = config.BackendState{
				Profile: config.GetRuntimeProfile().Name, Backend: config.RuntimeProfileSimulation,
				Source: "dashboard", Diagnostic: fmt.Sprintf("Kind dependencies missing (%s); using simulation", strings.Join(missing, ", ")),
			}
			return fmt.Errorf("missing Kind dependencies: %s", strings.Join(missing, ", "))
		}
		k.status = config.BackendState{
			Profile: config.GetRuntimeProfile().Name, Backend: "kind", Enabled: true, Source: "dashboard",
		}
		log.Printf("[KindBackend] dynamically ENABLED via UI")
	} else {
		k.status = config.BackendState{
			Profile: config.GetRuntimeProfile().Name, Backend: config.RuntimeProfileSimulation, Source: "dashboard",
		}
		log.Printf("[KindBackend] dynamically DISABLED via UI")
	}
	k.enabled = enabled
	return nil
}

func missingKindDependencies() []string {
	var missing []string
	kindName := orchestrator.GetKindBinaryName()
	localKind := filepath.Join(orchestrator.GetLocalBinPath(), kindName)
	if _, err := os.Stat(localKind); err != nil {
		if _, err := exec.LookPath(kindName); err != nil {
			missing = append(missing, "kind")
		}
	}
	if _, err := exec.LookPath("docker"); err != nil {
		missing = append(missing, "docker")
	}
	return missing
}

// CreateCluster runs `kind create cluster --name <name>`.
// It blocks until the cluster is ready, then returns the kubeconfig path.
func (k *KindBackend) CreateCluster(clusterName string) (kubeconfigPath string, err error) {
	if !k.enabled {
		return "", fmt.Errorf("kind backend not enabled")
	}

	// Track as pending
	kindName := sanitizeKindName(clusterName)
	k.pendingClusters.Store(kindName, true)
	defer k.pendingClusters.Delete(kindName)

	log.Printf("[KindBackend] Creating cluster: %s (kind name: %s)", clusterName, kindName)
	kubeconfigPath = fmt.Sprintf("/tmp/minisky-kubeconfig-%s.yaml", kindName)

	binPath := orchestrator.GetKindBinaryName()
	localKind := filepath.Join(orchestrator.GetLocalBinPath(), binPath)
	if _, err := os.Stat(localKind); err == nil {
		binPath = localKind
	}

	cmd := exec.Command(binPath, "create", "cluster",
		"--name", kindName,
		"--kubeconfig", kubeconfigPath,
		"--wait", "120s", // wait up to 2 minutes for control plane
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("kind create cluster failed: %w", err)
	}

	log.Printf("[KindBackend] ✅ Cluster '%s' ready. Kubeconfig: %s", kindName, kubeconfigPath)

	// Post-provisioning network hook
	// We dynamically attach the containers to minisky-net so they can communicate
	// with Compute VMs and BigQuery.
	log.Printf("[KindBackend] Linking cluster '%s' nodes to minisky-net...", kindName)
	go func() {
		out, err := exec.Command("docker", "ps", "--format", "{{.Names}}", "--filter", "name=^"+kindName+"-").Output()
		if err == nil {
			containers := strings.Split(strings.TrimSpace(string(out)), "\n")
			for _, cName := range containers {
				if cName == "" {
					continue
				}
				// Connect the node to minisky-net so it drops onto the same docker subnet as VMs
				exec.Command("docker", "network", "connect", "minisky-net", cName).Run()
				log.Printf("[KindBackend] Node '%s' connected to minisky-net", cName)
			}
		}
	}()

	return kubeconfigPath, nil
}

// DeleteCluster runs `kind delete cluster --name <name>`.
func (k *KindBackend) DeleteCluster(clusterName string) error {
	if !k.enabled {
		return fmt.Errorf("kind backend not enabled")
	}
	kindName := sanitizeKindName(clusterName)
	binPath := orchestrator.GetKindBinaryName()
	localKind := filepath.Join(orchestrator.GetLocalBinPath(), binPath)
	if _, err := os.Stat(localKind); err == nil {
		binPath = localKind
	}

	cmd := exec.Command(binPath, "delete", "cluster", "--name", kindName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// GetEndpoint returns the Kind cluster's API server endpoint from the kubeconfig.
func (k *KindBackend) GetEndpoint(clusterName string) string {
	return "127.0.0.1"
}

// ClusterInfo represents a kind cluster with status.
type ClusterInfo struct {
	Name   string `json:"name"`
	Status string `json:"status"` // RUNNING, PROVISIONING
}

// ListClusters returns a list of active and pending kind clusters.
func (k *KindBackend) ListClusters() ([]ClusterInfo, error) {
	if !k.enabled {
		return nil, fmt.Errorf("kind backend not enabled")
	}

	binPath := orchestrator.GetKindBinaryName()
	localKind := filepath.Join(orchestrator.GetLocalBinPath(), binPath)
	if _, err := os.Stat(localKind); err == nil {
		binPath = localKind
	}

	out, err := exec.Command(binPath, "get", "clusters").Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list kind clusters: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var clusters []ClusterInfo
	activeNames := make(map[string]bool)

	for _, l := range lines {
		if l != "" && l != "No kind clusters found." {
			clusters = append(clusters, ClusterInfo{Name: l, Status: "RUNNING"})
			activeNames[l] = true
		}
	}

	// Add pending clusters that haven't appeared in 'kind get clusters' yet
	k.pendingClusters.Range(func(key, value interface{}) bool {
		name := key.(string)
		if !activeNames[name] {
			clusters = append(clusters, ClusterInfo{Name: name, Status: "PROVISIONING"})
		}
		return true
	})

	return clusters, nil
}

func sanitizeKindName(name string) string {
	result := strings.ToLower(name)
	result = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, result)
	if len(result) > 63 {
		result = result[:63]
	}
	return strings.Trim(result, "-")
}
