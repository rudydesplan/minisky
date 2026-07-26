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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
)

// ClusterIdentity uniquely scopes an owned Kind backend.
type ClusterIdentity struct {
	Profile string
	Project string
	Zone    string
	Cluster string
}

type retryableUnavailableError struct{ message string }

func (err retryableUnavailableError) Error() string      { return err.message }
func (err retryableUnavailableError) OperationCode() int { return http.StatusServiceUnavailable }

type backendUnavailableCause struct {
	operation string
	err       error
}

func (err *backendUnavailableCause) Error() string {
	return fmt.Sprintf("%s unavailable: %v", err.operation, err.err)
}

func (err *backendUnavailableCause) Unwrap() error { return err.err }

func isBackendUnavailable(err error) bool {
	var unavailable *backendUnavailableCause
	return errors.As(err, &unavailable)
}

func (i ClusterIdentity) canonical() (string, error) {
	parts := []string{i.Profile, i.Project, i.Zone, i.Cluster}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("invalid GKE cluster identity")
		}
		for _, r := range part {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' ||
				r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
				return "", fmt.Errorf("invalid GKE cluster identity")
			}
		}
	}
	return strings.Join(parts, "/"), nil
}

// KindBackend drives real Kind cluster lifecycle.
type KindBackend struct {
	mu                            sync.RWMutex
	enabled                       bool
	pendingClusters               sync.Map
	kubeconfigOwners              sync.Map
	nameLocksMu                   sync.Mutex
	nameLocks                     map[string]*sync.Mutex
	status                        config.BackendState
	testConfigureKubeconfigTarget func(*secureKubeconfigTarget)
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
func (k *KindBackend) Enabled() bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.enabled
}

// Status reports the effective backend selected after dependency checks.
func (k *KindBackend) Status() config.BackendState {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.status
}

// SetEnabled toggles the Kind backend dynamically.
func (k *KindBackend) SetEnabled(enabled bool) error {
	if enabled {
		if missing := missingKindDependencies(); len(missing) > 0 {
			k.mu.Lock()
			k.status = config.BackendState{
				Profile: config.GetRuntimeProfile().Name, Backend: config.RuntimeProfileSimulation,
				Source: "dashboard", Diagnostic: fmt.Sprintf("Kind dependencies missing (%s); using simulation", strings.Join(missing, ", ")),
			}
			k.enabled = false
			k.mu.Unlock()
			return fmt.Errorf("missing Kind dependencies: %s", strings.Join(missing, ", "))
		}
		k.mu.Lock()
		k.status = config.BackendState{
			Profile: config.GetRuntimeProfile().Name, Backend: "kind", Enabled: true, Source: "dashboard",
		}
		k.enabled = true
		k.mu.Unlock()
		log.Printf("[KindBackend] dynamically ENABLED via UI")
	} else {
		k.mu.Lock()
		k.status = config.BackendState{
			Profile: config.GetRuntimeProfile().Name, Backend: config.RuntimeProfileSimulation, Source: "dashboard",
		}
		k.enabled = false
		k.mu.Unlock()
		log.Printf("[KindBackend] dynamically DISABLED via UI")
	}
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

// CreateCluster preserves the dashboard-facing backend API.
func (k *KindBackend) CreateCluster(identity ClusterIdentity) (string, error) {
	result, err := k.CreateClusterContext(context.Background(), identity)
	if err != nil {
		return "", err
	}
	if !result.Created {
		return "", nil
	}
	return kindKubeconfigPath(identity), nil
}

// CreateClusterContext runs `kind create cluster --name <name>` with cancellation.
func (k *KindBackend) CreateClusterContext(ctx context.Context, identity ClusterIdentity) (gkeBackendCreateResult, error) {
	if err := secureKubeconfigPlatformCheck(); err != nil {
		return gkeBackendCreateResult{}, err
	}
	if !k.Enabled() {
		return gkeBackendCreateResult{}, fmt.Errorf("kind backend not enabled")
	}

	logicalName, err := kindBackendName(identity)
	if err != nil {
		return gkeBackendCreateResult{}, err
	}
	unlock := k.lockName(logicalName)
	defer unlock()
	temporaryKubeconfig, ownership, err := prepareKubeconfigWithIntent(identity)
	if err != nil {
		return gkeBackendCreateResult{}, fmt.Errorf("prepare durable kubeconfig ownership intent: %w", err)
	}
	kindName := ownership.BackendName
	k.pendingClusters.Store(kindName, true)
	defer k.pendingClusters.Delete(kindName)
	terminalizePrepared := func(cause error) error {
		cleanupErr := secureDiscardKubeconfig(temporaryKubeconfig)
		if cleanupErr == nil {
			cleanupErr = writeKubeconfigIntent(identity, ownership, intentTerminal)
		} else {
			cleanupErr = errors.Join(cleanupErr,
				writeKubeconfigIntentError(identity, ownership, intentPrepared, cleanupErr.Error()))
		}
		return errors.Join(cause, cleanupErr)
	}
	exists, err := kindClusterExists(ctx, kindName)
	if err != nil {
		return gkeBackendCreateResult{Ownership: ownership},
			terminalizePrepared(fmt.Errorf("verify nonce backend absence: %w", err))
	}
	if exists {
		return gkeBackendCreateResult{Ownership: ownership},
			terminalizePrepared(fmt.Errorf("nonce backend collision %q", kindName))
	}
	if err := writeKubeconfigIntent(identity, ownership, intentCreateStarted); err != nil {
		return gkeBackendCreateResult{Ownership: ownership},
			terminalizePrepared(fmt.Errorf("persist CREATE_STARTED intent: %w", err))
	}
	if k.testConfigureKubeconfigTarget != nil {
		k.testConfigureKubeconfigTarget(temporaryKubeconfig)
	}
	abort := func(cause error) error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cleanupErr := k.cleanupOwnedCreateAttempt(cleanupCtx, identity, ownership, temporaryKubeconfig)
		if cleanupErr != nil {
			phaseErr := writeKubeconfigIntentError(
				identity, ownership, intentCleanupPending, cleanupErr.Error())
			var closeErr error
			if temporaryKubeconfig.file != nil {
				closeErr = errors.Join(closeErr, temporaryKubeconfig.file.Close())
				temporaryKubeconfig.file = nil
			}
			if temporaryKubeconfig.dir != nil {
				closeErr = errors.Join(closeErr, temporaryKubeconfig.dir.Close())
				temporaryKubeconfig.dir = nil
			}
			return errors.Join(cause, cleanupErr, phaseErr, closeErr)
		}
		return errors.Join(cause, writeKubeconfigIntent(identity, ownership, intentTerminal))
	}

	log.Printf("[KindBackend] Creating cluster: %s (owned kind name: %s)", identity.Cluster, kindName)
	kubeconfigPath := kindKubeconfigPath(identity)

	binPath := orchestrator.GetKindBinaryName()
	localKind := filepath.Join(orchestrator.GetLocalBinPath(), binPath)
	if _, err := os.Stat(localKind); err == nil {
		binPath = localKind
	}

	cmd := exec.CommandContext(ctx, binPath, "create", "cluster")
	commandKubeconfig, err := secureKubeconfigCommandPath(temporaryKubeconfig, cmd)
	if err != nil {
		return gkeBackendCreateResult{}, abort(fmt.Errorf("secure Kind kubeconfig target: %w", err))
	}
	cmd.Args = append(cmd.Args,
		"--name", kindName,
		"--kubeconfig", commandKubeconfig,
		"--wait", "120s", // wait up to 2 minutes for control plane
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return gkeBackendCreateResult{Ownership: ownership},
			abort(fmt.Errorf("kind create cluster failed: %w", err))
	}
	if err := writeKubeconfigIntent(identity, ownership, intentBackendCreated); err != nil {
		return gkeBackendCreateResult{Ownership: ownership},
			abort(fmt.Errorf("persist BACKEND_CREATED intent: %w", err))
	}

	log.Printf("[KindBackend] ✅ Cluster '%s' ready. Kubeconfig: %s", kindName, kubeconfigPath)

	// Attach the nodes before reporting success so network failures become
	// terminal create errors and the caller can clean up the partial cluster.
	log.Printf("[KindBackend] Linking cluster '%s' nodes to minisky-net...", kindName)
	out, err := exec.CommandContext(ctx, "docker", "ps", "--format", "{{.Names}}", "--filter", "name=^"+kindName+"-").Output()
	if err != nil {
		return gkeBackendCreateResult{Ownership: ownership},
			abort(fmt.Errorf("list Kind node containers: %w", err))
	}
	containers := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, cName := range containers {
		if cName == "" {
			continue
		}
		if output, err := exec.CommandContext(ctx, "docker", "network", "connect", "minisky-net", cName).CombinedOutput(); err != nil {
			return gkeBackendCreateResult{Ownership: ownership}, abort(fmt.Errorf(
				"connect Kind node %s to minisky-net: %w (%s)",
				cName, err, strings.TrimSpace(string(output))))
		}
		log.Printf("[KindBackend] Node '%s' connected to minisky-net", cName)
	}
	if err := securePublishKubeconfig(temporaryKubeconfig, kubeconfigPath); err != nil {
		return gkeBackendCreateResult{Ownership: ownership},
			abort(fmt.Errorf("publish kubeconfig: %w", err))
	}
	k.kubeconfigOwners.Store(logicalName, ownership)

	return gkeBackendCreateResult{Created: true, Ownership: ownership}, nil
}

func (k *KindBackend) CommitKubeconfigIntent(identity ClusterIdentity, ownership *kubeconfigOwnership) error {
	if !ownership.hasBackendNonce() {
		return fmt.Errorf("refusing legacy backend ownership without nonce evidence")
	}
	return writeKubeconfigIntent(identity, ownership, intentCommitted)
}

func (k *KindBackend) cleanupOwnedCreateAttempt(
	ctx context.Context,
	identity ClusterIdentity,
	ownership *kubeconfigOwnership,
	target *secureKubeconfigTarget,
) error {
	if ownership == nil || !ownership.matchesIdentity(identity) || !ownership.hasBackendNonce() {
		return fmt.Errorf("refusing cleanup without durable nonce ownership")
	}
	if !k.Enabled() {
		return fmt.Errorf("owned backend cleanup pending: kind backend disabled")
	}
	exists, err := kindClusterExists(ctx, ownership.BackendName)
	if err != nil {
		return fmt.Errorf("prove owned backend absence: %w", err)
	}
	if exists {
		if err := deleteKindClusterByName(ctx, ownership.BackendName); err != nil {
			return fmt.Errorf("delete owned backend %q: %w", ownership.BackendName, err)
		}
		exists, err = kindClusterExists(ctx, ownership.BackendName)
		if err != nil {
			return fmt.Errorf("prove owned backend deletion: %w", err)
		}
		if exists {
			return fmt.Errorf("prove owned backend deletion: backend still exists")
		}
	}
	if target != nil {
		if err := secureDiscardKubeconfig(target); err != nil {
			return fmt.Errorf("zeroize failed create kubeconfig: %w", err)
		}
	} else if err := secureQuarantineOwnedKubeconfig(
		kindKubeconfigPath(identity), ownership,
	); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("zeroize failed create kubeconfig: %w", err)
	}
	return nil
}

func deleteKindClusterByName(ctx context.Context, name string) error {
	_, err := runKindCommand(ctx, "delete", "cluster", "--name", name)
	return err
}

func runKindCommand(ctx context.Context, args ...string) ([]byte, error) {
	binPath := orchestrator.GetKindBinaryName()
	localKind := filepath.Join(orchestrator.GetLocalBinPath(), binPath)
	if _, err := os.Stat(localKind); err == nil {
		binPath = localKind
	}
	output, err := exec.CommandContext(ctx, binPath, args...).CombinedOutput()
	if err == nil {
		return output, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return output, ctxErr
	}
	if errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
		return output, &backendUnavailableCause{operation: "Kind executable", err: err}
	}
	message := strings.ToLower(string(output))
	if !strings.Contains(message, "permission denied") &&
		(strings.Contains(message, "cannot connect to the docker daemon") ||
			strings.Contains(message, "is the docker daemon running") ||
			strings.Contains(message, "docker daemon is not running") ||
			strings.Contains(message, "connect: connection refused") ||
			strings.Contains(message, "docker.sock: connect: no such file or directory") ||
			strings.Contains(message, "error during connect:") ||
			strings.Contains(message, "failed to connect to docker") ||
			strings.Contains(message, `pipe/docker_engine`) && strings.Contains(message, "cannot find")) {
		return output, &backendUnavailableCause{
			operation: "Docker backend", err: fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output))),
		}
	}
	return output, fmt.Errorf("kind %s failed: %w (%s)",
		strings.Join(args, " "), err, strings.TrimSpace(string(output)))
}

// DeleteCluster preserves the dashboard-facing backend API.
func (k *KindBackend) DeleteCluster(identity ClusterIdentity) error {
	return k.DeleteClusterContext(context.Background(), identity)
}

// DeleteClusterContext runs `kind delete cluster --name <name>` with cancellation.
func (k *KindBackend) DeleteClusterContext(ctx context.Context, identity ClusterIdentity) error {
	if err := secureKubeconfigPlatformCheck(); err != nil {
		return err
	}
	logicalName, err := kindBackendName(identity)
	if err != nil {
		return err
	}
	rawOwnership, ok := k.kubeconfigOwners.Load(logicalName)
	ownership, _ := rawOwnership.(*kubeconfigOwnership)
	if !ok || !ownership.matchesIdentity(identity) || !ownership.hasBackendNonce() {
		return fmt.Errorf("refusing cluster deletion without trusted nonce backend ownership")
	}
	if !k.Enabled() {
		unavailable := retryableUnavailableError{
			message: "UNAVAILABLE: Kind backend cleanup is temporarily unavailable; retry deletion later",
		}
		return errors.Join(unavailable, k.persistDeleteUnavailable(identity, ownership, unavailable.Error()))
	}
	unlock := k.lockName(logicalName)
	defer unlock()
	exists, err := kindClusterExists(ctx, ownership.BackendName)
	if err != nil {
		return k.classifyDeleteFailure(identity, ownership, err)
	}
	kubeconfigPath := kindKubeconfigPath(identity)
	if exists {
		if err := deleteKindClusterByName(ctx, ownership.BackendName); err != nil {
			return k.classifyDeleteFailure(identity, ownership, err)
		}
		exists, err = kindClusterExists(ctx, ownership.BackendName)
		if err != nil {
			return k.classifyDeleteFailure(
				identity, ownership, fmt.Errorf("prove backend absence after delete: %w", err))
		}
		if exists {
			return fmt.Errorf("prove backend absence after delete: backend still exists")
		}
	}
	if err := secureQuarantineOwnedKubeconfig(kubeconfigPath, ownership); err != nil {
		return fmt.Errorf("quarantine kubeconfig: %w", err)
	}
	return writeKubeconfigIntent(identity, ownership, intentDeleteCleaned)
}

func (k *KindBackend) CheckDeleteAvailability(
	ctx context.Context,
	identity ClusterIdentity,
) error {
	logicalName, err := kindBackendName(identity)
	if err != nil {
		return err
	}
	raw, ok := k.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	if !ok || !ownership.matchesIdentity(identity) || !ownership.hasBackendNonce() {
		return fmt.Errorf("refusing availability check without trusted nonce ownership")
	}
	if !k.Enabled() {
		return &backendUnavailableCause{
			operation: "Kind backend", err: errors.New("backend disabled"),
		}
	}
	_, err = kindClusterExists(ctx, ownership.BackendName)
	return err
}

func (k *KindBackend) classifyDeleteFailure(
	identity ClusterIdentity,
	ownership *kubeconfigOwnership,
	cause error,
) error {
	if !isBackendUnavailable(cause) {
		return cause
	}
	unavailable := retryableUnavailableError{
		message: "UNAVAILABLE: Kind backend cleanup is temporarily unavailable; retry deletion later",
	}
	return errors.Join(
		unavailable,
		cause,
		k.persistDeleteUnavailable(identity, ownership, unavailable.Error()+": "+cause.Error()))
}

func (k *KindBackend) MarkDeleteUnavailable(identity ClusterIdentity) error {
	logicalName, err := kindBackendName(identity)
	if err != nil {
		return err
	}
	raw, ok := k.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	if !ok || !ownership.matchesIdentity(identity) || !ownership.hasBackendNonce() {
		return fmt.Errorf("refusing unavailable delete without trusted nonce ownership")
	}
	return k.persistDeleteUnavailable(
		identity, ownership,
		"UNAVAILABLE: Kind backend cleanup is temporarily unavailable; retry deletion later")
}

func (k *KindBackend) persistDeleteUnavailable(
	identity ClusterIdentity,
	ownership *kubeconfigOwnership,
	message string,
) error {
	return writeKubeconfigIntentError(identity, ownership, intentDeletePending, message)
}

func (k *KindBackend) FinalizeDeleteIntent(
	identity ClusterIdentity,
	ownership *kubeconfigOwnership,
) error {
	if !ownership.matchesIdentity(identity) || !ownership.hasBackendNonce() {
		return fmt.Errorf("refusing to terminalize legacy deletion intent")
	}
	if err := writeKubeconfigIntent(identity, ownership, intentTerminal); err != nil {
		return err
	}
	if logicalName, err := kindBackendName(identity); err == nil {
		k.kubeconfigOwners.Delete(logicalName)
	}
	return nil
}

func (k *KindBackend) CleanupClusterContext(ctx context.Context, identity ClusterIdentity) error {
	return fmt.Errorf("refusing cleanup without durable nonce ownership")
}

func (k *KindBackend) RestoreKubeconfigOwnership(identity ClusterIdentity, ownership *kubeconfigOwnership) {
	if ownership == nil || !ownership.matchesIdentity(identity) || !ownership.hasBackendNonce() {
		return
	}
	if name, err := kindBackendName(identity); err == nil {
		k.kubeconfigOwners.Store(name, ownership)
	}
}

func (k *KindBackend) ReconcileKubeconfigIntents(
	ctx context.Context,
	profile string,
	durable map[string]*kubeconfigOwnership,
) error {
	if err := secureKubeconfigPlatformCheck(); err != nil {
		return nil
	}
	intents, err := loadAllKubeconfigIntents(profile)
	if err != nil {
		return err
	}
	var reconcileErr error
	for _, intent := range intents {
		ownership := intent.Ownership
		identity := ClusterIdentity{
			Profile: ownership.Profile, Project: ownership.Project,
			Zone: ownership.Zone, Cluster: ownership.Cluster,
		}
		key := clusterKey(identity.Project, identity.Zone, identity.Cluster)
		if !ownership.hasBackendNonce() {
			reconcileErr = errors.Join(reconcileErr,
				fmt.Errorf("legacy GKE intent %s lacks backend nonce; explicit adoption or manual cleanup required", key))
			continue
		}
		if persisted := durable[key]; persisted != nil &&
			persisted.matchesIdentity(identity) &&
			persisted.Device == ownership.Device && persisted.Inode == ownership.Inode &&
			persisted.BackendName == ownership.BackendName {
			k.RestoreKubeconfigOwnership(identity, persisted)
			if intent.Phase == intentDeletePending || intent.Phase == intentDeleteCleaned {
				continue
			}
			if intent.Phase != intentCommitted {
				reconcileErr = errors.Join(reconcileErr,
					writeKubeconfigIntent(identity, persisted, intentCommitted))
			}
			continue
		}
		if intent.Phase == intentTerminal {
			continue
		}
		if intent.Phase == intentDeleteCleaned {
			reconcileErr = errors.Join(reconcileErr,
				writeKubeconfigIntent(identity, ownership, intentTerminal))
			continue
		}
		if intent.Phase == intentPrepared {
			err := secureQuarantineOwnedKubeconfig(kindKubeconfigPath(identity), ownership)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				reconcileErr = errors.Join(reconcileErr, err,
					writeKubeconfigIntentError(identity, ownership, intentPrepared, err.Error()))
				continue
			}
			reconcileErr = errors.Join(reconcileErr,
				writeKubeconfigIntent(identity, ownership, intentTerminal))
			continue
		}
		cleanupErr := k.cleanupOwnedCreateAttempt(ctx, identity, ownership, nil)
		if cleanupErr != nil {
			reconcileErr = errors.Join(reconcileErr, cleanupErr,
				writeKubeconfigIntentError(
					identity, ownership, intentCleanupPending, cleanupErr.Error()))
			continue
		}
		reconcileErr = errors.Join(reconcileErr, writeKubeconfigIntent(identity, ownership, intentTerminal))
	}
	return reconcileErr
}

func (k *KindBackend) lockName(name string) func() {
	k.nameLocksMu.Lock()
	if k.nameLocks == nil {
		k.nameLocks = make(map[string]*sync.Mutex)
	}
	lock := k.nameLocks[name]
	if lock == nil {
		lock = &sync.Mutex{}
		k.nameLocks[name] = lock
	}
	k.nameLocksMu.Unlock()
	lock.Lock()
	return lock.Unlock
}

func kindKubeconfigPath(identity ClusterIdentity) string {
	name, _ := kindBackendName(identity)
	stateDir := config.GetStateDir()
	if resolved, err := filepath.EvalSymlinks(stateDir); err == nil {
		stateDir = resolved
	}
	return filepath.Join(stateDir, "profiles", identity.Profile, "runtime", "gke", name+".kubeconfig")
}

func legacyKindKubeconfigPath(name string) string {
	runtimeDir := config.GetRuntimeDir()
	if resolved, err := filepath.EvalSymlinks(runtimeDir); err == nil {
		runtimeDir = resolved
	}
	return filepath.Join(runtimeDir, "gke", name+".kubeconfig")
}

func kindBackendName(identity ClusterIdentity) (string, error) {
	canonical, err := identity.canonical()
	if err != nil {
		return "", err
	}
	prefix := sanitizeKindName(identity.Cluster)
	if len(prefix) > 36 {
		prefix = prefix[:36]
	}
	sum := sha256.Sum256([]byte(canonical))
	return fmt.Sprintf("minisky-%s-%x", prefix, sum[:8]), nil
}

// ReadKubeconfig returns only the deterministic file owned by this identity.
func (k *KindBackend) ReadKubeconfig(identity ClusterIdentity) ([]byte, error) {
	if err := secureKubeconfigPlatformCheck(); err != nil {
		return nil, err
	}
	if _, err := identity.canonical(); err != nil {
		return nil, err
	}
	kindName, err := kindBackendName(identity)
	if err != nil {
		return nil, err
	}
	unlock := k.lockName(kindName)
	defer unlock()
	path := kindKubeconfigPath(identity)
	// For clusters created by this backend process, retain the published inode
	// identity so a same-UID pathname replacement is rejected on the next read.
	// After process restart there is no trusted inode continuity; O_NOFOLLOW,
	// regular-file, and permission validation remain the enforceable boundary.
	expected, _ := k.kubeconfigOwners.Load(kindName)
	ownership, _ := expected.(*kubeconfigOwnership)
	if !ownership.matchesIdentity(identity) || !ownership.hasBackendNonce() {
		return nil, fmt.Errorf("refusing kubeconfig read without trusted nonce ownership")
	}
	return secureReadKubeconfigOwnership(path, ownership)
}

func kindClusterExists(ctx context.Context, name string) (bool, error) {
	output, err := runKindCommand(ctx, "get", "clusters")
	if err != nil {
		return false, err
	}
	for _, cluster := range strings.Fields(string(output)) {
		if cluster == name {
			return true, nil
		}
	}
	return false, nil
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
	if !k.Enabled() {
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
