package gke

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

const gkeStateEntry = "gke/metadata"

type gkeStore interface {
	Load(string, any) error
	Save(string, any) error
}

type gkeMetadata struct {
	Backend           string                          `json:"backend"`
	Clusters          map[string]*Cluster             `json:"clusters"`
	Ownerships        map[string]*kubeconfigOwnership `json:"kubeconfigOwnerships,omitempty"`
	Operations        map[string]*GkeOperation        `json:"operations,omitempty"`
	OwnershipChecksum string                          `json:"kubeconfigOwnershipChecksum,omitempty"`
}

func kubeconfigOwnershipChecksum(ownerships map[string]*kubeconfigOwnership) (string, error) {
	data, err := json.Marshal(ownerships)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func NewAPIWithStore(opMgr *orchestrator.OperationManager, store gkeStore) (*API, error) {
	return newAPIWithStoreAndBackend(opMgr, store, NewKindBackend())
}

func newAPIWithStoreAndBackend(
	opMgr *orchestrator.OperationManager,
	store gkeStore,
	backend gkeBackend,
) (*API, error) {
	api := newAPIWithBackend(opMgr, backend, "", nil, store)
	if store == nil {
		return api, nil
	}
	var persisted gkeMetadata
	if err := store.Load(gkeStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load GKE metadata: %w", err)
	}
	validationContext := state.EntryValidationContext{Profile: config.GetProfile()}
	if target, ok := store.(*state.Store); ok {
		validationContext.Store = target
		validationContext.Profile = target.Profile()
	}
	if err := validateGKEMetadataImport(validationContext, &persisted); err != nil {
		return nil, fmt.Errorf("validate GKE metadata: %w", err)
	}
	if persisted.Clusters != nil {
		api.clusters = persisted.Clusters
	}
	if persisted.Ownerships != nil {
		api.ownerships = persisted.Ownerships
	}
	if persisted.Operations != nil {
		api.operations = persisted.Operations
	}
	if backend, ok := api.backend.(*KindBackend); ok {
		for _, ownership := range api.ownerships {
			backend.RestoreKubeconfigOwnership(ClusterIdentity{
				Profile: ownership.Profile, Project: ownership.Project,
				Zone: ownership.Zone, Cluster: ownership.Cluster,
			}, ownership)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := backend.ReconcileKubeconfigIntents(ctx, validationContext.Profile, api.ownerships)
		cancel()
		if err != nil {
			log.Printf("[Shim: GKE] kubeconfig reconciliation remains pending: %v", err)
		}
	}
	changed := false
	const interrupted = "operation interrupted by MiniSky restart; side effects were not replayed"
	for _, operation := range api.operations {
		if operation == nil || operation.Status == "DONE" {
			continue
		}
		operation.Status = "DONE"
		operation.EndTime = time.Now().UTC().Format(time.RFC3339)
		operation.StatusMessage = interrupted
		operation.Error = &orchestrator.OperationError{
			Code: http.StatusInternalServerError, Message: interrupted,
		}
		changed = true
	}
	reconciler, canReconcile := api.backend.(interface {
		ReconcileClusterContext(context.Context, ClusterIdentity, *kubeconfigOwnership) (bool, error)
	})
	for key, cluster := range api.clusters {
		if cluster == nil {
			delete(api.clusters, key)
			changed = true
			continue
		}
		ownership := api.ownerships[key]
		if persisted.Backend == "kind" && ownership != nil && canReconcile {
			identity := ClusterIdentity{
				Profile: ownership.Profile, Project: ownership.Project,
				Zone: ownership.Zone, Cluster: ownership.Cluster,
			}
			exists, err := reconciler.ReconcileClusterContext(
				context.Background(), identity, ownership)
			if err == nil && exists {
				if cluster.Status != "RUNNING" || cluster.StatusMessage != "" {
					cluster.Status = "RUNNING"
					cluster.StatusMessage = ""
					changed = true
				}
				continue
			}
			message := "metadata restored; exactly owned Kind backend is absent"
			if err != nil {
				message = "metadata restored; Kind backend reconciliation failed: " + err.Error()
			}
			degradeRestoredCluster(cluster, message)
			changed = true
			continue
		}
		degradeRestoredCluster(
			cluster,
			"metadata restored; backend availability was not reconciled after restart",
		)
		changed = true
	}
	if changed {
		if err := api.persistMetadata(); err != nil {
			return nil, fmt.Errorf("persist GKE restart reconciliation: %w", err)
		}
	}
	return api, nil
}

func degradeRestoredCluster(cluster *Cluster, message string) {
	cluster.Status = "ERROR"
	cluster.StatusMessage = message
	cluster.Endpoint = ""
	cluster.MasterAuth = nil
}

func validateGKEMetadataImport(context state.EntryValidationContext, metadata *gkeMetadata) error {
	if metadata == nil {
		return errors.New("metadata is null")
	}
	switch metadata.Backend {
	case "", "simulation", "kind":
	default:
		return fmt.Errorf("unsupported backend %q", metadata.Backend)
	}
	for key, cluster := range metadata.Clusters {
		parts := strings.Split(key, ":")
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return fmt.Errorf("invalid cluster slot %q", key)
		}
		if cluster == nil {
			return fmt.Errorf("cluster slot %q is null", key)
		}
		if cluster.Name != parts[2] {
			return fmt.Errorf("cluster slot %q does not match name %q", key, cluster.Name)
		}
		location := cluster.Location
		if location == "" {
			location = cluster.Zone
		}
		if location != parts[1] {
			return fmt.Errorf("cluster slot %q does not match location %q", key, location)
		}
	}
	for name, operation := range metadata.Operations {
		if name == "" || operation == nil || operation.Name != name {
			return fmt.Errorf("invalid GKE operation slot %q", name)
		}
		if operation.Zone == "" || operation.TargetLink == "" ||
			operation.OperationType != "CREATE_CLUSTER" &&
				operation.OperationType != "DELETE_CLUSTER" {
			return fmt.Errorf("invalid GKE operation %q", name)
		}
		targetProject := extractSegmentAfter(operation.TargetLink, "projects")
		targetZone := firstOf(
			extractSegmentAfter(operation.TargetLink, "zones"),
			extractSegmentAfter(operation.TargetLink, "locations"),
		)
		if targetProject == "" || targetZone != operation.Zone ||
			extractSegmentAfter(operation.TargetLink, "clusters") == "" {
			return fmt.Errorf("GKE operation %q has ambiguous target scope", name)
		}
		switch operation.Status {
		case "PENDING", "RUNNING", "DONE":
		default:
			return fmt.Errorf("invalid GKE operation status %q", operation.Status)
		}
	}

	if len(metadata.Ownerships) == 0 {
		if metadata.OwnershipChecksum == "" {
			return nil
		}
		emptyChecksum, err := kubeconfigOwnershipChecksum(map[string]*kubeconfigOwnership{})
		if err != nil {
			return err
		}
		if metadata.OwnershipChecksum != emptyChecksum {
			return errors.New("kubeconfig ownership checksum does not match empty ownership state")
		}
		return nil
	}
	checksum, err := kubeconfigOwnershipChecksum(metadata.Ownerships)
	if err != nil {
		return fmt.Errorf("checksum kubeconfig ownership metadata: %w", err)
	}
	if metadata.OwnershipChecksum != checksum {
		return errors.New("kubeconfig ownership checksum mismatch")
	}
	for key, ownership := range metadata.Ownerships {
		if ownership == nil {
			return fmt.Errorf("kubeconfig ownership slot %q is null", key)
		}
		if ownership.Profile != context.Profile ||
			key != clusterKey(ownership.Project, ownership.Zone, ownership.Cluster) {
			return fmt.Errorf("kubeconfig ownership slot %q has mismatched identity", key)
		}
		if metadata.Clusters[key] == nil {
			return fmt.Errorf("kubeconfig ownership slot %q has no cluster", key)
		}
		if !ownership.isDurable() {
			return fmt.Errorf("kubeconfig ownership slot %q lacks a valid nonce or digest", key)
		}
		if ownership.Device == 0 || ownership.Inode == 0 {
			return fmt.Errorf("kubeconfig ownership slot %q lacks pinned file identity", key)
		}
	}
	return nil
}

func (api *API) persistMetadata() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	backend := "simulation"
	if api.backend.Enabled() {
		backend = "kind"
	}
	snapshot := gkeMetadata{
		Backend: backend, Clusters: make(map[string]*Cluster, len(api.clusters)),
		Ownerships: make(map[string]*kubeconfigOwnership, len(api.ownerships)),
		Operations: make(map[string]*GkeOperation, len(api.operations)),
	}
	for key, cluster := range api.clusters {
		snapshot.Clusters[key] = cloneCluster(cluster)
	}
	for key, ownership := range api.ownerships {
		if ownership != nil {
			clone := *ownership
			snapshot.Ownerships[key] = &clone
		}
	}
	for key, operation := range api.operations {
		snapshot.Operations[key] = cloneGKEOperation(operation)
	}
	api.mu.RUnlock()
	checksum, err := kubeconfigOwnershipChecksum(snapshot.Ownerships)
	if err != nil {
		return fmt.Errorf("checksum GKE kubeconfig ownership metadata: %w", err)
	}
	snapshot.OwnershipChecksum = checksum
	return api.stateStore.Save(gkeStateEntry, snapshot)
}
