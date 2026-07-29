package memorystore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/shims/logging"
	"minisky/pkg/state"
)

const (
	memorystoreStateEntry = "memorystore/redis"
	redisMetadataSchema   = "minisky-memorystore-redis"
	redisMetadataVersion  = 1
)

const redisBackendTimeout = 35 * time.Second

var redisHookRegistrationAttempts atomic.Uint32

func init() {
	redisHookRegistrationAttempts.Add(1)
	state.MustRegisterEntryValidator(memorystoreStateEntry,
		state.StrictEntryValidator[redisMetadata](validateRedisMetadata))
	state.MustRegisterPortableEntryCodec(memorystoreStateEntry, state.PortableEntryCodec{
		Export: exportPortableRedisMetadata,
		Import: importPortableRedisMetadata,
	})
	state.MustRegisterSnapshotValidator("memorystore/redis-operations", validateRedisSnapshot)
	state.MustRegisterSnapshotCodec("memorystore/redis-operations", normalizePortableRedisSnapshot)
	registry.Register("redis.googleapis.com", func(ctx *registry.Context) http.Handler {
		var logAPI *logging.API
		if handler, ok := ctx.GetShim("logging.googleapis.com").(*logging.API); ok {
			logAPI = handler
		}
		return NewAPI(ctx.OpMgr, ctx.SvcMgr, logAPI)
	})
	registry.Register("memcache.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewMemcacheAPI(ctx.OpMgr, memcacheBackendFromManager(ctx.SvcMgr))
	})
}

type Instance struct {
	Name                  string             `json:"name"`
	DisplayName           string             `json:"displayName,omitempty"`
	Labels                map[string]string  `json:"labels,omitempty"`
	Tier                  string             `json:"tier"`
	MemorySizeGb          int                `json:"memorySizeGb"`
	Host                  string             `json:"host,omitempty"`
	Port                  int                `json:"port,omitempty"`
	State                 string             `json:"state"`
	CreateTime            string             `json:"createTime"`
	LocationId            string             `json:"locationId"`
	AlternativeLocationId string             `json:"alternativeLocationId,omitempty"`
	AuthorizedNetwork     string             `json:"authorizedNetwork,omitempty"`
	ConnectMode           string             `json:"connectMode,omitempty"`
	PersistenceConfig     *PersistenceConfig `json:"persistenceConfig,omitempty"`
	RedisVersion          string             `json:"redisVersion,omitempty"`
	TransitEncryptionMode string             `json:"transitEncryptionMode,omitempty"`
	BackendID             string             `json:"-"`
}

type PersistenceConfig struct {
	PersistenceMode   string `json:"persistenceMode"`
	RdbSnapshotPeriod string `json:"rdbSnapshotPeriod,omitempty"`
}

type redisBackend interface {
	ProvisionRedis(context.Context, orchestrator.RedisBackendSpec) (string, orchestrator.RedisBackendSpec, error)
	ReconcileRedis(context.Context, orchestrator.RedisBackendSpec) (string, orchestrator.RedisBackendSpec, bool, error)
	PublishRedis(context.Context, orchestrator.RedisBackendSpec) error
	UnpublishRedis(orchestrator.RedisBackendSpec)
	DiscardRedis(context.Context, orchestrator.RedisBackendSpec, bool) error
	DeleteRedis(context.Context, orchestrator.RedisBackendSpec) error
}

type serviceManagerBackend struct {
	manager *orchestrator.ServiceManager
}

func (b serviceManagerBackend) ProvisionRedis(
	ctx context.Context,
	spec orchestrator.RedisBackendSpec,
) (string, orchestrator.RedisBackendSpec, error) {
	if b.manager == nil {
		return "", spec, fmt.Errorf("Docker service manager is unavailable")
	}
	return b.manager.ProvisionRedisExact(ctx, spec)
}

func (b serviceManagerBackend) ReconcileRedis(
	ctx context.Context,
	spec orchestrator.RedisBackendSpec,
) (string, orchestrator.RedisBackendSpec, bool, error) {
	if b.manager == nil {
		return "", spec, false, fmt.Errorf("Docker service manager is unavailable")
	}
	return b.manager.ReconcileRedisExact(ctx, spec)
}

func (b serviceManagerBackend) DeleteRedis(ctx context.Context, spec orchestrator.RedisBackendSpec) error {
	if b.manager == nil {
		return fmt.Errorf("Docker service manager is unavailable")
	}
	return b.manager.DeleteRedisExact(ctx, spec)
}

func (b serviceManagerBackend) PublishRedis(ctx context.Context, spec orchestrator.RedisBackendSpec) error {
	if b.manager == nil {
		return fmt.Errorf("Docker service manager is unavailable")
	}
	return b.manager.PublishRedisRuntime(ctx, spec)
}

func (b serviceManagerBackend) UnpublishRedis(spec orchestrator.RedisBackendSpec) {
	if b.manager != nil {
		b.manager.UnpublishRedisRuntime(spec)
	}
}

func (b serviceManagerBackend) DiscardRedis(
	ctx context.Context,
	spec orchestrator.RedisBackendSpec,
	removeVolume bool,
) error {
	if b.manager == nil {
		return fmt.Errorf("Docker service manager is unavailable")
	}
	return b.manager.DiscardRedisProvisional(ctx, spec, removeVolume)
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type stateTransactionalStore interface {
	TransformEntries(string, state.EntryTransform) (state.TransformResult, error)
}

type redisMetadata struct {
	Schema     string                       `json:"schema"`
	Version    int                          `json:"version"`
	Instances  map[string]persistedInstance `json:"instances"`
	Operations map[string]operationTarget   `json:"operations,omitempty"`
}

type persistedInstance struct {
	Instance  *Instance                     `json:"instance"`
	BackendID string                        `json:"backendId"`
	Backend   orchestrator.RedisBackendSpec `json:"backend"`
}

type operationTarget struct {
	ManagerName string `json:"managerName"`
	ResourceKey string `json:"resourceKey"`
	Delete      bool   `json:"delete,omitempty"`
}

type legacyRedisMetadata struct {
	Instances  map[string]legacyPersistedInstance `json:"instances"`
	Operations map[string]operationTarget         `json:"operations,omitempty"`
}

type legacyPersistedInstance struct {
	Instance  *Instance `json:"instance"`
	BackendID string    `json:"backendId"`
}

func validateRedisMetadata(context state.EntryValidationContext, metadata *redisMetadata) error {
	operations, err := redisOperationsFromSnapshot(context.Entries)
	if err != nil {
		return err
	}
	return validateRedisMetadataWithOperations(metadata, operations)
}

func validateRedisSnapshot(context state.SnapshotValidationContext) error {
	operations, err := redisOperationsFromSnapshot(context.Entries)
	if err != nil {
		return err
	}
	payload, hasMetadata := context.Entries[memorystoreStateEntry]
	if !hasMetadata {
		for name, operation := range operations {
			if operation != nil &&
				(operation.Kind == "redis#operation" || operation.ServiceKind == "redis#operation") {
				return fmt.Errorf("Redis durable operation %q has no metadata sibling", name)
			}
		}
		return nil
	}
	if operations == nil {
		return errors.New("Redis metadata requires an explicit durable operation sibling")
	}
	var metadata redisMetadata
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return fmt.Errorf("decode Redis metadata sibling: %w", err)
	}
	return validateRedisMetadataWithOperations(&metadata, operations)
}

func normalizePortableRedisSnapshot(context state.SnapshotValidationContext) error {
	metadataPayload, hasMetadata := context.Entries[memorystoreStateEntry]
	operationPayload, hasOperations := context.Entries["orchestrator/operations"]
	if !hasMetadata || !hasOperations {
		return nil
	}
	var metadata redisMetadata
	if err := json.Unmarshal(metadataPayload, &metadata); err != nil {
		return err
	}
	var operations map[string]*orchestrator.Operation
	if err := json.Unmarshal(operationPayload, &operations); err != nil {
		return err
	}
	changed := false
	activeTargets := make(map[string]int)
	for _, target := range metadata.Operations {
		if operation := operations[target.ManagerName]; validPortablePendingRedisOperation(operation) {
			activeTargets[target.ResourceKey]++
		}
	}
	for _, target := range metadata.Operations {
		operation := operations[target.ManagerName]
		if !validPortablePendingRedisOperation(operation) || activeTargets[target.ResourceKey] != 1 {
			continue
		}
		operation.Status = orchestrator.StatusDone
		operation.Done = true
		operation.Progress = 100
		operation.EndTime = operation.StartTime
		if operation.EndTime == "" {
			operation.EndTime = operation.InsertTime
		}
		operation.Error = &orchestrator.OperationError{
			Code: 500, Message: "operation interrupted by portable Redis snapshot; side effects were not replayed",
		}
		operation.Response = nil
		changed = true
	}
	if changed {
		payload, err := json.Marshal(operations)
		if err != nil {
			return err
		}
		context.Entries["orchestrator/operations"] = payload
	}
	return nil
}

func validPortablePendingRedisOperation(operation *orchestrator.Operation) bool {
	if operation == nil ||
		(operation.Status != orchestrator.StatusPending &&
			operation.Status != orchestrator.StatusRunning) ||
		operation.Done || operation.Progress < 0 || operation.Progress >= 100 ||
		operation.Error != nil || len(operation.Response) != 0 || operation.EndTime != "" {
		return false
	}
	insertTime, err := time.Parse(time.RFC3339, operation.InsertTime)
	if err != nil {
		return false
	}
	if operation.Status == orchestrator.StatusPending {
		return operation.StartTime == ""
	}
	startTime, err := time.Parse(time.RFC3339, operation.StartTime)
	return err == nil && !startTime.Before(insertTime)
}

func validateRedisMetadataWithOperations(
	metadata *redisMetadata,
	operations map[string]*orchestrator.Operation,
) error {
	if metadata.Schema != redisMetadataSchema || metadata.Version != redisMetadataVersion {
		return fmt.Errorf("unsupported Redis metadata schema %q version %d",
			metadata.Schema, metadata.Version)
	}
	if metadata.Instances == nil {
		return errors.New("Redis metadata instances are required")
	}
	backendIDs := make(map[string]string, len(metadata.Instances))
	for key, persisted := range metadata.Instances {
		instance := persisted.Instance
		if instance == nil || key == "" || instance.Name != key {
			return fmt.Errorf("Redis instance key %q does not match its required name", key)
		}
		project, location, id, ok := canonicalRedisInstanceName(key)
		if !ok {
			return fmt.Errorf("Redis instance key %q is malformed", key)
		}
		if instance.LocationId != location {
			return fmt.Errorf("Redis instance %q locationId does not match its canonical path", key)
		}
		expectedBackendID := backendID(project, location, id)
		if persisted.BackendID != expectedBackendID ||
			persisted.Backend.ResourceID != expectedBackendID {
			return fmt.Errorf("Redis instance %q backend identity is invalid", key)
		}
		if previous, duplicate := backendIDs[persisted.BackendID]; duplicate {
			return fmt.Errorf("Redis instances %q and %q duplicate backend identity", previous, key)
		}
		backendIDs[persisted.BackendID] = key
		if instance.MemorySizeGb <= 0 {
			return fmt.Errorf("Redis instance %q memorySizeGb is invalid", key)
		}
		if instance.Tier != "BASIC" && instance.Tier != "STANDARD_HA" {
			return fmt.Errorf("Redis instance %q tier %q is unsupported", key, instance.Tier)
		}
		if instance.RedisVersion != "REDIS_7_2" {
			return fmt.Errorf("Redis instance %q version %q is unsupported", key, instance.RedisVersion)
		}
		if instance.ConnectMode != "" && instance.ConnectMode != "DIRECT_PEERING" {
			return fmt.Errorf("Redis instance %q connectMode %q is unsupported", key, instance.ConnectMode)
		}
		if instance.TransitEncryptionMode != "" && instance.TransitEncryptionMode != "DISABLED" {
			return fmt.Errorf("Redis instance %q transitEncryptionMode %q is unsupported",
				key, instance.TransitEncryptionMode)
		}
		if instance.CreateTime != "" {
			if _, err := time.Parse(time.RFC3339, instance.CreateTime); err != nil {
				return fmt.Errorf("Redis instance %q createTime is invalid", key)
			}
		}
		if instance.AuthorizedNetwork != "" {
			if !validRedisAuthorizedNetwork(project, instance.AuthorizedNetwork) {
				return fmt.Errorf("Redis instance %q authorizedNetwork is noncanonical", key)
			}
		}
		if err := validateRedisPersistenceConfig(instance.PersistenceConfig); err != nil {
			return fmt.Errorf("Redis instance %q has unsupported durable fields", key)
		}
		switch instance.State {
		case "CREATING", "READY", "REPAIRING", "DELETING":
		default:
			return fmt.Errorf("Redis instance %q has unsupported state %q", key, instance.State)
		}
		expected := orchestrator.Redis72BackendSpec(expectedBackendID)
		backend := persisted.Backend
		if backend.Image != expected.Image || backend.RepoDigest != expected.RepoDigest ||
			backend.Platform != expected.Platform {
			return fmt.Errorf("Redis instance %q has unsupported backend image contract", key)
		}
		hasRuntime := hasRedisRuntimeProvenance(backend)
		if !hasRuntime {
			if instance.State != "CREATING" && instance.State != "REPAIRING" {
				return fmt.Errorf("Redis instance %q lacks runtime provenance outside a metadata-only state", key)
			}
			if instance.Host != "" || instance.Port != 0 {
				return fmt.Errorf("Redis instance %q exposes a profile-local endpoint without provenance", key)
			}
			continue
		}
		if !isHexIdentity(backend.ContainerID, false) || backend.Generation == 0 ||
			!isHexIdentity(backend.ImageID, true) ||
			!isHexIdentity(backend.VolumeIdentity, true) ||
			!isHexIdentity(backend.VolumeProvenance, true) ||
			!isHexIdentity(backend.ContainerIdentity, true) || backend.HostPort == "" {
			return fmt.Errorf("Redis instance %q has incomplete runtime provenance", key)
		}
		port, err := strconv.Atoi(backend.HostPort)
		if err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("Redis instance %q runtime host port is invalid", key)
		}
		if instance.State == "REPAIRING" && instance.Host == "" && instance.Port == 0 {
			continue
		}
		if instance.Port != port {
			return fmt.Errorf("Redis instance %q runtime port %d does not match backend host port %d",
				key, instance.Port, port)
		}
		if instance.Host != "127.0.0.1" {
			return fmt.Errorf("Redis instance %q runtime host is not canonical loopback", key)
		}
	}
	managerNames := make(map[string]string, len(metadata.Operations))
	operationIDs := make(map[string]string, len(metadata.Operations))
	activeTargets := make(map[string]string)
	type terminalOutcome struct {
		name      string
		target    operationTarget
		operation *orchestrator.Operation
		end       time.Time
	}
	latestTerminal := make(map[string]terminalOutcome)
	if len(metadata.Operations) != 0 && operations == nil {
		return errors.New("Redis operation metadata requires an explicit durable operation sibling")
	}
	operationNames := make([]string, 0, len(metadata.Operations))
	for name := range metadata.Operations {
		operationNames = append(operationNames, name)
	}
	sort.Strings(operationNames)
	for _, name := range operationNames {
		target := metadata.Operations[name]
		project, location, _, ok := canonicalRedisInstanceName(target.ResourceKey)
		if !ok {
			return fmt.Errorf("Redis operation %q target is malformed", name)
		}
		expectedName := fmt.Sprintf("projects/%s/locations/%s/operations/%s",
			project, location, target.ManagerName)
		if name != expectedName || !validRedisOperationManagerName(target.ManagerName) {
			return fmt.Errorf("Redis operation %q is malformed", name)
		}
		if previous, duplicate := managerNames[target.ManagerName]; duplicate {
			return fmt.Errorf("Redis operations %q and %q duplicate manager identity", previous, name)
		}
		managerNames[target.ManagerName] = name
		if operations == nil {
			continue
		}
		operation := operations[target.ManagerName]
		if err := validateRedisOperationRelationship(name, target, operation, metadata.Instances); err != nil {
			return err
		}
		if previous, duplicate := operationIDs[operation.ID]; duplicate {
			return fmt.Errorf("Redis operations %q and %q duplicate operation ID", previous, name)
		}
		operationIDs[operation.ID] = name
		if operation.Status != orchestrator.StatusDone {
			if previous, conflict := activeTargets[target.ResourceKey]; conflict {
				return fmt.Errorf("Redis operations %q and %q conflict on one active resource", previous, name)
			}
			activeTargets[target.ResourceKey] = name
		} else {
			end, _ := time.Parse(time.RFC3339, operation.EndTime)
			previous, exists := latestTerminal[target.ResourceKey]
			if exists && end.Equal(previous.end) {
				return fmt.Errorf("Redis operations %q and %q have ambiguous terminal ordering",
					previous.name, name)
			}
			if !exists || end.After(previous.end) {
				latestTerminal[target.ResourceKey] = terminalOutcome{
					name: name, target: target, operation: operation, end: end,
				}
			}
		}
	}
	for resourceKey, outcome := range latestTerminal {
		if _, active := activeTargets[resourceKey]; active {
			continue
		}
		persisted, exists := metadata.Instances[resourceKey]
		if outcome.operation.Error != nil {
			if !exists {
				continue
			}
			if persisted.Instance == nil {
				return fmt.Errorf("Redis operation %q failed with malformed retained metadata",
					outcome.name)
			}
			if !outcome.target.Delete {
				if persisted.Instance.State != "REPAIRING" ||
					hasRedisRuntimeProvenance(persisted.Backend) {
					return fmt.Errorf("Redis operation %q failed creation retains impossible runtime",
						outcome.name)
				}
				continue
			}
			if persisted.Instance.State != "READY" && persisted.Instance.State != "REPAIRING" {
				return fmt.Errorf("Redis operation %q failed deletion retains impossible state",
					outcome.name)
			}
			continue
		}
		if outcome.target.Delete {
			if exists {
				return fmt.Errorf("Redis operation %q completed deletion retains its resource",
					outcome.name)
			}
			continue
		}
		if !exists || persisted.Instance == nil ||
			(persisted.Instance.State != "READY" && persisted.Instance.State != "REPAIRING") {
			return fmt.Errorf("Redis operation %q completed creation lacks its resource", outcome.name)
		}
	}
	if operations != nil {
		for managerName, operation := range operations {
			if operation != nil &&
				(operation.Kind == "redis#operation" || operation.ServiceKind == "redis#operation") {
				if _, ok := managerNames[managerName]; !ok {
					return fmt.Errorf("Redis durable operation %q is orphaned", managerName)
				}
			}
		}
	}
	return nil
}

func validateRedisOperationRelationship(
	name string,
	target operationTarget,
	operation *orchestrator.Operation,
	instances map[string]persistedInstance,
) error {
	if operation == nil || operation.Name != target.ManagerName ||
		operation.Kind != "redis#operation" || operation.TargetLink != target.ResourceKey {
		return fmt.Errorf("Redis operation %q durable identity is mismatched", name)
	}
	_, location, _, _ := canonicalRedisInstanceName(target.ResourceKey)
	project, _, _, _ := canonicalRedisInstanceName(target.ResourceKey)
	if operation.Region != location || operation.Zone != "" ||
		operation.Project != project || operation.Location != location ||
		operation.ServiceKind != "redis#operation" {
		return fmt.Errorf("Redis operation %q scope is mismatched", name)
	}
	if _, err := strconv.ParseUint(operation.ID, 10, 63); err != nil ||
		operation.Progress < 0 || operation.Progress > 100 {
		return fmt.Errorf("Redis operation %q lifecycle metadata is invalid", name)
	}
	insertTime, err := time.Parse(time.RFC3339, operation.InsertTime)
	if err != nil {
		return fmt.Errorf("Redis operation %q insertTime is invalid", name)
	}
	var startTime time.Time
	if operation.StartTime != "" {
		startTime, err = time.Parse(time.RFC3339, operation.StartTime)
		if err != nil || startTime.Before(insertTime) {
			return fmt.Errorf("Redis operation %q startTime is invalid", name)
		}
	}
	var endTime time.Time
	if operation.EndTime != "" {
		endTime, err = time.Parse(time.RFC3339, operation.EndTime)
		lowerBound := insertTime
		if !startTime.IsZero() {
			lowerBound = startTime
		}
		if err != nil || endTime.Before(lowerBound) {
			return fmt.Errorf("Redis operation %q endTime is invalid", name)
		}
	}
	expectedVerb := "CREATE"
	if target.Delete {
		expectedVerb = "DELETE"
	}
	if operation.OperationType != expectedVerb {
		return fmt.Errorf("Redis operation %q verb is mismatched", name)
	}
	switch operation.Status {
	case orchestrator.StatusPending, orchestrator.StatusRunning:
		if operation.Done || operation.Error != nil || len(operation.Response) != 0 ||
			operation.EndTime != "" || operation.Progress >= 100 ||
			(operation.Status == orchestrator.StatusPending && operation.StartTime != "") ||
			(operation.Status == orchestrator.StatusRunning && operation.StartTime == "") {
			return fmt.Errorf("Redis operation %q nonterminal result is inconsistent", name)
		}
	case orchestrator.StatusDone:
		if !operation.Done || operation.Progress != 100 || operation.EndTime == "" ||
			operation.Error != nil && len(operation.Response) != 0 {
			return fmt.Errorf("Redis operation %q terminal result is inconsistent", name)
		}
		if operation.Error != nil &&
			(operation.Error.Code < 400 || operation.Error.Code > 599 ||
				strings.TrimSpace(operation.Error.Message) == "") {
			return fmt.Errorf("Redis operation %q terminal error is malformed", name)
		}
		if len(operation.Response) != 0 {
			if !json.Valid(operation.Response) {
				return fmt.Errorf("Redis operation %q terminal response is malformed", name)
			}
			return fmt.Errorf("Redis operation %q carries noncanonical durable response data", name)
		}
	default:
		return fmt.Errorf("Redis operation %q status %q is unsupported", name, operation.Status)
	}
	persisted, exists := instances[target.ResourceKey]
	if operation.Status != orchestrator.StatusDone {
		if !exists || persisted.Instance == nil {
			return fmt.Errorf("Redis operation %q has no nonterminal resource", name)
		}
		expectedState := "CREATING"
		if target.Delete {
			expectedState = "DELETING"
		}
		if persisted.Instance.State != expectedState {
			return fmt.Errorf("Redis operation %q resource state is inconsistent", name)
		}
		return nil
	}
	return nil
}

func redisOperationsFromSnapshot(
	entries map[string]json.RawMessage,
) (map[string]*orchestrator.Operation, error) {
	if entries == nil {
		return nil, nil
	}
	payload, ok := entries["orchestrator/operations"]
	if !ok {
		return nil, nil
	}
	var operations map[string]*orchestrator.Operation
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&operations); err != nil {
		return nil, fmt.Errorf("decode Redis operation relationships: %w", err)
	}
	if operations == nil {
		operations = make(map[string]*orchestrator.Operation)
	}
	for name, operation := range operations {
		if operation == nil || operation.Name != name {
			return nil, fmt.Errorf("durable operation key %q is noncanonical", name)
		}
	}
	return operations, nil
}

func canonicalRedisInstanceName(name string) (string, string, string, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[2] != "locations" ||
		parts[4] != "instances" || parts[1] == "" || parts[3] == "" || !validID(parts[5]) {
		return "", "", "", false
	}
	return parts[1], parts[3], parts[5], true
}

func validateRedisPersistenceConfig(config *PersistenceConfig) error {
	if config == nil {
		return nil
	}
	switch config.PersistenceMode {
	case "", "DISABLED":
		if config.RdbSnapshotPeriod != "" {
			return errors.New("RDB snapshot period requires RDB persistence")
		}
	case "RDB":
		switch config.RdbSnapshotPeriod {
		case "", "ONE_HOUR", "SIX_HOURS", "TWELVE_HOURS", "TWENTY_FOUR_HOURS":
		default:
			return errors.New("unsupported RDB snapshot period")
		}
	default:
		return errors.New("unsupported persistence mode")
	}
	return nil
}

func validRedisAuthorizedNetwork(project, network string) bool {
	parts := strings.Split(network, "/")
	return len(parts) == 5 && parts[0] == "projects" && parts[1] == project &&
		parts[2] == "global" && parts[3] == "networks" && validID(parts[4])
}

func validRedisOperationManagerName(name string) bool {
	parts := strings.Split(strings.TrimPrefix(name, "operation-"), "-")
	if !strings.HasPrefix(name, "operation-") || len(parts) != 2 || len(parts[1]) != 8 {
		return false
	}
	if _, err := strconv.ParseInt(parts[0], 10, 64); err != nil {
		return false
	}
	for _, char := range parts[1] {
		if !((char >= 'a' && char <= 'z') || (char >= '0' && char <= '9')) {
			return false
		}
	}
	return true
}

func hasRedisRuntimeProvenance(backend orchestrator.RedisBackendSpec) bool {
	return backend.ImageID != "" || backend.VolumeIdentity != "" ||
		backend.VolumeProvenance != "" || backend.ContainerIdentity != "" ||
		backend.ContainerID != "" || backend.Generation != 0 || backend.HostPort != ""
}

func isHexIdentity(value string, prefixed bool) bool {
	if prefixed {
		if !strings.HasPrefix(value, "sha256:") {
			return false
		}
		value = strings.TrimPrefix(value, "sha256:")
	}
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}

func isLegacyRedisMetadata(payload json.RawMessage) (bool, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return false, err
	}
	_, hasSchema := envelope["schema"]
	_, hasVersion := envelope["version"]
	return !hasSchema && !hasVersion, nil
}

func migrateLegacyRedisEntries(entries map[string]json.RawMessage) error {
	payload, exists := entries[memorystoreStateEntry]
	if !exists {
		return errors.New("legacy Redis metadata entry disappeared during migration")
	}
	operations, operationsPresent, err := redisOperationsFromCommittedEntries(entries)
	if err != nil {
		return err
	}
	metadata, migrated, err := migrateLegacyRedisMetadata(payload, operations)
	if err != nil {
		return err
	}
	if !migrated {
		return nil
	}
	if !operationsPresent {
		if len(metadata.Operations) != 0 {
			return errors.New("legacy Redis operation metadata has no durable sibling")
		}
		entries["orchestrator/operations"] = json.RawMessage(`{}`)
	}
	metadataPayload, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	entries[memorystoreStateEntry] = metadataPayload
	return nil
}

func redisOperationsFromCommittedEntries(
	entries map[string]json.RawMessage,
) (map[string]*orchestrator.Operation, bool, error) {
	_, present := entries["orchestrator/operations"]
	operations, err := redisOperationsFromSnapshot(entries)
	return operations, present, err
}

func migrateLegacyRedisMetadata(
	payload json.RawMessage,
	operations map[string]*orchestrator.Operation,
) (redisMetadata, bool, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return redisMetadata{}, false, err
	}
	_, hasSchema := envelope["schema"]
	_, hasVersion := envelope["version"]
	if hasSchema || hasVersion {
		if !hasSchema || !hasVersion {
			return redisMetadata{}, false, errors.New("Redis metadata schema/version envelope is incomplete")
		}
		var current redisMetadata
		decoder := json.NewDecoder(strings.NewReader(string(payload)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&current); err != nil {
			return redisMetadata{}, false, err
		}
		if err := validateRedisMetadataWithOperations(&current, operations); err != nil {
			return redisMetadata{}, false, err
		}
		return current, false, nil
	}
	var legacy legacyRedisMetadata
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&legacy); err != nil {
		return redisMetadata{}, false, err
	}
	current := redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances:  make(map[string]persistedInstance, len(legacy.Instances)),
		Operations: legacy.Operations,
	}
	for key, persisted := range legacy.Instances {
		project, location, id, ok := canonicalRedisInstanceName(key)
		if !ok || persisted.Instance == nil || persisted.Instance.Name != key {
			return redisMetadata{}, false,
				fmt.Errorf("invalid legacy Redis metadata: instance key %q is noncanonical", key)
		}
		expectedBackendID := backendID(project, location, id)
		if persisted.BackendID != expectedBackendID {
			return redisMetadata{}, false,
				fmt.Errorf("invalid legacy Redis metadata: instance %q backend identity is ambiguous", key)
		}
		instance := cloneInstance(persisted.Instance)
		instance.State = "REPAIRING"
		instance.Host = ""
		instance.Port = 0
		current.Instances[key] = persistedInstance{
			Instance: instance, BackendID: expectedBackendID,
			Backend: orchestrator.Redis72BackendSpec(expectedBackendID),
		}
	}
	if len(current.Operations) != 0 && operations == nil {
		return redisMetadata{}, false,
			errors.New("invalid legacy Redis metadata: durable operation provenance is unavailable")
	}
	for _, target := range current.Operations {
		if operation := operations[target.ManagerName]; operation != nil &&
			operation.Status != orchestrator.StatusDone {
			return redisMetadata{}, false,
				fmt.Errorf("invalid legacy Redis metadata: operation %q is nonterminal", target.ManagerName)
		}
	}
	if err := validateRedisMetadataWithOperations(&current, operations); err != nil {
		return redisMetadata{}, false, fmt.Errorf("invalid legacy Redis metadata: %w", err)
	}
	return current, true, nil
}

func portableRedisMetadata(
	_ state.EntryValidationContext,
	payload json.RawMessage,
) (json.RawMessage, error) {
	var metadata redisMetadata
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return nil, err
	}
	for key, persisted := range metadata.Instances {
		if persisted.Instance == nil {
			continue
		}
		if !canNormalizePortableRedisInstance(persisted) {
			continue
		}
		persisted.Instance = cloneInstance(persisted.Instance)
		persisted.Instance.State = "REPAIRING"
		persisted.Instance.Host = ""
		persisted.Instance.Port = 0
		persisted.Backend = orchestrator.Redis72BackendSpec(persisted.BackendID)
		metadata.Instances[key] = persisted
	}
	return json.Marshal(metadata)
}

func canNormalizePortableRedisInstance(persisted persistedInstance) bool {
	instance := persisted.Instance
	if instance == nil {
		return false
	}
	switch instance.State {
	case "CREATING", "DELETING", "REPAIRING":
	case "READY":
	default:
		return false
	}
	backend := persisted.Backend
	hasLocal := backend.HostPort != "" || backend.ContainerID != "" ||
		backend.ImageID != "" || backend.VolumeIdentity != "" ||
		backend.VolumeProvenance != "" || backend.ContainerIdentity != "" ||
		backend.Generation != 0
	if !hasLocal {
		return instance.State != "READY"
	}
	if backend.ImageID == "" || backend.VolumeIdentity == "" ||
		backend.VolumeProvenance == "" || backend.ContainerIdentity == "" ||
		backend.ContainerID == "" || backend.Generation == 0 || backend.HostPort == "" ||
		instance.Host != "127.0.0.1" {
		return false
	}
	port, err := strconv.Atoi(backend.HostPort)
	return err == nil && port == instance.Port
}

func exportPortableRedisMetadata(
	context state.EntryValidationContext,
	payload json.RawMessage,
) (json.RawMessage, error) {
	return portableRedisMetadata(context, payload)
}

func importPortableRedisMetadata(
	context state.EntryValidationContext,
	payload json.RawMessage,
) (json.RawMessage, error) {
	return portableRedisMetadata(context, payload)
}

type redisPublicationClass uint8

const (
	redisPublicationUnchanged redisPublicationClass = iota
	redisPublicationReplacement
)

type API struct {
	mu            sync.RWMutex
	transactionMu sync.RWMutex
	persistMu     sync.Mutex
	opMgr         *orchestrator.OperationManager
	backend       redisBackend
	logAPI        *logging.API
	store         stateStore
	instances     map[string]*Instance
	backends      map[string]orchestrator.RedisBackendSpec
	operations    map[string]operationTarget
	initErr       error
}

func NewAPI(opMgr *orchestrator.OperationManager, manager *orchestrator.ServiceManager, logAPI *logging.API) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Memorystore] state disabled: %v", err)
		return newAPI(opMgr, serviceManagerBackend{manager: manager}, logAPI, nil)
	}
	api, err := NewAPIWithStore(opMgr, serviceManagerBackend{manager: manager}, logAPI, store)
	if err != nil {
		log.Printf("[Shim: Memorystore] state rehydration failed: %v", err)
		disabled := newAPI(opMgr, serviceManagerBackend{manager: manager}, logAPI, nil)
		disabled.initErr = err
		return disabled
	}
	return api
}

func NewAPIWithStore(opMgr *orchestrator.OperationManager, backend redisBackend, logAPI *logging.API, store stateStore) (*API, error) {
	api := newAPI(opMgr, backend, logAPI, store)
	if store == nil {
		return api, nil
	}
	var payload json.RawMessage
	if err := store.Load(memorystoreStateEntry, &payload); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Memorystore metadata: %w", err)
	}
	legacy, err := isLegacyRedisMetadata(payload)
	if err != nil {
		return nil, fmt.Errorf("validate Memorystore metadata: %w", err)
	}
	var saved redisMetadata
	if legacy {
		transactionalStore, ok := store.(stateTransactionalStore)
		if !ok {
			return nil, errors.New("persist migrated Memorystore metadata: transactional state transform is unavailable")
		}
		result, transformErr := transactionalStore.TransformEntries("", migrateLegacyRedisEntries)
		if transformErr != nil {
			return nil, fmt.Errorf("persist migrated Memorystore metadata: %w", transformErr)
		}
		committedPayload, exists := result.Entries[memorystoreStateEntry]
		if !exists {
			return nil, errors.New("read back migrated Memorystore metadata: entry is absent")
		}
		committedOperations, committedPresent, err := redisOperationsFromCommittedEntries(result.Entries)
		if err != nil {
			return nil, fmt.Errorf("read back migrated Redis operation sibling: %w", err)
		}
		if !committedPresent {
			return nil, errors.New("read back migrated Redis operation sibling: entry is absent")
		}
		var remainedLegacy bool
		saved, remainedLegacy, err = migrateLegacyRedisMetadata(committedPayload, committedOperations)
		if err != nil {
			return nil, fmt.Errorf("validate committed Memorystore migration: %w", err)
		}
		if remainedLegacy {
			return nil, errors.New("validate committed Memorystore migration: legacy data remained")
		}
	} else {
		operationSnapshot, operationSiblingPresent, loadErr :=
			redisOperationsForLocalLoad(store, api.opMgr)
		if loadErr != nil {
			return nil, fmt.Errorf("load Redis durable operation sibling: %w", loadErr)
		}
		var migrated bool
		saved, migrated, err = migrateLegacyRedisMetadata(payload, operationSnapshot)
		if err != nil {
			return nil, fmt.Errorf("validate Memorystore metadata: %w", err)
		}
		if migrated {
			return nil, errors.New("validate Memorystore metadata: legacy classification changed")
		}
		if !operationSiblingPresent {
			return nil, errors.New("validate Memorystore metadata: durable operation sibling is absent")
		}
	}
	if saved.Instances != nil {
		for key, persisted := range saved.Instances {
			if persisted.Instance != nil {
				persisted.Instance.BackendID = persisted.BackendID
				api.instances[key] = persisted.Instance
				if persisted.Backend.ResourceID != "" {
					api.backends[key] = persisted.Backend
				}
			}
		}
	}
	if saved.Operations != nil {
		api.operations = saved.Operations
	}
	type pendingRedisPublication struct {
		key      string
		previous orchestrator.RedisBackendSpec
		resolved orchestrator.RedisBackendSpec
		class    redisPublicationClass
	}
	var pendingPublications []pendingRedisPublication
	changed := false
	for key, instance := range api.instances {
		if instance == nil {
			delete(api.instances, key)
			changed = true
			continue
		}
		if instance.State == "DELETING" {
			backendSpec, ok := api.backends[key]
			if !ok {
				return nil, fmt.Errorf("resume deleting Redis backend %q: persisted immutable identity is missing",
					instance.BackendID)
			}
			deleteCtx, deleteCancel := context.WithTimeout(context.Background(), redisBackendTimeout)
			deleteErr := backend.DeleteRedis(deleteCtx, backendSpec)
			deleteCancel()
			if deleteErr != nil {
				return nil, fmt.Errorf("resume deleting Redis backend %q: %w", instance.BackendID, deleteErr)
			}
			delete(api.instances, key)
			delete(api.backends, key)
			for _, target := range api.operations {
				if target.Delete && target.ResourceKey == key {
					api.opMgr.MarkDone(target.ManagerName)
				}
			}
			changed = true
			continue
		}
		backendSpec, ok := api.backends[key]
		if !ok {
			instance.State = "REPAIRING"
			instance.Host = ""
			instance.Port = 0
			changed = true
			continue
		}
		if instance.State == "REPAIRING" && !hasRedisRuntimeProvenance(backendSpec) {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), redisBackendTimeout)
		endpoint, resolved, owned, err := backend.ReconcileRedis(ctx, backendSpec)
		cancel()
		if err == nil && !owned && instance.State == "CREATING" {
			delete(api.instances, key)
			delete(api.backends, key)
			changed = true
			continue
		}
		api.backends[key] = resolved
		if err != nil || !owned {
			instance.State = "REPAIRING"
			instance.Host = ""
			instance.Port = 0
			changed = true
			continue
		}
		host, port, err := parseLoopbackEndpoint(endpoint)
		if err != nil {
			instance.State = "REPAIRING"
			instance.Host = ""
			instance.Port = 0
		} else {
			instance.State = "READY"
			instance.Host = host
			instance.Port = port
		}
		class := redisPublicationUnchanged
		if resolved.ContainerID != backendSpec.ContainerID {
			class = redisPublicationReplacement
		}
		pendingPublications = append(pendingPublications, pendingRedisPublication{
			key: key, previous: backendSpec, resolved: resolved, class: class,
		})
		changed = true
	}
	sort.Slice(pendingPublications, func(i, j int) bool {
		return pendingPublications[i].key < pendingPublications[j].key
	})
	if changed {
		if err := api.persist(); err != nil {
			var compensationErr error
			for _, pending := range pendingPublications {
				if pending.class == redisPublicationReplacement {
					compensationErr = errors.Join(compensationErr,
						api.discardRedisProvisional(pending.resolved, false))
				}
			}
			if compensationErr != nil {
				return nil, errors.Join(fmt.Errorf("persist Memorystore reconciliation: %w", err),
					fmt.Errorf("compensate provisional Redis reconciliation: %w", compensationErr))
			}
			return nil, fmt.Errorf("persist Memorystore reconciliation: %w", err)
		}
	}
	for index, pending := range pendingPublications {
		ctx, cancel := context.WithTimeout(context.Background(), redisBackendTimeout)
		err := backend.PublishRedis(ctx, pending.resolved)
		cancel()
		if err == nil {
			continue
		}
		var compensationErr error
		for candidateIndex, candidate := range pendingPublications {
			publishedUnchanged := candidateIndex < index &&
				candidate.class == redisPublicationUnchanged
			if candidateIndex < index && candidate.class == redisPublicationReplacement {
				backend.UnpublishRedis(candidate.resolved)
			}
			if candidate.class == redisPublicationReplacement {
				compensationErr = errors.Join(compensationErr,
					api.discardRedisProvisional(candidate.resolved, false))
			}
			if publishedUnchanged {
				continue
			}
			api.backends[candidate.key] = candidate.previous
			if instance := api.instances[candidate.key]; instance != nil {
				instance.State = "REPAIRING"
				instance.Host = ""
				instance.Port = 0
			}
		}
		compensationErr = errors.Join(compensationErr, api.persist())
		return nil, errors.Join(fmt.Errorf("publish persisted Redis runtime: %w", err), compensationErr)
	}
	return api, nil
}

func redisOperationsForLocalLoad(
	store stateStore,
	manager *orchestrator.OperationManager,
) (map[string]*orchestrator.Operation, bool, error) {
	if _, ok := store.(stateTransactionalStore); ok {
		var payload json.RawMessage
		if err := store.Load("orchestrator/operations", &payload); err != nil {
			if errors.Is(err, state.ErrNotFound) {
				return nil, false, nil
			}
			return nil, false, err
		}
		operations, err := redisOperationsFromSnapshot(map[string]json.RawMessage{
			"orchestrator/operations": payload,
		})
		return operations, true, err
	}
	var operations map[string]*orchestrator.Operation
	for _, operation := range manager.List() {
		if operation != nil {
			if operations == nil {
				operations = make(map[string]*orchestrator.Operation)
			}
			operations[operation.Name] = operation
		}
	}
	return operations, true, nil
}

func newAPI(opMgr *orchestrator.OperationManager, backend redisBackend, logAPI *logging.API, store stateStore) *API {
	if opMgr == nil {
		opMgr = orchestrator.NewOperationManager()
	}
	return &API{
		opMgr:      opMgr,
		backend:    backend,
		logAPI:     logAPI,
		store:      store,
		instances:  make(map[string]*Instance),
		backends:   make(map[string]orchestrator.RedisBackendSpec),
		operations: make(map[string]operationTarget),
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Memorystore] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if api.initErr != nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Memorystore state is unavailable")
		return
	}
	if strings.EqualFold(strings.Split(r.Host, ":")[0], "memorystore.googleapis.com") {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"Memorystore for Valkey requires a dedicated owned Valkey backend; the Redis backend is not reused")
		return
	}
	switch {
	case strings.Contains(r.URL.Path, "/operations/") && r.Method == http.MethodGet:
		api.getOperation(w, r)
	case strings.HasSuffix(r.URL.Path, "/instances") && r.Method == http.MethodPost:
		api.createInstance(w, r)
	case strings.HasSuffix(r.URL.Path, "/instances") && r.Method == http.MethodGet:
		api.listInstances(w, r)
	case strings.Contains(r.URL.Path, "/instances/") && r.Method == http.MethodGet:
		api.getInstance(w, r)
	case strings.Contains(r.URL.Path, "/instances/") && r.Method == http.MethodDelete:
		api.deleteInstance(w, r)
	case strings.Contains(r.URL.Path, ":export") || strings.Contains(r.URL.Path, ":import") ||
		strings.Contains(r.URL.Path, ":failover") || strings.Contains(r.URL.Path, ":upgrade"):
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Memorystore import, export, failover, and upgrade are not implemented")
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Memorystore resource not found")
	}
}

func (api *API) createInstance(w http.ResponseWriter, r *http.Request) {
	project, location := projectLocation(r.URL.Path)
	id := r.URL.Query().Get("instanceId")
	if project == "" || location == "" || !validID(id) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project, location, and a valid instanceId are required")
		return
	}
	var instance Instance
	if err := decodeJSON(r, &instance); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid Redis instance JSON")
		return
	}
	if instance.MemorySizeGb <= 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "memorySizeGb must be greater than zero")
		return
	}
	if instance.Tier == "" {
		instance.Tier = "BASIC"
	}
	if instance.Tier != "BASIC" && instance.Tier != "STANDARD_HA" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "tier must be BASIC or STANDARD_HA")
		return
	}
	if instance.TransitEncryptionMode != "" && instance.TransitEncryptionMode != "DISABLED" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"transitEncryptionMode must be DISABLED because the local Valkey backend does not support TLS")
		return
	}
	if instance.ConnectMode != "" && instance.ConnectMode != "DIRECT_PEERING" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"connectMode must be DIRECT_PEERING because the local Valkey backend does not implement private service access")
		return
	}
	if instance.AuthorizedNetwork != "" &&
		!validRedisAuthorizedNetwork(project, instance.AuthorizedNetwork) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"authorizedNetwork must be a canonical network in the Redis instance project")
		return
	}
	if err := validateRedisPersistenceConfig(instance.PersistenceConfig); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if instance.RedisVersion == "" {
		instance.RedisVersion = "REDIS_7_2"
	}
	backendResource := backendID(project, location, id)
	backendSpec, err := redisBackendSpec(instance.RedisVersion, backendResource)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	name := fmt.Sprintf("projects/%s/locations/%s/instances/%s", project, location, id)
	key := name
	instance.Name = name
	instance.LocationId = location
	instance.State = "CREATING"
	instance.CreateTime = time.Now().UTC().Format(time.RFC3339)
	instance.BackendID = backendResource

	api.transactionMu.Lock()
	api.mu.Lock()
	if _, exists := api.instances[key]; exists {
		api.mu.Unlock()
		api.transactionMu.Unlock()
		writeError(w, http.StatusConflict, "ALREADY_EXISTS", "Redis instance already exists: "+id)
		return
	}
	managerOp, err := api.opMgr.RegisterDurable("redis#operation", "CREATE", name, "", location)
	if err != nil {
		api.mu.Unlock()
		api.transactionMu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Redis operation")
		return
	}
	serviceName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, managerOp.Name)
	api.instances[key] = cloneInstance(&instance)
	api.backends[key] = backendSpec
	api.operations[serviceName] = operationTarget{ManagerName: managerOp.Name, ResourceKey: key}
	api.mu.Unlock()
	if err := api.persistLocked(); err != nil {
		api.mu.Lock()
		delete(api.operations, serviceName)
		delete(api.instances, key)
		delete(api.backends, key)
		api.mu.Unlock()
		api.transactionMu.Unlock()
		_ = api.opMgr.RollbackScopedRegistration(managerOp.Name)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Redis instance and operation")
		return
	}
	api.transactionMu.Unlock()
	api.opMgr.RunAsync(managerOp.Name, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), redisBackendTimeout)
		endpoint, resolved, err := api.backend.ProvisionRedis(ctx, backendSpec)
		cancel()
		if err != nil {
			var cleanupErr error
			if resolved.ContainerID != "" && resolved.VolumeIdentity != "" {
				cleanupErr = api.discardRedisProvisional(resolved, true)
			}
			api.transactionMu.Lock()
			api.mu.Lock()
			if cleanupErr == nil {
				delete(api.instances, key)
				delete(api.backends, key)
			} else if current := api.instances[key]; current != nil {
				current.State = "REPAIRING"
				current.Host = ""
				current.Port = 0
			}
			api.mu.Unlock()
			persistErr := api.persistLocked()
			api.transactionMu.Unlock()
			return errors.Join(err, cleanupErr, persistErr)
		}
		host, port, parseErr := parseLoopbackEndpoint(endpoint)
		if parseErr != nil {
			cleanupErr := api.discardRedisProvisional(resolved, true)
			api.transactionMu.Lock()
			api.mu.Lock()
			if cleanupErr == nil {
				delete(api.instances, key)
				delete(api.backends, key)
			} else if current := api.instances[key]; current != nil {
				api.backends[key] = resolved
				current.State = "REPAIRING"
				current.Host = ""
				current.Port = 0
			}
			api.mu.Unlock()
			persistErr := api.persistLocked()
			api.transactionMu.Unlock()
			return errors.Join(parseErr, cleanupErr, persistErr)
		}
		api.transactionMu.Lock()
		api.mu.Lock()
		previousInstance := cloneInstance(api.instances[key])
		previousBackend := api.backends[key]
		current := api.instances[key]
		api.backends[key] = resolved
		if current != nil {
			current.State = "READY"
			current.Host = host
			current.Port = port
		}
		api.mu.Unlock()
		persistErr := api.persistLocked()
		if persistErr != nil {
			api.mu.Lock()
			if previousInstance == nil {
				delete(api.instances, key)
			} else {
				api.instances[key] = previousInstance
			}
			api.backends[key] = previousBackend
			api.mu.Unlock()
		}
		api.transactionMu.Unlock()
		if persistErr != nil {
			cleanupErr := api.discardRedisProvisional(resolved, true)
			var compensationErr error
			api.transactionMu.Lock()
			api.mu.Lock()
			if cleanupErr == nil {
				delete(api.instances, key)
				delete(api.backends, key)
			} else {
				api.backends[key] = resolved
				if current := api.instances[key]; current != nil {
					current.State = "REPAIRING"
					current.Host = ""
					current.Port = 0
				}
			}
			api.mu.Unlock()
			compensationErr = api.persistLocked()
			api.transactionMu.Unlock()
			return errors.Join(persistErr, cleanupErr, compensationErr)
		}
		publishCtx, publishCancel := context.WithTimeout(context.Background(), redisBackendTimeout)
		publishErr := api.backend.PublishRedis(publishCtx, resolved)
		publishCancel()
		if publishErr != nil {
			cleanupErr := api.discardRedisProvisional(resolved, true)
			api.transactionMu.Lock()
			api.mu.Lock()
			if cleanupErr == nil {
				delete(api.instances, key)
				delete(api.backends, key)
			} else if current := api.instances[key]; current != nil {
				current.State = "REPAIRING"
				current.Host = ""
				current.Port = 0
			}
			api.mu.Unlock()
			compensationErr := api.persistLocked()
			api.transactionMu.Unlock()
			return errors.Join(publishErr, cleanupErr, compensationErr)
		}
		api.pushLog(project, "INFO", id, "Redis instance is READY")
		return nil
	})
	api.writeOperation(w, serviceName, managerOp)
}

func (api *API) listInstances(w http.ResponseWriter, r *http.Request) {
	project, location := projectLocation(r.URL.Path)
	prefix := fmt.Sprintf("projects/%s/locations/%s/instances/", project, location)
	api.transactionMu.RLock()
	defer api.transactionMu.RUnlock()
	api.mu.RLock()
	instances := make([]*Instance, 0)
	for key, instance := range api.instances {
		if strings.HasPrefix(key, prefix) {
			instances = append(instances, cloneInstance(instance))
		}
	}
	api.mu.RUnlock()
	sort.Slice(instances, func(i, j int) bool { return instances[i].Name < instances[j].Name })
	_ = json.NewEncoder(w).Encode(map[string]any{"instances": instances})
}

func (api *API) getInstance(w http.ResponseWriter, r *http.Request) {
	name := instanceName(r.URL.Path)
	api.transactionMu.RLock()
	defer api.transactionMu.RUnlock()
	api.mu.RLock()
	instance := cloneInstance(api.instances[name])
	api.mu.RUnlock()
	if instance == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Redis instance not found")
		return
	}
	_ = json.NewEncoder(w).Encode(instance)
}

func (api *API) deleteInstance(w http.ResponseWriter, r *http.Request) {
	name := instanceName(r.URL.Path)
	api.transactionMu.Lock()
	api.mu.Lock()
	instance := api.instances[name]
	if instance == nil {
		api.mu.Unlock()
		api.transactionMu.Unlock()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Redis instance not found")
		return
	}
	backendSpec, ok := api.backends[name]
	if !ok {
		api.mu.Unlock()
		api.transactionMu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE",
			"Redis backend immutable identity is unavailable")
		return
	}
	project, location, id := instanceParts(name)
	previous := cloneInstance(instance)
	managerOp, err := api.opMgr.RegisterDurable("redis#operation", "DELETE", name, "", location)
	if err != nil {
		api.mu.Unlock()
		api.transactionMu.Unlock()
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Redis operation")
		return
	}
	serviceName := fmt.Sprintf("projects/%s/locations/%s/operations/%s", project, location, managerOp.Name)
	instance.State = "DELETING"
	api.operations[serviceName] = operationTarget{ManagerName: managerOp.Name, ResourceKey: name, Delete: true}
	api.mu.Unlock()
	if err := api.persistLocked(); err != nil {
		api.mu.Lock()
		delete(api.operations, serviceName)
		api.instances[name] = previous
		api.mu.Unlock()
		api.transactionMu.Unlock()
		_ = api.opMgr.RollbackScopedRegistration(managerOp.Name)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "failed to persist Redis deletion")
		return
	}
	api.transactionMu.Unlock()
	api.opMgr.RunAsync(managerOp.Name, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), redisBackendTimeout)
		err := api.backend.DeleteRedis(ctx, backendSpec)
		cancel()
		api.transactionMu.Lock()
		if err != nil {
			api.mu.Lock()
			if current := api.instances[name]; current != nil {
				current.State = "REPAIRING"
				current.Host = ""
				current.Port = 0
			}
			api.mu.Unlock()
			if persistErr := api.persistLocked(); persistErr != nil {
				api.transactionMu.Unlock()
				return errors.Join(err, persistErr)
			}
			api.transactionMu.Unlock()
			return err
		}
		api.mu.Lock()
		delete(api.instances, name)
		delete(api.backends, name)
		api.mu.Unlock()
		if err := api.persistLocked(); err != nil {
			api.transactionMu.Unlock()
			return err
		}
		api.transactionMu.Unlock()
		api.pushLog(project, "INFO", id, "Redis instance deleted")
		return nil
	})
	api.writeOperation(w, serviceName, managerOp)
}

func (api *API) cleanupRedisBackend(spec orchestrator.RedisBackendSpec) error {
	ctx, cancel := context.WithTimeout(context.Background(), redisBackendTimeout)
	defer cancel()
	return api.backend.DeleteRedis(ctx, spec)
}

func (api *API) discardRedisProvisional(
	spec orchestrator.RedisBackendSpec,
	removeVolume bool,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), redisBackendTimeout)
	defer cancel()
	return api.backend.DiscardRedis(ctx, spec, removeVolume)
}

func (api *API) getOperation(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/v1/")
	api.transactionMu.RLock()
	defer api.transactionMu.RUnlock()
	api.mu.RLock()
	target, ok := api.operations[name]
	api.mu.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Redis operation not found")
		return
	}
	operation := api.opMgr.Get(target.ManagerName)
	if operation == nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Redis operation not found")
		return
	}
	api.writeOperation(w, name, operation)
}

func (api *API) writeOperation(w http.ResponseWriter, name string, operation *orchestrator.Operation) {
	response := map[string]any{
		"name": name,
		"done": operation.Done,
		"metadata": map[string]any{
			"target": operation.TargetLink,
			"verb":   operation.OperationType,
		},
	}
	if operation.Error != nil {
		response["error"] = operation.Error
	} else if operation.Done {
		api.mu.RLock()
		target := api.operations[name]
		instance := cloneInstance(api.instances[target.ResourceKey])
		api.mu.RUnlock()
		if target.Delete {
			response["response"] = map[string]any{}
		} else if instance != nil {
			response["response"] = instance
		}
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (api *API) persist() error {
	api.transactionMu.RLock()
	defer api.transactionMu.RUnlock()
	return api.persistLocked()
}

// persistLocked snapshots and saves while the caller owns transactionMu for
// mutation, or while persist holds its read side for a standalone snapshot.
func (api *API) persistLocked() error {
	if api.store == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	instances := make(map[string]persistedInstance, len(api.instances))
	for key, instance := range api.instances {
		instances[key] = persistedInstance{
			Instance:  instance,
			BackendID: instance.BackendID,
			Backend:   api.backends[key],
		}
	}
	operations := make(map[string]operationTarget, len(api.operations))
	for name, target := range api.operations {
		operations[name] = target
	}
	payload, err := json.Marshal(redisMetadata{
		Schema: redisMetadataSchema, Version: redisMetadataVersion,
		Instances: instances, Operations: operations,
	})
	api.mu.RUnlock()
	if err != nil {
		return err
	}
	return api.store.Save(memorystoreStateEntry, json.RawMessage(payload))
}

func (api *API) pushLog(project, severity, id, message string) {
	if api.logAPI != nil {
		api.logAPI.PushLog(project, severity, "memorystore_instance", id, message)
	}
}

func redisBackendSpec(version, resourceID string) (orchestrator.RedisBackendSpec, error) {
	if version != "REDIS_7_2" {
		return orchestrator.RedisBackendSpec{}, fmt.Errorf("unsupported Redis version %q", version)
	}
	return orchestrator.Redis72BackendSpec(resourceID), nil
}

func parseLoopbackEndpoint(endpoint string) (string, int, error) {
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", 0, fmt.Errorf("invalid Redis endpoint: %w", err)
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return "", 0, fmt.Errorf("Redis endpoint is not loopback")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("invalid Redis endpoint port")
	}
	return host, port, nil
}

func projectLocation(path string) (string, string) {
	return extractAfter(path, "projects"), extractAfter(path, "locations")
}

func instanceName(path string) string {
	project, location := projectLocation(path)
	return fmt.Sprintf("projects/%s/locations/%s/instances/%s", project, location, extractAfter(path, "instances"))
}

func instanceParts(name string) (string, string, string) {
	return extractAfter(name, "projects"), extractAfter(name, "locations"), extractAfter(name, "instances")
}

func extractAfter(path, segment string) string {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part == segment && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func validID(id string) bool {
	if len(id) < 1 || len(id) > 40 {
		return false
	}
	for i, char := range id {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || (char == '-' && i > 0) {
			continue
		}
		return false
	}
	return id[len(id)-1] != '-'
}

func backendID(project, location, id string) string {
	hasher := sha256.New()
	for _, part := range []string{project, location, id} {
		_ = binary.Write(hasher, binary.BigEndian, uint32(len(part)))
		_, _ = hasher.Write([]byte(part))
	}
	return fmt.Sprintf("redis-%x", hasher.Sum(nil)[:16])
}

func cloneInstance(instance *Instance) *Instance {
	if instance == nil {
		return nil
	}
	clone := *instance
	if instance.Labels != nil {
		clone.Labels = make(map[string]string, len(instance.Labels))
		for key, value := range instance.Labels {
			clone.Labels[key] = value
		}
	}
	if instance.PersistenceConfig != nil {
		config := *instance.PersistenceConfig
		clone.PersistenceConfig = &config
	}
	return &clone
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "status": status, "message": message, "details": []any{}},
	})
}
