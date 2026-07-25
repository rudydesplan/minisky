package iam

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"

	"minisky/pkg/orchestrator"
)

var (
	workloadIdentityIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{3,31}$`)
	projectIDPattern          = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
)

const (
	maxWorkloadIdentityRequestBytes = 1 << 20
	maxWorkloadIdentityJWKSBytes    = 64 << 10
)

// WorkloadIdentityPool is the metadata-only local representation of an IAM
// workload identity pool. MiniSky does not execute external identity exchange.
type WorkloadIdentityPool struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName,omitempty"`
	Description string `json:"description,omitempty"`
	Disabled    bool   `json:"disabled,omitempty"`
	State       string `json:"state"`
}

type WorkloadIdentityPoolOIDC struct {
	IssuerURI        string   `json:"issuerUri,omitempty"`
	AllowedAudiences []string `json:"allowedAudiences,omitempty"`
	JWKSJSON         string   `json:"jwksJson,omitempty"`
}

// WorkloadIdentityPoolProvider stores provider configuration without claiming
// to execute unsupported AWS, SAML, or X.509 exchanges.
type WorkloadIdentityPoolProvider struct {
	Name               string                    `json:"name"`
	DisplayName        string                    `json:"displayName,omitempty"`
	Description        string                    `json:"description,omitempty"`
	Disabled           bool                      `json:"disabled,omitempty"`
	State              string                    `json:"state"`
	AttributeMapping   map[string]string         `json:"attributeMapping,omitempty"`
	AttributeCondition string                    `json:"attributeCondition,omitempty"`
	OIDC               *WorkloadIdentityPoolOIDC `json:"oidc,omitempty"`
	AWS                map[string]any            `json:"aws,omitempty"`
	SAML               map[string]any            `json:"saml,omitempty"`
	X509               map[string]any            `json:"x509,omitempty"`
}

// WorkloadIdentityProviderConfig is an immutable-by-convention snapshot used
// by the local STS shim. Both resources are deep-copied while IAM's read lock
// is held, so callers cannot race with or mutate persisted control-plane state.
type WorkloadIdentityProviderConfig struct {
	Pool     *WorkloadIdentityPool
	Provider *WorkloadIdentityPoolProvider
}

// LookupWorkloadIdentityProvider resolves only the canonical WIF STS audience
// form. It performs direct map lookups rather than accepting audience aliases.
func (api *API) LookupWorkloadIdentityProvider(audience string) (*WorkloadIdentityProviderConfig, bool) {
	poolName, providerName, ok := parseWorkloadIdentityProviderAudience(audience)
	if !ok {
		return nil, false
	}

	api.mu.RLock()
	pool := cloneWorkloadIdentityPool(api.workloadIdentityPools[poolName])
	provider := cloneWorkloadIdentityProvider(api.workloadIdentityProviders[providerName])
	api.mu.RUnlock()
	if pool == nil || provider == nil {
		return nil, false
	}
	return &WorkloadIdentityProviderConfig{Pool: pool, Provider: provider}, true
}

func (api *API) routeWorkloadIdentity(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/"), "/")
	if strings.Contains(path, "/operations/") || strings.HasSuffix(path, "/operations") {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		api.getWorkloadIdentityOperation(w, path)
		return
	}
	if strings.HasSuffix(path, ":undelete") {
		writeErrorResponse(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Workload Identity Pool undelete is not supported")
		return
	}
	if strings.HasSuffix(path, ":listAttestationRules") {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
			return
		}
		api.listAttestationRules(w, strings.TrimSuffix(path, ":listAttestationRules"))
		return
	}

	poolMarker := "/workloadIdentityPools"
	position := strings.Index(path, poolMarker)
	if position < 0 {
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", "Workload Identity Pool resource not found")
		return
	}
	parent := path[:position]
	if !validWorkloadIdentityParent(parent) {
		writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			"parent must match projects/{projectID}/locations/global")
		return
	}
	suffix := strings.TrimPrefix(path[position+len(poolMarker):], "/")
	parts := strings.Split(suffix, "/")
	poolID := ""
	if suffix != "" {
		poolID = parts[0]
	}

	if len(parts) >= 2 && parts[1] == "providers" {
		if len(parts) > 3 {
			writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", "Workload Identity Pool Provider resource not found")
			return
		}
		providerID := ""
		if len(parts) >= 3 {
			providerID = parts[2]
		}
		api.routeWorkloadIdentityProviders(w, r, parent, poolID, providerID)
		return
	}
	if len(parts) > 1 {
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", "Workload Identity Pool resource not found")
		return
	}

	switch r.Method {
	case http.MethodPost:
		if suffix != "" {
			writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", "Workload Identity Pool resource not found")
			return
		}
		api.createWorkloadIdentityPool(w, r, parent)
	case http.MethodGet:
		if poolID == "" {
			api.listWorkloadIdentityPools(w, parent)
		} else {
			api.getWorkloadIdentityPool(w, parent+"/workloadIdentityPools/"+poolID)
		}
	case http.MethodPatch:
		if poolID == "" {
			writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Workload Identity Pool name is required")
			return
		}
		api.patchWorkloadIdentityPool(w, r, parent+"/workloadIdentityPools/"+poolID)
	case http.MethodDelete:
		if poolID == "" {
			writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Workload Identity Pool name is required")
			return
		}
		api.deleteWorkloadIdentityPool(w, parent+"/workloadIdentityPools/"+poolID)
	default:
		writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createWorkloadIdentityPool(w http.ResponseWriter, r *http.Request, parent string) {
	id := r.URL.Query().Get("workloadIdentityPoolId")
	if !validWorkloadIdentityID(id) {
		writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT", "workloadIdentityPoolId is required and must be a valid resource ID")
		return
	}
	var pool WorkloadIdentityPool
	if err := decodeJSONBody(w, r, &pool); err != nil {
		writeWorkloadIdentityDecodeError(w, err)
		return
	}
	pool.Name = parent + "/workloadIdentityPools/" + id
	pool.State = "ACTIVE"

	op, result, mutationErr := api.commitWorkloadIdentityMutation("CREATE", pool.Name,
		func(pools map[string]*WorkloadIdentityPool, _ map[string]*WorkloadIdentityPoolProvider) (any, *workloadIdentityMutationError) {
			if _, exists := pools[pool.Name]; exists {
				return nil, &workloadIdentityMutationError{
					code: http.StatusConflict, status: "ALREADY_EXISTS",
					message: "WorkloadIdentityPool already exists: " + pool.Name,
				}
			}
			pools[pool.Name] = cloneWorkloadIdentityPool(&pool)
			return cloneWorkloadIdentityPool(&pool), nil
		})
	if mutationErr != nil {
		writeErrorResponse(w, mutationErr.code, mutationErr.status, mutationErr.message)
		return
	}
	api.writeCommittedWorkloadIdentityOperation(w, op, pool.Name, result)
}

func (api *API) getWorkloadIdentityPool(w http.ResponseWriter, name string) {
	api.mu.RLock()
	pool := cloneWorkloadIdentityPool(api.workloadIdentityPools[name])
	api.mu.RUnlock()
	if pool == nil {
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", "WorkloadIdentityPool not found: "+name)
		return
	}
	_ = json.NewEncoder(w).Encode(pool)
}

func (api *API) listWorkloadIdentityPools(w http.ResponseWriter, parent string) {
	prefix := parent + "/workloadIdentityPools/"
	api.mu.RLock()
	pools := make([]WorkloadIdentityPool, 0)
	for name, pool := range api.workloadIdentityPools {
		if strings.HasPrefix(name, prefix) {
			pools = append(pools, *cloneWorkloadIdentityPool(pool))
		}
	}
	api.mu.RUnlock()
	sort.Slice(pools, func(i, j int) bool { return pools[i].Name < pools[j].Name })
	_ = json.NewEncoder(w).Encode(map[string]any{"workloadIdentityPools": pools})
}

func (api *API) patchWorkloadIdentityPool(w http.ResponseWriter, r *http.Request, name string) {
	mask, ok := requiredUpdateMask(w, r)
	if !ok {
		return
	}
	if !validateUpdateMask(w, mask, "displayName", "description", "disabled") {
		return
	}
	var patch WorkloadIdentityPool
	if err := decodeJSONBody(w, r, &patch); err != nil {
		writeWorkloadIdentityDecodeError(w, err)
		return
	}
	op, result, mutationErr := api.commitWorkloadIdentityMutation("UPDATE", name,
		func(pools map[string]*WorkloadIdentityPool, _ map[string]*WorkloadIdentityPoolProvider) (any, *workloadIdentityMutationError) {
			pool := pools[name]
			if pool == nil {
				return nil, &workloadIdentityMutationError{
					code: http.StatusNotFound, status: "NOT_FOUND",
					message: "WorkloadIdentityPool not found: " + name,
				}
			}
			for _, field := range mask {
				switch normalizeMaskField(field) {
				case "displayName":
					pool.DisplayName = patch.DisplayName
				case "description":
					pool.Description = patch.Description
				case "disabled":
					pool.Disabled = patch.Disabled
				}
			}
			return cloneWorkloadIdentityPool(pool), nil
		})
	if mutationErr != nil {
		writeErrorResponse(w, mutationErr.code, mutationErr.status, mutationErr.message)
		return
	}
	api.writeCommittedWorkloadIdentityOperation(w, op, name, result)
}

func (api *API) deleteWorkloadIdentityPool(w http.ResponseWriter, name string) {
	op, result, mutationErr := api.commitWorkloadIdentityMutation("DELETE", name,
		func(pools map[string]*WorkloadIdentityPool, providers map[string]*WorkloadIdentityPoolProvider) (any, *workloadIdentityMutationError) {
			if pools[name] == nil {
				return nil, &workloadIdentityMutationError{
					code: http.StatusNotFound, status: "NOT_FOUND",
					message: "WorkloadIdentityPool not found: " + name,
				}
			}
			delete(pools, name)
			for providerName := range providers {
				if strings.HasPrefix(providerName, name+"/providers/") {
					delete(providers, providerName)
				}
			}
			return nil, nil
		})
	if mutationErr != nil {
		writeErrorResponse(w, mutationErr.code, mutationErr.status, mutationErr.message)
		return
	}
	api.writeCommittedWorkloadIdentityOperation(w, op, name, result)
}

func (api *API) routeWorkloadIdentityProviders(w http.ResponseWriter, r *http.Request, parent, poolID, providerID string) {
	poolName := parent + "/workloadIdentityPools/" + poolID
	if poolID == "" {
		writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Workload Identity Pool name is required")
		return
	}
	name := poolName + "/providers/" + providerID
	switch r.Method {
	case http.MethodPost:
		if providerID != "" {
			writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", "Workload Identity Pool Provider resource not found")
			return
		}
		api.createWorkloadIdentityProvider(w, r, poolName)
	case http.MethodGet:
		if providerID == "" {
			api.listWorkloadIdentityProviders(w, poolName)
		} else {
			api.getWorkloadIdentityProvider(w, name)
		}
	case http.MethodPatch:
		api.patchWorkloadIdentityProvider(w, r, name)
	case http.MethodDelete:
		api.deleteWorkloadIdentityProvider(w, name)
	default:
		writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createWorkloadIdentityProvider(w http.ResponseWriter, r *http.Request, poolName string) {
	id := r.URL.Query().Get("workloadIdentityPoolProviderId")
	if !validWorkloadIdentityID(id) {
		writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT", "workloadIdentityPoolProviderId is required and must be a valid resource ID")
		return
	}
	var provider WorkloadIdentityPoolProvider
	if err := decodeJSONBody(w, r, &provider); err != nil {
		writeWorkloadIdentityDecodeError(w, err)
		return
	}
	if !validateWorkloadIdentityJWKS(w, &provider) {
		return
	}
	provider.Name = poolName + "/providers/" + id
	provider.State = "ACTIVE"

	op, result, mutationErr := api.commitWorkloadIdentityMutation("CREATE", provider.Name,
		func(pools map[string]*WorkloadIdentityPool, providers map[string]*WorkloadIdentityPoolProvider) (any, *workloadIdentityMutationError) {
			if pools[poolName] == nil {
				return nil, &workloadIdentityMutationError{
					code: http.StatusNotFound, status: "NOT_FOUND",
					message: "WorkloadIdentityPool not found: " + poolName,
				}
			}
			if _, exists := providers[provider.Name]; exists {
				return nil, &workloadIdentityMutationError{
					code: http.StatusConflict, status: "ALREADY_EXISTS",
					message: "WorkloadIdentityPoolProvider already exists: " + provider.Name,
				}
			}
			providers[provider.Name] = cloneWorkloadIdentityProvider(&provider)
			return cloneWorkloadIdentityProvider(&provider), nil
		})
	if mutationErr != nil {
		writeErrorResponse(w, mutationErr.code, mutationErr.status, mutationErr.message)
		return
	}
	api.writeCommittedWorkloadIdentityOperation(w, op, provider.Name, result)
}

func (api *API) getWorkloadIdentityProvider(w http.ResponseWriter, name string) {
	api.mu.RLock()
	provider := cloneWorkloadIdentityProvider(api.workloadIdentityProviders[name])
	api.mu.RUnlock()
	if provider == nil {
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", "WorkloadIdentityPoolProvider not found: "+name)
		return
	}
	_ = json.NewEncoder(w).Encode(provider)
}

func (api *API) listWorkloadIdentityProviders(w http.ResponseWriter, poolName string) {
	prefix := poolName + "/providers/"
	api.mu.RLock()
	if api.workloadIdentityPools[poolName] == nil {
		api.mu.RUnlock()
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", "WorkloadIdentityPool not found: "+poolName)
		return
	}
	providers := make([]WorkloadIdentityPoolProvider, 0)
	for name, provider := range api.workloadIdentityProviders {
		if strings.HasPrefix(name, prefix) {
			providers = append(providers, *cloneWorkloadIdentityProvider(provider))
		}
	}
	api.mu.RUnlock()
	sort.Slice(providers, func(i, j int) bool { return providers[i].Name < providers[j].Name })
	_ = json.NewEncoder(w).Encode(map[string]any{"workloadIdentityPoolProviders": providers})
}

func (api *API) patchWorkloadIdentityProvider(w http.ResponseWriter, r *http.Request, name string) {
	mask, ok := requiredUpdateMask(w, r)
	if !ok {
		return
	}
	if !validateUpdateMask(w, mask,
		"displayName", "description", "disabled", "attributeMapping", "attributeCondition",
		"oidc", "oidc.allowedAudiences", "oidc.issuerUri", "oidc.jwksJson", "aws", "saml", "x509",
	) {
		return
	}
	var patch WorkloadIdentityPoolProvider
	if err := decodeJSONBody(w, r, &patch); err != nil {
		writeWorkloadIdentityDecodeError(w, err)
		return
	}
	if !validateWorkloadIdentityJWKS(w, &patch) {
		return
	}
	op, result, mutationErr := api.commitWorkloadIdentityMutation("UPDATE", name,
		func(_ map[string]*WorkloadIdentityPool, providers map[string]*WorkloadIdentityPoolProvider) (any, *workloadIdentityMutationError) {
			provider := providers[name]
			if provider == nil {
				return nil, &workloadIdentityMutationError{
					code: http.StatusNotFound, status: "NOT_FOUND",
					message: "WorkloadIdentityPoolProvider not found: " + name,
				}
			}
			for _, field := range mask {
				switch normalizeMaskField(field) {
				case "displayName":
					provider.DisplayName = patch.DisplayName
				case "description":
					provider.Description = patch.Description
				case "disabled":
					provider.Disabled = patch.Disabled
				case "attributeMapping":
					provider.AttributeMapping = cloneStringMap(patch.AttributeMapping)
				case "attributeCondition":
					provider.AttributeCondition = patch.AttributeCondition
				case "oidc":
					provider.OIDC = cloneOIDC(patch.OIDC)
				case "oidc.allowedAudiences":
					if provider.OIDC == nil {
						provider.OIDC = &WorkloadIdentityPoolOIDC{}
					}
					if patch.OIDC != nil {
						provider.OIDC.AllowedAudiences = append([]string(nil), patch.OIDC.AllowedAudiences...)
					}
				case "oidc.issuerUri":
					if provider.OIDC == nil {
						provider.OIDC = &WorkloadIdentityPoolOIDC{}
					}
					if patch.OIDC != nil {
						provider.OIDC.IssuerURI = patch.OIDC.IssuerURI
					}
				case "oidc.jwksJson":
					if provider.OIDC == nil {
						provider.OIDC = &WorkloadIdentityPoolOIDC{}
					}
					if patch.OIDC != nil {
						provider.OIDC.JWKSJSON = patch.OIDC.JWKSJSON
					}
				case "aws":
					provider.AWS = cloneAnyMap(patch.AWS)
				case "saml":
					provider.SAML = cloneAnyMap(patch.SAML)
				case "x509":
					provider.X509 = cloneAnyMap(patch.X509)
				}
			}
			return cloneWorkloadIdentityProvider(provider), nil
		})
	if mutationErr != nil {
		writeErrorResponse(w, mutationErr.code, mutationErr.status, mutationErr.message)
		return
	}
	api.writeCommittedWorkloadIdentityOperation(w, op, name, result)
}

func (api *API) deleteWorkloadIdentityProvider(w http.ResponseWriter, name string) {
	op, result, mutationErr := api.commitWorkloadIdentityMutation("DELETE", name,
		func(_ map[string]*WorkloadIdentityPool, providers map[string]*WorkloadIdentityPoolProvider) (any, *workloadIdentityMutationError) {
			if providers[name] == nil {
				return nil, &workloadIdentityMutationError{
					code: http.StatusNotFound, status: "NOT_FOUND",
					message: "WorkloadIdentityPoolProvider not found: " + name,
				}
			}
			delete(providers, name)
			return nil, nil
		})
	if mutationErr != nil {
		writeErrorResponse(w, mutationErr.code, mutationErr.status, mutationErr.message)
		return
	}
	api.writeCommittedWorkloadIdentityOperation(w, op, name, result)
}

func (api *API) listAttestationRules(w http.ResponseWriter, poolName string) {
	api.mu.RLock()
	pool := api.workloadIdentityPools[poolName]
	api.mu.RUnlock()
	if pool == nil {
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", "WorkloadIdentityPool not found: "+poolName)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"attestationRules": []any{}})
}

type workloadIdentityMutationError struct {
	code    int
	status  string
	message string
}

func (api *API) commitWorkloadIdentityMutation(
	operationType, target string,
	mutate func(map[string]*WorkloadIdentityPool, map[string]*WorkloadIdentityPoolProvider) (any, *workloadIdentityMutationError),
) (*orchestrator.Operation, any, *workloadIdentityMutationError) {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	metadata := cloneIAMMetadata(api.serviceAccounts, api.keys, api.policies)
	metadata.WorkloadIdentityPools = cloneWorkloadIdentityPools(api.workloadIdentityPools)
	metadata.WorkloadIdentityProviders = cloneWorkloadIdentityProviders(api.workloadIdentityProviders)
	api.mu.RUnlock()

	response, mutationErr := mutate(metadata.WorkloadIdentityPools, metadata.WorkloadIdentityProviders)
	if mutationErr != nil {
		return nil, nil, mutationErr
	}

	var op *orchestrator.Operation
	if api.opMgr != nil {
		var err error
		op, err = api.opMgr.RegisterDurable("iam#workloadIdentityOperation", operationType, target, "", "global")
		if err != nil {
			return nil, nil, &workloadIdentityMutationError{
				code: http.StatusInternalServerError, status: "INTERNAL",
				message: "failed to persist Workload Identity operation",
			}
		}
	}

	if api.store != nil {
		if err := api.store.Save(iamStateEntry, metadata); err != nil {
			if op != nil {
				_ = api.opMgr.FailDurable(op.Name, http.StatusInternalServerError, "failed to persist IAM metadata")
			}
			return nil, nil, &workloadIdentityMutationError{
				code: http.StatusInternalServerError, status: "INTERNAL",
				message: "Failed to persist IAM metadata",
			}
		}
	}

	api.mu.Lock()
	api.workloadIdentityPools = metadata.WorkloadIdentityPools
	api.workloadIdentityProviders = metadata.WorkloadIdentityProviders
	api.mu.Unlock()
	return op, response, nil
}

func (api *API) writeCommittedWorkloadIdentityOperation(w http.ResponseWriter, op *orchestrator.Operation, target string, response any) {
	if op == nil {
		if response == nil {
			response = map[string]any{}
		}
		_ = json.NewEncoder(w).Encode(response)
		return
	}
	serviceName := workloadIdentityOperationParent(target) + "/operations/" + op.Name
	writeWorkloadIdentityOperationResponse(w, serviceName, op, response)
	api.opMgr.RunAsync(op.Name, func() error { return nil })
}

func workloadIdentityOperationParent(target string) string {
	if position := strings.Index(target, "/providers/"); position >= 0 {
		return target[:position]
	}
	return target
}

func (api *API) getWorkloadIdentityOperation(w http.ResponseWriter, serviceName string) {
	const marker = "/operations/"
	position := strings.LastIndex(serviceName, marker)
	if position < 0 || strings.TrimSpace(serviceName[position+len(marker):]) == "" {
		writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT", "operation name is malformed")
		return
	}
	managerName := serviceName[position+len(marker):]
	if strings.Contains(managerName, "/") || api.opMgr == nil {
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", "Workload Identity operation not found: "+serviceName)
		return
	}
	op := api.opMgr.Get(managerName)
	if op == nil ||
		op.Kind != "iam#workloadIdentityOperation" ||
		serviceName[:position] != workloadIdentityOperationParent(op.TargetLink) {
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", "Workload Identity operation not found: "+serviceName)
		return
	}
	var response any
	if op.Done && op.OperationType != "DELETE" {
		api.mu.RLock()
		if provider := api.workloadIdentityProviders[op.TargetLink]; provider != nil {
			response = cloneWorkloadIdentityProvider(provider)
		} else {
			response = cloneWorkloadIdentityPool(api.workloadIdentityPools[op.TargetLink])
		}
		api.mu.RUnlock()
	}
	writeWorkloadIdentityOperationResponse(w, serviceName, op, response)
}

func writeWorkloadIdentityOperationResponse(w http.ResponseWriter, serviceName string, op *orchestrator.Operation, response any) {
	body := map[string]any{
		"name": serviceName,
		"done": op.Done,
		"metadata": map[string]any{
			"target": op.TargetLink,
			"verb":   op.OperationType,
		},
	}
	if op.Error != nil {
		body["error"] = map[string]any{"code": op.Error.Code, "message": op.Error.Message}
	} else if op.Done {
		if response == nil {
			response = map[string]any{}
		}
		body["response"] = response
	}
	_ = json.NewEncoder(w).Encode(body)
}

func requiredUpdateMask(w http.ResponseWriter, r *http.Request) ([]string, bool) {
	value := strings.TrimSpace(r.URL.Query().Get("updateMask"))
	if value == "" {
		writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT", "updateMask is required")
		return nil, false
	}
	fields := strings.Split(value, ",")
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
		if fields[i] == "" {
			writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT", "updateMask contains an empty field")
			return nil, false
		}
	}
	return fields, true
}

func validateUpdateMask(w http.ResponseWriter, fields []string, allowed ...string) bool {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		allowedSet[field] = struct{}{}
	}
	for _, field := range fields {
		if _, ok := allowedSet[normalizeMaskField(field)]; !ok {
			writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT", "unsupported updateMask field: "+field)
			return false
		}
	}
	return true
}

func normalizeMaskField(field string) string {
	switch field {
	case "display_name":
		return "displayName"
	case "attribute_mapping":
		return "attributeMapping"
	case "attribute_condition":
		return "attributeCondition"
	case "oidc.allowed_audiences":
		return "oidc.allowedAudiences"
	case "oidc.issuer_uri":
		return "oidc.issuerUri"
	case "oidc.jwks_json":
		return "oidc.jwksJson"
	default:
		return field
	}
}

func validWorkloadIdentityID(id string) bool {
	return workloadIdentityIDPattern.MatchString(id) && !strings.HasPrefix(id, "gcp-")
}

func validWorkloadIdentityParent(parent string) bool {
	parts := strings.Split(parent, "/")
	return len(parts) == 4 &&
		parts[0] == "projects" &&
		projectIDPattern.MatchString(parts[1]) &&
		parts[2] == "locations" &&
		parts[3] == "global"
}

// ValidWorkloadIdentityProviderAudience reports whether audience is the
// canonical local project-ID form accepted by MiniSky's WIF implementation.
func ValidWorkloadIdentityProviderAudience(audience string) bool {
	_, _, ok := parseWorkloadIdentityProviderAudience(audience)
	return ok
}

func parseWorkloadIdentityProviderAudience(audience string) (poolName, providerName string, ok bool) {
	const prefix = "//iam.googleapis.com/"
	if !strings.HasPrefix(audience, prefix) {
		return "", "", false
	}
	providerName = strings.TrimPrefix(audience, prefix)
	parts := strings.Split(providerName, "/")
	if len(parts) != 8 ||
		!validWorkloadIdentityParent(strings.Join(parts[:4], "/")) ||
		parts[4] != "workloadIdentityPools" ||
		!validWorkloadIdentityID(parts[5]) ||
		parts[6] != "providers" ||
		!validWorkloadIdentityID(parts[7]) {
		return "", "", false
	}
	poolName = strings.Join(parts[:6], "/")
	return poolName, providerName, true
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, target any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxWorkloadIdentityRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("request body contains trailing JSON")
		}
		return err
	}
	return nil
}

func writeWorkloadIdentityDecodeError(w http.ResponseWriter, err error) {
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		writeErrorResponse(w, http.StatusRequestEntityTooLarge, "RESOURCE_EXHAUSTED", "request body exceeds 1 MiB")
		return
	}
	writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Parse error: "+err.Error())
}

func validateWorkloadIdentityJWKS(w http.ResponseWriter, provider *WorkloadIdentityPoolProvider) bool {
	if provider.OIDC != nil && len(provider.OIDC.JWKSJSON) > maxWorkloadIdentityJWKSBytes {
		writeErrorResponse(w, http.StatusBadRequest, "INVALID_ARGUMENT", "oidc.jwksJson exceeds 64 KiB")
		return false
	}
	return true
}

func writeErrorResponse(w http.ResponseWriter, code int, status, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
			"status":  status,
			"details": []any{},
		},
	})
}

func cloneWorkloadIdentityPool(pool *WorkloadIdentityPool) *WorkloadIdentityPool {
	if pool == nil {
		return nil
	}
	result := *pool
	return &result
}

func cloneWorkloadIdentityPools(pools map[string]*WorkloadIdentityPool) map[string]*WorkloadIdentityPool {
	result := make(map[string]*WorkloadIdentityPool, len(pools))
	for name, pool := range pools {
		result[name] = cloneWorkloadIdentityPool(pool)
	}
	return result
}

func cloneWorkloadIdentityProvider(provider *WorkloadIdentityPoolProvider) *WorkloadIdentityPoolProvider {
	if provider == nil {
		return nil
	}
	result := *provider
	result.AttributeMapping = cloneStringMap(provider.AttributeMapping)
	result.OIDC = cloneOIDC(provider.OIDC)
	result.AWS = cloneAnyMap(provider.AWS)
	result.SAML = cloneAnyMap(provider.SAML)
	result.X509 = cloneAnyMap(provider.X509)
	return &result
}

func cloneWorkloadIdentityProviders(providers map[string]*WorkloadIdentityPoolProvider) map[string]*WorkloadIdentityPoolProvider {
	result := make(map[string]*WorkloadIdentityPoolProvider, len(providers))
	for name, provider := range providers {
		result[name] = cloneWorkloadIdentityProvider(provider)
	}
	return result
}

func cloneOIDC(oidc *WorkloadIdentityPoolOIDC) *WorkloadIdentityPoolOIDC {
	if oidc == nil {
		return nil
	}
	result := *oidc
	result.AllowedAudiences = append([]string(nil), oidc.AllowedAudiences...)
	return &result
}

func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func cloneAnyMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	encoded, err := json.Marshal(source)
	if err != nil {
		return nil
	}
	var result map[string]any
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil
	}
	return result
}
