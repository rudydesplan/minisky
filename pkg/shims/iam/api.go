package iam

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	localsecurity "minisky/pkg/security"
	"minisky/pkg/state"
)

const principalHeader = "X-MiniSky-Principal"

func init() {
	registry.Register("iam.googleapis.com", func(ctx *registry.Context) http.Handler {
		api := NewAPI()
		api.opMgr = ctx.OpMgr
		return api
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types
// ─────────────────────────────────────────────────────────────────────────────

// ServiceAccount mirrors the GCP IAM ServiceAccount resource.
type ServiceAccount struct {
	Name           string `json:"name"`
	ProjectId      string `json:"projectId"`
	UniqueId       string `json:"uniqueId"`
	Email          string `json:"email"`
	DisplayName    string `json:"displayName,omitempty"`
	Description    string `json:"description,omitempty"`
	Disabled       bool   `json:"disabled"`
	Etag           string `json:"etag"`
	OAuth2ClientId string `json:"oauth2ClientId"`
}

// ServiceAccountKey mirrors the GCP IAM ServiceAccountKey resource.
// The PrivateKeyData is a base64-encoded fake JSON service account key file.
type ServiceAccountKey struct {
	Name            string `json:"name"`
	KeyType         string `json:"keyType"`        // USER_MANAGED
	KeyOrigin       string `json:"keyOrigin"`      // GOOGLE_PROVIDED
	KeyAlgorithm    string `json:"keyAlgorithm"`   // KEY_ALG_RSA_2048
	PrivateKeyType  string `json:"privateKeyType"` // TYPE_GOOGLE_CREDENTIALS_FILE
	PrivateKeyData  string `json:"privateKeyData"` // base64 JSON
	ValidAfterTime  string `json:"validAfterTime"`
	ValidBeforeTime string `json:"validBeforeTime"`
	Disabled        bool   `json:"disabled,omitempty"`
}

// IamPolicy mirrors the GCP IAM Policy binding structure.
type IamPolicy struct {
	Version  int       `json:"version"`
	Etag     string    `json:"etag"`
	Bindings []Binding `json:"bindings"`
}

// Binding is a single role→members entry in a Policy.
type Binding struct {
	Role    string   `json:"role"`
	Members []string `json:"members"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API is the high-fidelity IAM v1 shim.
// It handles service account CRUD, key generation, and IAM policy management.
type API struct {
	mu                        sync.RWMutex
	persistMu                 sync.Mutex
	store                     stateStore
	opMgr                     *orchestrator.OperationManager
	serviceAccounts           map[string]*ServiceAccount      // key: "project:email"
	keys                      map[string][]*ServiceAccountKey // key: "project:email"
	policies                  map[string]*IamPolicy           // key: resource full name
	workloadIdentityPools     map[string]*WorkloadIdentityPool
	workloadIdentityProviders map[string]*WorkloadIdentityPoolProvider
	strict                    bool
	issuer                    *localsecurity.Issuer
	tokenAudience             string
	hierarchy                 hierarchyResolver
	persistenceErr            error
}

type hierarchyResolver interface {
	Ancestors(resource string) []string
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

const iamStateEntry = "iam/metadata"

type iamMetadata struct {
	ServiceAccounts           map[string]*ServiceAccount               `json:"serviceAccounts"`
	Keys                      map[string][]*ServiceAccountKey          `json:"keys"`
	Policies                  map[string]*IamPolicy                    `json:"policies"`
	WorkloadIdentityPools     map[string]*WorkloadIdentityPool         `json:"workloadIdentityPools,omitempty"`
	WorkloadIdentityProviders map[string]*WorkloadIdentityPoolProvider `json:"workloadIdentityProviders,omitempty"`
}

func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		degraded := newAPI(nil)
		degraded.persistenceErr = fmt.Errorf("open IAM state: %w", err)
		log.Printf("[Shim: IAM] persistence degraded: %v", degraded.persistenceErr)
		return degraded
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		degraded := newAPI(store)
		degraded.persistenceErr = err
		log.Printf("[Shim: IAM] persistence degraded: %v", err)
		return degraded
	}
	if issuer, issuerErr := localsecurity.LoadIssuer(config.GetProfileDir()); issuerErr != nil {
		log.Printf("[Shim: IAM] local credential issuer unavailable: %v", issuerErr)
	} else {
		api.issuer = issuer
	}
	return api
}

// NewAPIWithStore constructs an IAM shim backed by the supplied metadata store.
// It reports unreadable state instead of silently replacing it.
func NewAPIWithStore(store stateStore) (*API, error) {
	api := newAPI(store)
	if store == nil {
		return api, nil
	}
	var persisted iamMetadata
	if err := store.Load(iamStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load IAM metadata: %w", err)
	}
	if persisted.ServiceAccounts != nil {
		api.serviceAccounts = persisted.ServiceAccounts
	}
	if persisted.Keys != nil {
		api.keys = persisted.Keys
	}
	if persisted.Policies != nil {
		api.policies = persisted.Policies
	}
	if persisted.WorkloadIdentityPools != nil {
		api.workloadIdentityPools = cloneWorkloadIdentityPools(persisted.WorkloadIdentityPools)
	}
	if persisted.WorkloadIdentityProviders != nil {
		api.workloadIdentityProviders = cloneWorkloadIdentityProviders(persisted.WorkloadIdentityProviders)
	}
	return api, nil
}

func newAPI(store stateStore) *API {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	return &API{
		store:                     store,
		serviceAccounts:           make(map[string]*ServiceAccount),
		keys:                      make(map[string][]*ServiceAccountKey),
		policies:                  make(map[string]*IamPolicy),
		workloadIdentityPools:     make(map[string]*WorkloadIdentityPool),
		workloadIdentityProviders: make(map[string]*WorkloadIdentityPoolProvider),
		strict:                    strings.EqualFold(strings.TrimSpace(os.Getenv("MINISKY_IAM_MODE")), "strict"),
		issuer:                    localsecurity.NewIssuer(key, time.Now),
		tokenAudience:             "minisky-gateway",
	}
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	if hierarchy, ok := ctx.GetShim("cloudresourcemanager.googleapis.com").(hierarchyResolver); ok {
		api.hierarchy = hierarchy
	}
}

func (api *API) persistMetadata() error {
	if api.store == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	metadata := cloneIAMMetadata(api.serviceAccounts, api.keys, api.policies)
	metadata.WorkloadIdentityPools = cloneWorkloadIdentityPools(api.workloadIdentityPools)
	metadata.WorkloadIdentityProviders = cloneWorkloadIdentityProviders(api.workloadIdentityProviders)
	api.mu.RUnlock()
	return api.store.Save(iamStateEntry, metadata)
}

func (api *API) PersistenceError() error {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.persistenceErr
}

// EnforcementEnabled reports whether cross-shim mutation checks are active.
func (api *API) EnforcementEnabled() bool {
	return api.strict
}

// Authorize evaluates one permission against the policy stored for resource.
// Callers supply the explicit local principal from X-MiniSky-Principal.
func (api *API) Authorize(resource, principal, permission string) bool {
	if !api.strict {
		return true
	}
	if strings.TrimSpace(principal) == "" {
		return false
	}
	canonical := strings.TrimPrefix(resource, "/v1/")
	resources := []string{canonical}
	if alias := serviceAccountPolicyAlias(canonical); alias != "" {
		resources = append(resources, alias)
	}
	parts := strings.Split(strings.Trim(canonical, "/"), "/")
	projectResource := ""
	for index, part := range parts {
		if part == "projects" && index+1 < len(parts) {
			projectResource = "projects/" + strings.TrimSuffix(parts[index+1], ":")
			break
		}
	}
	if projectResource != "" && projectResource != canonical {
		resources = append(resources, projectResource)
	}
	if api.hierarchy != nil {
		ancestorRoot := canonical
		if projectResource != "" {
			ancestorRoot = projectResource
		}
		for _, ancestor := range api.hierarchy.Ancestors(ancestorRoot) {
			if !containsString(resources, ancestor) {
				resources = append(resources, ancestor)
			}
		}
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	for _, candidate := range resources {
		policy := api.policies[candidate]
		if policy == nil {
			policy = api.policies["/v1/"+strings.TrimPrefix(candidate, "/")]
		}
		if _, allowed := permissionsForPrincipal(policy, principal)[permission]; allowed {
			return true
		}
	}
	return false
}

func serviceAccountPolicyAlias(resource string) string {
	const wildcardPrefix = "projects/-/serviceAccounts/"
	if !strings.HasPrefix(resource, wildcardPrefix) {
		return ""
	}
	email := strings.TrimPrefix(resource, wildcardPrefix)
	if email == "" || strings.Contains(email, "/") {
		return ""
	}
	_, domain, ok := strings.Cut(email, "@")
	if !ok {
		return ""
	}
	project := strings.TrimSuffix(domain, ".iam.gserviceaccount.com")
	if project == domain || project == "" {
		return ""
	}
	return "projects/" + project + "/serviceAccounts/" + email
}

func (api *API) IssueLocalToken(subject, audience string, scopes []string, lifetime time.Duration) (string, time.Time, error) {
	api.mu.RLock()
	disabled := false
	if strings.HasPrefix(subject, "serviceAccount:") {
		email := strings.TrimPrefix(subject, "serviceAccount:")
		for _, account := range api.serviceAccounts {
			if account.Email == email {
				disabled = account.Disabled
				break
			}
		}
	}
	api.mu.RUnlock()
	if disabled {
		return "", time.Time{}, errors.New("service account is disabled")
	}
	token, claims, err := api.issuer.Issue(localsecurity.TokenRequest{
		Subject: subject, Audience: audience, Scopes: scopes, Lifetime: lifetime,
	})
	return token, claims.ExpiresAt, err
}

func (api *API) VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error) {
	return api.issuer.Verify(token, localsecurity.VerifyOptions{Audience: audience, RequiredScope: scope})
}

// SetTokenAudience configures the audience shared by the gateway and local
// credential-issuing shims.
func (api *API) SetTokenAudience(audience string) {
	audience = strings.TrimSpace(audience)
	if audience == "" {
		audience = "minisky-gateway"
	}
	api.mu.Lock()
	api.tokenAudience = audience
	api.mu.Unlock()
}

// TokenAudience returns the configured local credential audience.
func (api *API) TokenAudience() string {
	api.mu.RLock()
	defer api.mu.RUnlock()
	return api.tokenAudience
}

// ResolveServiceAccount resolves either an email address or unique ID to the
// account's canonical email and reports its current enabled state.
func (api *API) ResolveServiceAccount(identifier string) (email string, disabled, found bool) {
	identifier = strings.TrimSpace(identifier)
	api.mu.RLock()
	defer api.mu.RUnlock()
	for _, account := range api.serviceAccounts {
		if account.Email == identifier || account.UniqueId == identifier {
			return account.Email, account.Disabled, true
		}
	}
	return "", false, false
}

func (api *API) persistOrError(w http.ResponseWriter) bool {
	if err := api.persistMetadata(); err != nil {
		log.Printf("[Shim: IAM] persist metadata: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist IAM metadata")
		return false
	}
	return true
}

// ServeHTTP dispatches based on path structure.
//
// Supported paths:
//
//	POST   /v1/projects/{project}/serviceAccounts
//	GET    /v1/projects/{project}/serviceAccounts
//	GET    /v1/projects/{project}/serviceAccounts/{email}
//	DELETE /v1/projects/{project}/serviceAccounts/{email}
//	POST   /v1/projects/{project}/serviceAccounts/{email}/keys
//	GET    /v1/projects/{project}/serviceAccounts/{email}/keys
//	POST   /v1/{resource}:setIamPolicy
//	GET    /v1/{resource}:getIamPolicy
//	POST   /v1/{resource}:testIamPermissions
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: IAM] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if err := api.PersistenceError(); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "IAM persistence is unavailable")
		return
	}

	path := r.URL.Path

	if strings.Contains(path, "/workloadIdentityPools") {
		api.routeWorkloadIdentity(w, r)
		return
	}

	// Policy verbs come as path suffixes after a colon
	switch {
	case strings.HasSuffix(path, ":disable") && strings.Contains(path, "/keys/"):
		api.disableKey(w, strings.TrimSuffix(path, ":disable"))
		return
	case strings.HasSuffix(path, ":setIamPolicy"):
		api.setIamPolicy(w, r, strings.TrimSuffix(path, ":setIamPolicy"))
		return
	case strings.HasSuffix(path, ":getIamPolicy"):
		api.getIamPolicy(w, r, strings.TrimSuffix(path, ":getIamPolicy"))
		return
	case strings.HasSuffix(path, ":testIamPermissions"):
		api.testIamPermissions(w, r, strings.TrimSuffix(path, ":testIamPermissions"))
		return
	}

	// Service Accounts
	if strings.Contains(path, "/serviceAccounts") {
		api.routeServiceAccounts(w, r, path)
		return
	}

	w.WriteHeader(http.StatusNotFound)
	writeError(w, 404, "NOT_FOUND", "IAM resource not found: "+path)
}

// ─────────────────────────────────────────────────────────────────────────────
// Service Accounts
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeServiceAccounts(w http.ResponseWriter, r *http.Request, path string) {
	project := extractSegmentAfter(path, "projects")
	email := extractSegmentAfter(path, "serviceAccounts")

	// Keys sub-collection
	if strings.Contains(path, "/keys") {
		api.routeKeys(w, r, project, email)
		return
	}

	switch r.Method {
	case http.MethodPost:
		api.createServiceAccount(w, r, project)
	case http.MethodGet:
		if email != "" {
			api.getServiceAccount(w, project, email)
		} else {
			api.listServiceAccounts(w, project)
		}
	case http.MethodDelete:
		api.deleteServiceAccount(w, project, email)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) createServiceAccount(w http.ResponseWriter, r *http.Request, project string) {
	var body struct {
		AccountId      string `json:"accountId"`
		ServiceAccount struct {
			DisplayName string `json:"displayName"`
			Description string `json:"description"`
		} `json:"serviceAccount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	if body.AccountId == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Field 'accountId' is required")
		return
	}

	email := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", body.AccountId, project)
	sa := &ServiceAccount{
		Name:           fmt.Sprintf("projects/%s/serviceAccounts/%s", project, email),
		ProjectId:      project,
		UniqueId:       uniqueNumericID(),
		Email:          email,
		DisplayName:    body.ServiceAccount.DisplayName,
		Description:    body.ServiceAccount.Description,
		Disabled:       false,
		Etag:           newEtag(),
		OAuth2ClientId: uniqueNumericID(),
	}

	key := project + ":" + email
	api.mu.Lock()
	api.serviceAccounts[key] = sa
	api.mu.Unlock()

	if !api.persistOrError(w) {
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sa)
}

func (api *API) getServiceAccount(w http.ResponseWriter, project, email string) {
	key := project + ":" + email
	api.mu.RLock()
	sa, ok := api.serviceAccounts[key]
	api.mu.RUnlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", fmt.Sprintf("ServiceAccount '%s' not found", email))
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(sa)
}

func (api *API) listServiceAccounts(w http.ResponseWriter, project string) {
	prefix := project + ":"
	api.mu.RLock()
	items := []*ServiceAccount{}
	for k, v := range api.serviceAccounts {
		if strings.HasPrefix(k, prefix) {
			items = append(items, v)
		}
	}
	api.mu.RUnlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"accounts":      items,
		"nextPageToken": "",
	})
}

func (api *API) deleteServiceAccount(w http.ResponseWriter, project, email string) {
	key := project + ":" + email
	api.mu.Lock()
	_, ok := api.serviceAccounts[key]
	if ok {
		delete(api.serviceAccounts, key)
		delete(api.keys, key)
	}
	api.mu.Unlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", fmt.Sprintf("ServiceAccount '%s' not found", email))
		return
	}
	if !api.persistOrError(w) {
		return
	}
	// GCP returns empty 200 on successful delete
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{})
}

// ─────────────────────────────────────────────────────────────────────────────
// Service Account Keys
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeKeys(w http.ResponseWriter, r *http.Request, project, email string) {
	keyID := extractSegmentAfter(r.URL.Path, "keys")
	switch r.Method {
	case http.MethodPost:
		api.createKey(w, project, email)
	case http.MethodGet:
		api.listKeys(w, project, email)
	case http.MethodDelete:
		api.deleteKey(w, project, email, keyID)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) disableKey(w http.ResponseWriter, path string) {
	project := extractSegmentAfter(path, "projects")
	email := extractSegmentAfter(path, "serviceAccounts")
	keyID := extractSegmentAfter(path, "keys")
	api.mu.Lock()
	key := api.findKeyLocked(project, email, keyID)
	if key != nil {
		key.Disabled = true
	}
	api.mu.Unlock()
	if key == nil {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Service account key not found")
		return
	}
	if !api.persistOrError(w) {
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

func (api *API) deleteKey(w http.ResponseWriter, project, email, keyID string) {
	accountKey := project + ":" + email
	api.mu.Lock()
	keys := api.keys[accountKey]
	found := false
	for index, key := range keys {
		if strings.HasSuffix(key.Name, "/keys/"+keyID) {
			api.keys[accountKey] = append(keys[:index], keys[index+1:]...)
			found = true
			break
		}
	}
	api.mu.Unlock()
	if !found {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Service account key not found")
		return
	}
	if !api.persistOrError(w) {
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

func (api *API) findKeyLocked(project, email, keyID string) *ServiceAccountKey {
	for _, key := range api.keys[project+":"+email] {
		if strings.HasSuffix(key.Name, "/keys/"+keyID) {
			return key
		}
	}
	return nil
}

// KeyUsable reports whether persisted local key metadata is enabled and within
// its validity window. MiniSky never treats these keys as Google credentials.
func (api *API) KeyUsable(name string, now time.Time) bool {
	api.mu.RLock()
	defer api.mu.RUnlock()
	for _, keys := range api.keys {
		for _, key := range keys {
			if key.Name != name || key.Disabled {
				continue
			}
			after, afterErr := time.Parse(time.RFC3339, key.ValidAfterTime)
			before, beforeErr := time.Parse(time.RFC3339, key.ValidBeforeTime)
			return afterErr == nil && beforeErr == nil && !now.Before(after) && now.Before(before)
		}
	}
	return false
}

func (api *API) createKey(w http.ResponseWriter, project, email string) {
	saKey := project + ":" + email
	api.mu.RLock()
	_, exists := api.serviceAccounts[saKey]
	api.mu.RUnlock()

	if !exists {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "ServiceAccount not found: "+email)
		return
	}

	keyID := fmt.Sprintf("%x", time.Now().UnixNano())
	keyName := fmt.Sprintf("projects/%s/serviceAccounts/%s/keys/%s", project, email, keyID)

	// Build a realistic (but non-functional) service account JSON key
	fakeKeyJSON := fmt.Sprintf(`{
  "type": "service_account",
  "project_id": "%s",
  "private_key_id": "%s",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA0Z3VS5JJcds3xHn/ygWep4PAtEsHAMmGQMBGHTETMFSb79Fg\n(minisky-fake-key-non-functional)\n-----END RSA PRIVATE KEY-----\n",
  "client_email": "%s",
  "client_id": "%s",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token"
}`, project, keyID, email, uniqueNumericID())

	key := &ServiceAccountKey{
		Name:            keyName,
		KeyType:         "USER_MANAGED",
		KeyOrigin:       "GOOGLE_PROVIDED",
		KeyAlgorithm:    "KEY_ALG_RSA_2048",
		PrivateKeyType:  "TYPE_GOOGLE_CREDENTIALS_FILE",
		PrivateKeyData:  b64Encode(fakeKeyJSON),
		ValidAfterTime:  time.Now().UTC().Format(time.RFC3339),
		ValidBeforeTime: time.Now().Add(87600 * time.Hour).UTC().Format(time.RFC3339), // 10 years
	}

	api.mu.Lock()
	api.keys[saKey] = append(api.keys[saKey], key)
	api.mu.Unlock()

	if !api.persistOrError(w) {
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(key)
}

func (api *API) listKeys(w http.ResponseWriter, project, email string) {
	saKey := project + ":" + email
	api.mu.RLock()
	keys := api.keys[saKey]
	api.mu.RUnlock()

	if keys == nil {
		keys = []*ServiceAccountKey{}
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"keys": keys,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// IAM Policy management
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) setIamPolicy(w http.ResponseWriter, r *http.Request, resource string) {
	var body struct {
		Policy IamPolicy `json:"policy"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}

	policy := body.Policy
	policy.Etag = newEtag()
	if policy.Version == 0 {
		policy.Version = 1
	}

	api.mu.Lock()
	api.policies[resource] = clonePolicy(&policy)
	api.mu.Unlock()

	if !api.persistOrError(w) {
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(clonePolicy(&policy))
}

func (api *API) getIamPolicy(w http.ResponseWriter, r *http.Request, resource string) {
	api.mu.RLock()
	policy, ok := api.policies[resource]
	policy = clonePolicy(policy)
	api.mu.RUnlock()

	if !ok {
		// Return an empty policy — same as GCP for resources with no policy set
		policy = &IamPolicy{
			Version:  1,
			Etag:     newEtag(),
			Bindings: []Binding{},
		}
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(policy)
}

// testIamPermissions returns the subset of requested permissions granted to the
// local caller. Permissive mode remains the default for backwards compatibility.
func (api *API) testIamPermissions(w http.ResponseWriter, r *http.Request, resource string) {
	var body struct {
		Permissions []string `json:"permissions"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	if body.Permissions == nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Field 'permissions' is required")
		return
	}

	if !api.strict {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"permissions": body.Permissions,
		})
		return
	}

	principal := strings.TrimSpace(r.Header.Get(principalHeader))
	if principal == "" {
		w.WriteHeader(http.StatusForbidden)
		writeError(w, http.StatusForbidden, "PERMISSION_DENIED", principalHeader+" is required in strict IAM mode")
		return
	}

	api.mu.RLock()
	policy := clonePolicy(api.policies[resource])
	api.mu.RUnlock()

	allowedSet := permissionsForPrincipal(policy, principal)
	allowed := make([]string, 0, len(body.Permissions))
	for _, permission := range body.Permissions {
		if _, ok := allowedSet[permission]; ok {
			allowed = append(allowed, permission)
		}
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"permissions": allowed,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

func extractSegmentAfter(path, keyword string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == keyword && i+1 < len(parts) {
			// Strip colon-suffixed verbs
			seg := parts[i+1]
			if idx := strings.Index(seg, ":"); idx != -1 {
				seg = seg[:idx]
			}
			return seg
		}
	}
	return ""
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"code":    code,
			"status":  status,
			"message": message,
		},
	})
}

var rolePermissions = map[string][]string{
	"roles/minisky.viewer": {
		"minisky.dashboard.view",
		"bigquery.datasets.get",
		"bigquery.datasets.list",
		"compute.instances.get",
		"compute.instances.list",
		"pubsub.subscriptions.get",
		"pubsub.subscriptions.list",
		"pubsub.topics.get",
		"pubsub.topics.list",
		"storage.objects.get",
		"storage.objects.list",
	},
	"roles/minisky.editor": {
		"minisky.dashboard.view",
		"minisky.dashboard.manage",
		"bigquery.datasets.get",
		"bigquery.datasets.list",
		"bigquery.datasets.update",
		"compute.instances.get",
		"compute.instances.list",
		"compute.instances.create",
		"compute.instances.start",
		"compute.instances.stop",
		"pubsub.subscriptions.get",
		"pubsub.subscriptions.list",
		"pubsub.subscriptions.create",
		"pubsub.topics.get",
		"pubsub.topics.list",
		"pubsub.topics.create",
		"pubsub.topics.publish",
		"storage.objects.get",
		"storage.objects.list",
		"storage.objects.create",
		"storage.objects.update",
	},
	"roles/minisky.admin": {
		"minisky.dashboard.view",
		"minisky.dashboard.manage",
		"minisky.dashboard.terminal",
		"resourcemanager.projects.create",
		"iam.serviceAccounts.create",
		"iam.serviceAccounts.delete",
		"iam.serviceAccountKeys.create",
		"iam.serviceAccountKeys.delete",
		"iam.serviceAccountKeys.disable",
		"iam.serviceAccounts.setIamPolicy",
		"bigquery.datasets.get",
		"bigquery.datasets.list",
		"bigquery.datasets.update",
		"compute.disks.create",
		"compute.disks.delete",
		"compute.disks.get",
		"compute.instances.create",
		"compute.instances.delete",
		"compute.instances.get",
		"compute.instances.list",
		"compute.instances.start",
		"compute.instances.stop",
		"pubsub.subscriptions.consume",
		"pubsub.subscriptions.create",
		"pubsub.subscriptions.delete",
		"pubsub.subscriptions.get",
		"pubsub.subscriptions.list",
		"pubsub.topics.create",
		"pubsub.topics.delete",
		"pubsub.topics.get",
		"pubsub.topics.list",
		"pubsub.topics.publish",
		"storage.buckets.create",
		"storage.buckets.delete",
		"storage.buckets.get",
		"storage.buckets.list",
		"storage.buckets.update",
		"storage.objects.create",
		"storage.objects.delete",
		"storage.objects.get",
		"storage.objects.list",
		"storage.objects.update",
	},
	"roles/iam.serviceAccountTokenCreator": {
		"iam.serviceAccounts.getAccessToken",
	},
	"roles/iam.serviceAccountViewer": {
		"iam.serviceAccounts.get",
	},
	"roles/iam.workloadIdentityUser": {
		"iam.serviceAccounts.getAccessToken",
	},
	"roles/storage.admin": {
		"storage.buckets.create",
		"storage.buckets.delete",
		"storage.buckets.get",
		"storage.buckets.list",
		"storage.buckets.update",
		"storage.objects.create",
		"storage.objects.delete",
		"storage.objects.get",
		"storage.objects.list",
		"storage.objects.update",
	},
	"roles/storage.objectAdmin": {
		"storage.objects.create",
		"storage.objects.delete",
		"storage.objects.get",
		"storage.objects.list",
		"storage.objects.update",
	},
	"roles/storage.objectViewer": {
		"storage.objects.get",
		"storage.objects.list",
	},
	"roles/compute.admin": {
		"compute.disks.create",
		"compute.disks.delete",
		"compute.disks.get",
		"compute.instances.create",
		"compute.instances.delete",
		"compute.instances.get",
		"compute.instances.list",
		"compute.instances.start",
		"compute.instances.stop",
	},
	"roles/compute.viewer": {
		"compute.disks.get",
		"compute.disks.list",
		"compute.instances.get",
		"compute.instances.list",
	},
	"roles/pubsub.admin": {
		"pubsub.subscriptions.consume",
		"pubsub.subscriptions.create",
		"pubsub.subscriptions.delete",
		"pubsub.subscriptions.get",
		"pubsub.subscriptions.list",
		"pubsub.topics.create",
		"pubsub.topics.delete",
		"pubsub.topics.get",
		"pubsub.topics.list",
		"pubsub.topics.publish",
		"pubsub.topics.attachSubscription",
	},
	"roles/pubsub.viewer": {
		"pubsub.subscriptions.get",
		"pubsub.subscriptions.list",
		"pubsub.topics.get",
		"pubsub.topics.list",
	},
	"roles/spanner.admin": {
		"spanner.backups.list",
	},
	"roles/spanner.viewer": {
		"spanner.backups.list",
	},
}

func permissionsForPrincipal(policy *IamPolicy, principal string) map[string]struct{} {
	permissions := make(map[string]struct{})
	if policy == nil {
		return permissions
	}
	for _, binding := range policy.Bindings {
		if !containsString(binding.Members, principal) {
			continue
		}
		if permission, ok := strings.CutPrefix(binding.Role, "permission:"); ok && permission != "" {
			permissions[permission] = struct{}{}
			continue
		}
		for _, permission := range rolePermissions[binding.Role] {
			permissions[permission] = struct{}{}
		}
	}
	return permissions
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func clonePolicy(policy *IamPolicy) *IamPolicy {
	if policy == nil {
		return nil
	}
	clone := *policy
	clone.Bindings = make([]Binding, len(policy.Bindings))
	for i, binding := range policy.Bindings {
		clone.Bindings[i] = binding
		clone.Bindings[i].Members = append([]string(nil), binding.Members...)
	}
	return &clone
}

func cloneIAMMetadata(
	accounts map[string]*ServiceAccount,
	keys map[string][]*ServiceAccountKey,
	policies map[string]*IamPolicy,
) iamMetadata {
	result := iamMetadata{
		ServiceAccounts: make(map[string]*ServiceAccount, len(accounts)),
		Keys:            make(map[string][]*ServiceAccountKey, len(keys)),
		Policies:        make(map[string]*IamPolicy, len(policies)),
	}
	for name, account := range accounts {
		clone := *account
		result.ServiceAccounts[name] = &clone
	}
	for name, accountKeys := range keys {
		clones := make([]*ServiceAccountKey, len(accountKeys))
		for i, key := range accountKeys {
			clone := *key
			clones[i] = &clone
		}
		result.Keys[name] = clones
	}
	for resource, policy := range policies {
		result.Policies[resource] = clonePolicy(policy)
	}
	return result
}

func uniqueNumericID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func newEtag() string {
	return fmt.Sprintf("ACAB%x", time.Now().UnixNano())
}

// b64Encode returns the standard base64 encoding of s.
func b64Encode(s string) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	// Use encoding/base64 via import — but we can't add imports here without showing them at top.
	// Encode inline:
	src := []byte(s)
	out := make([]byte, 0, ((len(src)+2)/3)*4)
	for i := 0; i < len(src); i += 3 {
		var b0, b1, b2 byte
		b0 = src[i]
		if i+1 < len(src) {
			b1 = src[i+1]
		}
		if i+2 < len(src) {
			b2 = src[i+2]
		}
		out = append(out,
			chars[(b0>>2)&0x3F],
			chars[((b0&0x3)<<4)|((b1>>4)&0xF)],
			chars[((b1&0xF)<<2)|((b2>>6)&0x3)],
			chars[b2&0x3F],
		)
	}
	// Padding
	switch len(src) % 3 {
	case 1:
		out[len(out)-2] = '='
		out[len(out)-1] = '='
	case 2:
		out[len(out)-1] = '='
	}
	return string(out)
}
