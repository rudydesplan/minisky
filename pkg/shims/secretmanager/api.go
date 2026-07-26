package secretmanager

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/shims/logging"
	"minisky/pkg/state"
)

func init() {
	registry.Register("secretmanager.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI(ctx.SvcMgr, nil)
	})
}

// ---------------------------------------------------------------------------
// In-memory data model
// ---------------------------------------------------------------------------

type secretVersion struct {
	Name        string `json:"name"`
	CreateTime  string `json:"createTime"`
	DestroyTime string `json:"destroyTime,omitempty"`
	State       string `json:"state"`          // ENABLED | DISABLED | DESTROYED
	Payload     string `json:"data,omitempty"` // base64-encoded
}

type secret struct {
	Name        string            `json:"name"`
	CreateTime  string            `json:"createTime"`
	Labels      map[string]string `json:"labels,omitempty"`
	Replication map[string]any    `json:"replication"`
	versions    []*secretVersion
	mu          sync.Mutex
}

// ---------------------------------------------------------------------------
// API
// ---------------------------------------------------------------------------

type API struct {
	mu        sync.RWMutex
	persistMu sync.Mutex
	// map[project] -> map[secretId] -> *secret
	store          map[string]map[string]*secret
	svcMgr         *orchestrator.ServiceManager
	logAPI         *logging.API
	stateStore     secretStateStore
	persistenceErr error
}

func NewAPI(sm *orchestrator.ServiceManager, logAPI *logging.API) *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Secret Manager] state disabled: %v", err)
		return newAPI(sm, logAPI, nil)
	}
	api, err := NewAPIWithStore(sm, logAPI, store)
	if err != nil {
		log.Printf("[Shim: Secret Manager] state rehydration failed: %v", err)
		return newAPI(sm, logAPI, nil)
	}
	return api
}

func newAPI(sm *orchestrator.ServiceManager, logAPI *logging.API, store secretStateStore) *API {
	return &API{
		store:      make(map[string]map[string]*secret),
		svcMgr:     sm,
		logAPI:     logAPI,
		stateStore: store,
	}
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	if logShim, ok := ctx.GetShim("logging.googleapis.com").(*logging.API); ok {
		api.logAPI = logShim
	}
}

func (api *API) pushLog(projectId, severity, secretId, text string) {
	if api.logAPI == nil {
		return
	}
	api.logAPI.PushLog(projectId, severity, "secret_manager_secret", secretId, text)
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func jsonOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": msg, "status": http.StatusText(code)},
	})
}

func jsonErrorStatus(w http.ResponseWriter, code int, status, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": msg, "status": status},
	})
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Secret Manager] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	api.mu.RLock()
	persistenceErr := api.persistenceErr
	api.mu.RUnlock()
	if persistenceErr != nil {
		jsonErrorStatus(w, http.StatusServiceUnavailable, "UNAVAILABLE",
			"Secret Manager persistence is degraded: "+persistenceErr.Error())
		return
	}

	// Strip /v1 prefix if present
	path := strings.TrimPrefix(r.URL.Path, "/v1")

	// /projects/{project}/secrets
	// /projects/{project}/secrets/{secretId}
	// /projects/{project}/secrets/{secretId}:addVersion
	// /projects/{project}/secrets/{secretId}/versions
	// /projects/{project}/secrets/{secretId}/versions/{version}
	// /projects/{project}/secrets/{secretId}/versions/{version}:access

	parts := strings.Split(strings.Trim(path, "/"), "/")

	// Minimum: ["projects", project, "secrets"]
	if len(parts) < 3 || parts[0] != "projects" || parts[2] != "secrets" {
		jsonError(w, http.StatusNotFound, "not found")
		return
	}

	project := parts[1]

	switch {
	// List/Create secrets: /projects/{p}/secrets
	case len(parts) == 3:
		switch r.Method {
		case http.MethodGet:
			api.listSecrets(w, r, project)
		case http.MethodPost:
			api.createSecret(w, r, project)
		default:
			jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
		}

	// /projects/{p}/secrets/{id} or /projects/{p}/secrets/{id}:addVersion
	case len(parts) == 4:
		raw := parts[3]
		if colon := strings.Index(raw, ":"); colon >= 0 {
			secretId := raw[:colon]
			action := raw[colon+1:]
			if action == "addVersion" && r.Method == http.MethodPost {
				api.addVersion(w, r, project, secretId)
			} else {
				jsonError(w, http.StatusNotFound, fmt.Sprintf("unknown action: %s", action))
			}
		} else {
			switch r.Method {
			case http.MethodGet:
				api.getSecret(w, r, project, raw)
			case http.MethodDelete:
				api.deleteSecret(w, r, project, raw)
			default:
				jsonError(w, http.StatusMethodNotAllowed, "method not allowed")
			}
		}

	// /projects/{p}/secrets/{id}/versions
	case len(parts) == 5 && parts[4] == "versions":
		api.listVersions(w, r, project, parts[3])

	// /projects/{p}/secrets/{id}/versions/{v} or /versions/{v}:access
	case len(parts) == 6 && parts[4] == "versions":
		raw := parts[5]
		if colon := strings.Index(raw, ":"); colon >= 0 {
			versionRef := raw[:colon]
			action := raw[colon+1:]
			if action == "access" && r.Method == http.MethodGet {
				api.accessVersion(w, r, project, parts[3], versionRef)
			} else {
				jsonError(w, http.StatusNotFound, fmt.Sprintf("unknown version action: %s", action))
			}
		} else {
			api.getVersion(w, r, project, parts[3], raw)
		}

	default:
		jsonError(w, http.StatusNotFound, "route not found")
	}
}

// ---------------------------------------------------------------------------
// Secret CRUD
// ---------------------------------------------------------------------------

func (api *API) projectStore(project string) map[string]*secret {
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.store[project] == nil {
		api.store[project] = make(map[string]*secret)
	}
	return api.store[project]
}

func (api *API) createSecret(w http.ResponseWriter, r *http.Request, project string) {
	secretId := r.URL.Query().Get("secretId")
	if secretId == "" {
		jsonError(w, http.StatusBadRequest, "secretId query parameter is required")
		return
	}

	var body struct {
		Labels      map[string]string `json:"labels"`
		Replication map[string]any    `json:"replication"`
	}
	json.NewDecoder(r.Body).Decode(&body)

	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	previous := api.snapshotMetadata()
	snapshot := cloneSecretMetadata(previous)
	if _, exists := snapshot.Projects[project][secretId]; exists {
		jsonError(w, http.StatusConflict, fmt.Sprintf("Secret %s already exists", secretId))
		return
	}
	name := fmt.Sprintf("projects/%s/secrets/%s", project, secretId)
	s := &secret{
		Name:        name,
		CreateTime:  now(),
		Labels:      body.Labels,
		Replication: body.Replication,
	}
	if snapshot.Projects[project] == nil {
		snapshot.Projects[project] = make(map[string]persistedSecret)
	}
	snapshot.Projects[project][secretId] = persistedSecret{
		Name: s.Name, CreateTime: s.CreateTime, Labels: cloneStringMap(s.Labels),
		Replication: cloneAnyMap(s.Replication), Versions: []*secretVersion{},
	}
	if err := api.saveMetadataTransaction(previous, snapshot); err != nil {
		jsonError(w, http.StatusInternalServerError, "persist secret: "+err.Error())
		return
	}
	api.mu.Lock()
	if api.store[project] == nil {
		api.store[project] = make(map[string]*secret)
	}
	api.store[project][secretId] = s
	api.mu.Unlock()

	api.pushLog(project, "INFO", secretId, "Secret created: "+name)
	w.WriteHeader(http.StatusOK)
	jsonOK(w, map[string]any{
		"name":        s.Name,
		"createTime":  s.CreateTime,
		"replication": s.Replication,
		"labels":      s.Labels,
	})
}

func (api *API) getSecret(w http.ResponseWriter, r *http.Request, project, secretId string) {
	ps := api.projectStore(project)
	api.mu.RLock()
	s, ok := ps[secretId]
	api.mu.RUnlock()
	if !ok {
		jsonError(w, http.StatusNotFound, fmt.Sprintf("Secret %s not found", secretId))
		return
	}
	jsonOK(w, map[string]any{
		"name":        s.Name,
		"createTime":  s.CreateTime,
		"replication": s.Replication,
		"labels":      s.Labels,
	})
}

func (api *API) listSecrets(w http.ResponseWriter, r *http.Request, project string) {
	ps := api.projectStore(project)
	api.mu.RLock()
	defer api.mu.RUnlock()
	list := make([]map[string]any, 0, len(ps))
	for _, s := range ps {
		list = append(list, map[string]any{
			"name":        s.Name,
			"createTime":  s.CreateTime,
			"replication": s.Replication,
			"labels":      s.Labels,
		})
	}
	jsonOK(w, map[string]any{"secrets": list, "totalSize": len(list)})
}

func (api *API) deleteSecret(w http.ResponseWriter, r *http.Request, project, secretId string) {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	previous := api.snapshotMetadata()
	snapshot := cloneSecretMetadata(previous)
	_, ok := snapshot.Projects[project][secretId]
	if !ok {
		jsonError(w, http.StatusNotFound, fmt.Sprintf("Secret %s not found", secretId))
		return
	}
	delete(snapshot.Projects[project], secretId)
	if err := api.saveMetadataTransaction(previous, snapshot); err != nil {
		jsonError(w, http.StatusInternalServerError, "persist secret deletion: "+err.Error())
		return
	}
	api.mu.Lock()
	delete(api.store[project], secretId)
	api.mu.Unlock()
	api.pushLog(project, "INFO", secretId, "Secret deleted")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{})
}

// ---------------------------------------------------------------------------
// Versions
// ---------------------------------------------------------------------------

func (api *API) addVersion(w http.ResponseWriter, r *http.Request, project, secretId string) {
	api.persistMu.Lock()
	defer api.persistMu.Unlock()
	api.mu.RLock()
	s, ok := api.store[project][secretId]
	api.mu.RUnlock()
	if !ok {
		jsonError(w, http.StatusNotFound, fmt.Sprintf("Secret %s not found", secretId))
		return
	}

	var body struct {
		Payload struct {
			Data string `json:"data"` // base64
		} `json:"payload"`
	}
	json.NewDecoder(r.Body).Decode(&body)

	previous := api.snapshotMetadata()
	snapshot := cloneSecretMetadata(previous)
	saved := snapshot.Projects[project][secretId]
	versionNum := len(saved.Versions) + 1
	vName := fmt.Sprintf("projects/%s/secrets/%s/versions/%d", project, secretId, versionNum)
	v := &secretVersion{
		Name:       vName,
		CreateTime: now(),
		State:      "ENABLED",
		Payload:    body.Payload.Data,
	}
	versionCopy := *v
	saved.Versions = append(saved.Versions, &versionCopy)
	snapshot.Projects[project][secretId] = saved
	if err := api.saveMetadataTransaction(previous, snapshot); err != nil {
		jsonError(w, http.StatusInternalServerError, "persist secret version: "+err.Error())
		return
	}
	s.mu.Lock()
	s.versions = append(s.versions, v)
	s.mu.Unlock()

	api.pushLog(project, "INFO", secretId, fmt.Sprintf("Version %d added", versionNum))
	jsonOK(w, map[string]any{
		"name":       v.Name,
		"createTime": v.CreateTime,
		"state":      v.State,
	})
}

func (api *API) markPersistenceDegraded(err error) {
	api.mu.Lock()
	api.persistenceErr = err
	api.mu.Unlock()
}

func (api *API) listVersions(w http.ResponseWriter, r *http.Request, project, secretId string) {
	ps := api.projectStore(project)
	api.mu.RLock()
	s, ok := ps[secretId]
	api.mu.RUnlock()
	if !ok {
		jsonError(w, http.StatusNotFound, fmt.Sprintf("Secret %s not found", secretId))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := make([]map[string]any, 0, len(s.versions))
	for _, v := range s.versions {
		list = append(list, map[string]any{
			"name":       v.Name,
			"createTime": v.CreateTime,
			"state":      v.State,
		})
	}
	jsonOK(w, map[string]any{"versions": list, "totalSize": len(list)})
}

func (api *API) resolveVersion(s *secret, ref string) *secretVersion {
	if ref == "latest" {
		if len(s.versions) == 0 {
			return nil
		}
		return s.versions[len(s.versions)-1]
	}
	for _, v := range s.versions {
		if strings.HasSuffix(v.Name, "/"+ref) {
			return v
		}
	}
	return nil
}

func (api *API) getVersion(w http.ResponseWriter, r *http.Request, project, secretId, versionRef string) {
	ps := api.projectStore(project)
	api.mu.RLock()
	s, ok := ps[secretId]
	api.mu.RUnlock()
	if !ok {
		jsonError(w, http.StatusNotFound, fmt.Sprintf("Secret %s not found", secretId))
		return
	}
	s.mu.Lock()
	v := api.resolveVersion(s, versionRef)
	s.mu.Unlock()
	if v == nil {
		jsonError(w, http.StatusNotFound, "version not found")
		return
	}
	jsonOK(w, map[string]any{"name": v.Name, "createTime": v.CreateTime, "state": v.State})
}

func (api *API) accessVersion(w http.ResponseWriter, r *http.Request, project, secretId, versionRef string) {
	ps := api.projectStore(project)
	api.mu.RLock()
	s, ok := ps[secretId]
	api.mu.RUnlock()
	if !ok {
		jsonError(w, http.StatusNotFound, fmt.Sprintf("Secret %s not found", secretId))
		return
	}
	s.mu.Lock()
	v := api.resolveVersion(s, versionRef)
	s.mu.Unlock()
	if v == nil {
		jsonError(w, http.StatusNotFound, "version not found")
		return
	}
	if v.State != "ENABLED" {
		jsonError(w, http.StatusFailedDependency, fmt.Sprintf("version state is %s", v.State))
		return
	}
	api.pushLog(project, "INFO", secretId, "Secret version accessed: "+v.Name)
	jsonOK(w, map[string]any{
		"name":    v.Name,
		"payload": map[string]any{"data": v.Payload},
	})
}
