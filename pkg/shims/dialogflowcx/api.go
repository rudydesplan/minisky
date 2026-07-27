// Package dialogflowcx implements bounded Dialogflow CX v3 control-plane behavior.
package dialogflowcx

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

const stateEntry = "dialogflowcx/metadata"

func init() {
	state.MustRegisterEntryValidator(stateEntry, state.StrictEntryValidator(validateMetadata))
	registry.Register("dialogflow.googleapis.com", func(_ *registry.Context) http.Handler { return NewAPI() })
}

type Agent struct {
	Name                   string   `json:"name"`
	DisplayName            string   `json:"displayName"`
	DefaultLanguageCode    string   `json:"defaultLanguageCode"`
	TimeZone               string   `json:"timeZone"`
	Description            string   `json:"description,omitempty"`
	SupportedLanguageCodes []string `json:"supportedLanguageCodes,omitempty"`
	CreateTime             string   `json:"createTime,omitempty"`
}

type metadata struct {
	Agents map[string]*Agent `json:"agents"`
	Seq    uint64            `json:"seq"`
}

type entryStore interface {
	Load(string, any) error
	Save(string, any) error
}

type API struct {
	mu       sync.RWMutex
	mutateMu sync.Mutex
	store    entryStore
	agents   map[string]*Agent
	seq      uint64
}

func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	return NewAPIWithStore(state.NewGuardedEntryStore(store, err))
}

func NewAPIWithStore(store entryStore) *API {
	if _, guarded := store.(*state.GuardedEntryStore); store != nil && !guarded {
		store = state.NewGuardedEntryStore(store, nil)
	}
	api := &API{store: store, agents: map[string]*Agent{}}
	var saved metadata
	if store != nil {
		if err := store.Load(stateEntry, &saved); err == nil {
			api.agents, api.seq = saved.Agents, saved.Seq
			if api.agents == nil {
				api.agents = map[string]*Agent{}
			}
		}
	}
	return api
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/v3/")
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(path, ":detectIntent"):
		api.detectIntent(w, r)
	case strings.Contains(path, "/flows") || strings.Contains(path, "/intents") ||
		strings.Contains(path, "/entityTypes") || strings.Contains(path, "/webhooks") ||
		strings.Contains(path, "/testCases") || strings.Contains(path, "/environments"):
		writeError(w, 501, "UNIMPLEMENTED", "Dialogflow CX design-time child resources are not implemented")
	case strings.HasSuffix(path, "/agents"):
		if r.Method == http.MethodPost {
			api.createAgent(w, r, path)
		} else if r.Method == http.MethodGet {
			api.listAgents(w, r, path)
		} else {
			writeError(w, 405, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	case strings.Contains(path, "/agents/"):
		switch r.Method {
		case http.MethodGet:
			api.getAgent(w, path)
		case http.MethodDelete:
			api.deleteAgent(w, path)
		case http.MethodPatch:
			writeError(w, 501, "UNIMPLEMENTED", "agent update is not implemented")
		default:
			writeError(w, 405, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	default:
		writeError(w, 404, "NOT_FOUND", "Dialogflow CX resource not found")
	}
}

func (api *API) createAgent(w http.ResponseWriter, r *http.Request, path string) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var agent Agent
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&agent) != nil {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid request body")
		return
	}
	if agent.Name != "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'name' is output only")
		return
	}
	if agent.DisplayName == "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'displayName' is required")
		return
	}
	if agent.DefaultLanguageCode == "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'defaultLanguageCode' is required")
		return
	}
	if agent.TimeZone == "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'timeZone' is required")
		return
	}
	parent := strings.TrimSuffix(path, "/agents")
	if !validParent(parent) {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid parent path")
		return
	}
	api.mutateMu.Lock()
	defer api.mutateMu.Unlock()
	api.mu.Lock()
	api.seq++
	agent.Name = fmt.Sprintf("%s/agents/agent-%d", parent, api.seq)
	agent.CreateTime = time.Now().UTC().Format(time.RFC3339Nano)
	api.agents[agent.Name] = clone(&agent)
	api.mu.Unlock()
	if err := api.persist(); err != nil {
		api.mu.Lock()
		delete(api.agents, agent.Name)
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "state persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(&agent)
}

func (api *API) getAgent(w http.ResponseWriter, name string) {
	api.mu.RLock()
	agent := clone(api.agents[name])
	api.mu.RUnlock()
	if agent == nil {
		writeError(w, 404, "NOT_FOUND", "agent not found")
		return
	}
	_ = json.NewEncoder(w).Encode(agent)
}

func (api *API) listAgents(w http.ResponseWriter, r *http.Request, path string) {
	parent := strings.TrimSuffix(path, "/agents")
	size := 100
	if raw := r.URL.Query().Get("pageSize"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100 {
			writeError(w, 400, "INVALID_ARGUMENT", "field 'pageSize' must be between 1 and 100")
			return
		}
		size = value
	}
	api.mu.RLock()
	items := make([]*Agent, 0)
	for name, agent := range api.agents {
		if strings.HasPrefix(name, parent+"/agents/") {
			items = append(items, clone(agent))
		}
	}
	api.mu.RUnlock()
	page, token, err := pagination.Page(items, size, r.URL.Query().Get("pageToken"),
		pagination.Scope{Service: "dialogflow.googleapis.com", Parent: parent},
		func(agent *Agent) string { return agent.Name })
	if err != nil {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"agents": page, "nextPageToken": token})
}

func (api *API) deleteAgent(w http.ResponseWriter, name string) {
	api.mutateMu.Lock()
	defer api.mutateMu.Unlock()
	api.mu.Lock()
	previous := api.agents[name]
	if previous != nil {
		delete(api.agents, name)
	}
	api.mu.Unlock()
	if previous == nil {
		writeError(w, 404, "NOT_FOUND", "agent not found")
		return
	}
	if err := api.persist(); err != nil {
		api.mu.Lock()
		api.agents[name] = previous
		api.mu.Unlock()
		writeError(w, 503, "UNAVAILABLE", "state persistence failed")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{})
}

func (api *API) detectIntent(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v3/")
	sessionIndex := strings.Index(path, "/sessions/")
	if sessionIndex < 0 || !strings.Contains(path[:sessionIndex], "/agents/") {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid session path")
		return
	}
	agentName := path[:sessionIndex]
	api.mu.RLock()
	_, exists := api.agents[agentName]
	api.mu.RUnlock()
	if !exists {
		writeError(w, 404, "NOT_FOUND", "agent not found")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var body struct {
		QueryInput *struct {
			Text *struct {
				Text string `json:"text"`
			} `json:"text,omitempty"`
			LanguageCode string          `json:"languageCode"`
			Audio        json.RawMessage `json:"audio,omitempty"`
			Event        json.RawMessage `json:"event,omitempty"`
			Intent       json.RawMessage `json:"intent,omitempty"`
			Dtmf         json.RawMessage `json:"dtmf,omitempty"`
		} `json:"queryInput"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid request body")
		return
	}
	if body.QueryInput == nil || body.QueryInput.LanguageCode == "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'queryInput.languageCode' is required")
		return
	}
	if body.QueryInput.Text == nil || body.QueryInput.Text.Text == "" {
		if len(body.QueryInput.Audio) != 0 || len(body.QueryInput.Event) != 0 ||
			len(body.QueryInput.Intent) != 0 || len(body.QueryInput.Dtmf) != 0 {
			writeError(w, 501, "UNIMPLEMENTED", "non-text query input is not implemented")
		} else {
			writeError(w, 400, "INVALID_ARGUMENT", "field 'queryInput' must contain an input modality")
		}
		return
	}
	if len(body.QueryInput.Text.Text) > 4096 {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'queryInput.text.text' exceeds the 4096-byte local limit")
		return
	}
	writeError(w, 501, "UNIMPLEMENTED", "intent detection is not implemented; no intent or confidence was generated")
}

func (api *API) persist() error {
	if api.store == nil {
		return nil
	}
	api.mu.RLock()
	snapshot := metadata{Agents: map[string]*Agent{}, Seq: api.seq}
	for name, agent := range api.agents {
		snapshot.Agents[name] = clone(agent)
	}
	api.mu.RUnlock()
	return api.store.Save(stateEntry, snapshot)
}

func validateMetadata(_ state.EntryValidationContext, saved *metadata) error {
	return state.ValidateResourceMaps(*saved)
}

func validParent(parent string) bool {
	parts := strings.Split(parent, "/")
	return len(parts) == 4 && parts[0] == "projects" && parts[1] != "" && parts[2] == "locations" && parts[3] != ""
}

func clone(agent *Agent) *Agent {
	if agent == nil {
		return nil
	}
	raw, _ := json.Marshal(agent)
	var result Agent
	_ = json.Unmarshal(raw, &result)
	return &result
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "message": message, "status": status, "details": []any{},
	}})
}
