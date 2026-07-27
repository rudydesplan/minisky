package servicemesh

import (
	"encoding/json"
	"errors"

	"minisky/pkg/state"
)

const servicemeshStateEntry = "servicemesh/metadata"

func init() {
	state.MustRegisterEntryValidator(servicemeshStateEntry, state.StrictEntryValidator[servicemeshMetadata](nil))
}

type servicemeshStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type servicemeshMetadata struct {
	Meshes     map[string]*Mesh      `json:"meshes"`
	HttpRoutes map[string]*HttpRoute `json:"httpRoutes"`
}

func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	meshSnap := make(map[string]*Mesh, len(api.meshes))
	for k, v := range api.meshes {
		meshSnap[k] = cloneMesh(v)
	}
	routeSnap := make(map[string]*HttpRoute, len(api.httpRoutes))
	for k, v := range api.httpRoutes {
		routeSnap[k] = cloneHttpRoute(v)
	}
	api.mu.RUnlock()

	return api.stateStore.Save(servicemeshStateEntry, servicemeshMetadata{
		Meshes:     meshSnap,
		HttpRoutes: routeSnap,
	})
}

func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta servicemeshMetadata
	if err := api.stateStore.Load(servicemeshStateEntry, &meta); err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	if meta.Meshes != nil {
		api.meshes = meta.Meshes
	}
	if meta.HttpRoutes != nil {
		api.httpRoutes = meta.HttpRoutes
	}
	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, state.ErrNotFound)
}

func cloneMesh(m *Mesh) *Mesh {
	raw, _ := json.Marshal(m)
	var c Mesh
	_ = json.Unmarshal(raw, &c)
	return &c
}

func cloneHttpRoute(r *HttpRoute) *HttpRoute {
	raw, _ := json.Marshal(r)
	var c HttpRoute
	_ = json.Unmarshal(raw, &c)
	return &c
}
