package servicedirectory

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"minisky/pkg/state"
)

const stateEntry = "servicedirectory/metadata"

func init() {
	state.MustRegisterEntryValidator(stateEntry, state.StrictEntryValidator(validateServiceDirectoryMetadata))
}

type sdStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type sdMetadata struct {
	Namespaces map[string]*Namespace `json:"namespaces"`
	Services   map[string]*Service   `json:"services"`
	Endpoints  map[string]*Endpoint  `json:"endpoints"`
}

func validateServiceDirectoryMetadata(_ state.EntryValidationContext, metadata *sdMetadata) error {
	if err := state.ValidateResourceMaps(metadata); err != nil {
		return err
	}
	for name := range metadata.Services {
		index := strings.LastIndex(name, "/services/")
		if index < 0 {
			return fmt.Errorf("service %q has invalid parent hierarchy", name)
		}
		if _, ok := metadata.Namespaces[name[:index]]; !ok {
			return fmt.Errorf("service %q references missing namespace", name)
		}
	}
	for name := range metadata.Endpoints {
		index := strings.LastIndex(name, "/endpoints/")
		if index < 0 {
			return fmt.Errorf("endpoint %q has invalid parent hierarchy", name)
		}
		if _, ok := metadata.Services[name[:index]]; !ok {
			return fmt.Errorf("endpoint %q references missing service", name)
		}
	}
	return nil
}

// persistState deep-copies resources and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	snapshot := api.snapshot()
	return api.stateStore.Save(stateEntry, snapshot)
}

// snapshot returns a deep copy of all resources for safe serialization.
func (api *API) snapshot() sdMetadata {
	api.mu.RLock()
	defer api.mu.RUnlock()
	ns := make(map[string]*Namespace, len(api.namespaces))
	for k, v := range api.namespaces {
		ns[k] = deepCopyNamespace(v)
	}
	svcs := make(map[string]*Service, len(api.services))
	for k, v := range api.services {
		svcs[k] = deepCopyService(v)
	}
	eps := make(map[string]*Endpoint, len(api.endpoints))
	for k, v := range api.endpoints {
		eps[k] = deepCopyEndpoint(v)
	}
	return sdMetadata{Namespaces: ns, Services: svcs, Endpoints: eps}
}

// loadState rehydrates resources from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta sdMetadata
	if err := api.stateStore.Load(stateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Namespaces != nil {
		api.namespaces = meta.Namespaces
	}
	if meta.Services != nil {
		api.services = meta.Services
	}
	if meta.Endpoints != nil {
		api.endpoints = meta.Endpoints
	}
	return nil
}

func deepCopyNamespace(n *Namespace) *Namespace {
	raw, _ := json.Marshal(n)
	var clone Namespace
	_ = json.Unmarshal(raw, &clone)
	return &clone
}

func deepCopyService(s *Service) *Service {
	raw, _ := json.Marshal(s)
	var clone Service
	_ = json.Unmarshal(raw, &clone)
	return &clone
}

func deepCopyEndpoint(e *Endpoint) *Endpoint {
	raw, _ := json.Marshal(e)
	var clone Endpoint
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
