package apigateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"minisky/pkg/state"
)

const stateEntry = "apigateway/metadata"

func init() {
	state.MustRegisterEntryValidator(stateEntry, state.StrictEntryValidator(validateAPIGatewayMetadata))
}

type apigatewayStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type apigatewayMetadata struct {
	Apis     map[string]*Api       `json:"apis"`
	Configs  map[string]*ApiConfig `json:"configs"`
	Gateways map[string]*Gateway   `json:"gateways"`
}

func validateAPIGatewayMetadata(_ state.EntryValidationContext, metadata *apigatewayMetadata) error {
	if err := state.ValidateResourceMaps(metadata); err != nil {
		return err
	}
	for name := range metadata.Configs {
		index := strings.LastIndex(name, "/configs/")
		if index < 0 {
			return fmt.Errorf("API config %q has invalid parent hierarchy", name)
		}
		if _, ok := metadata.Apis[name[:index]]; !ok {
			return fmt.Errorf("API config %q references missing API", name)
		}
	}
	for name, gateway := range metadata.Gateways {
		if gateway != nil && gateway.ApiConfig != "" {
			if _, ok := metadata.Configs[gateway.ApiConfig]; !ok {
				return fmt.Errorf("gateway %q references missing API config %q", name, gateway.ApiConfig)
			}
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
func (api *API) snapshot() apigatewayMetadata {
	api.mu.RLock()
	defer api.mu.RUnlock()
	apis := make(map[string]*Api, len(api.apis))
	for k, v := range api.apis {
		apis[k] = deepCopyApi(v)
	}
	configs := make(map[string]*ApiConfig, len(api.configs))
	for k, v := range api.configs {
		configs[k] = deepCopyConfig(v)
	}
	gateways := make(map[string]*Gateway, len(api.gateways))
	for k, v := range api.gateways {
		gateways[k] = deepCopyGateway(v)
	}
	return apigatewayMetadata{Apis: apis, Configs: configs, Gateways: gateways}
}

// loadState rehydrates resources from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta apigatewayMetadata
	if err := api.stateStore.Load(stateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Apis != nil {
		api.apis = meta.Apis
	}
	if meta.Configs != nil {
		api.configs = meta.Configs
	}
	if meta.Gateways != nil {
		api.gateways = meta.Gateways
	}
	return nil
}

func deepCopyApi(a *Api) *Api {
	raw, _ := json.Marshal(a)
	var clone Api
	_ = json.Unmarshal(raw, &clone)
	return &clone
}

func deepCopyGateway(g *Gateway) *Gateway {
	raw, _ := json.Marshal(g)
	var clone Gateway
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
