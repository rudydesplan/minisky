package identityplatform

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"minisky/pkg/state"
)

const identityPlatformStateEntry = "identityplatform/metadata"

func init() {
	state.MustRegisterEntryValidator(identityPlatformStateEntry, state.StrictEntryValidator(validateIdentityPlatformMetadata))
}

type identityPlatformStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type identityPlatformMetadata struct {
	Tenants         map[string]*Tenant         `json:"tenants"`
	OAuthIdpConfigs map[string]*OAuthIdpConfig `json:"oauthIdpConfigs"`
	ProjectConfigs  map[string]*ProjectConfig  `json:"projectConfigs,omitempty"`
	TenantConfigs   map[string]*TenantConfig   `json:"tenantConfigs,omitempty"`
	TenantSeq       int                        `json:"tenantSeq"`
}

func validateIdentityPlatformMetadata(_ state.EntryValidationContext, metadata *identityPlatformMetadata) error {
	if metadata.TenantSeq < 0 {
		return fmt.Errorf("tenantSeq must not be negative")
	}
	for name := range metadata.Tenants {
		id := name[strings.LastIndex(name, "/")+1:]
		value, err := strconv.Atoi(strings.TrimPrefix(id, "tenant-"))
		if strings.HasPrefix(id, "tenant-") && err == nil && value > metadata.TenantSeq {
			return fmt.Errorf("tenantSeq %d collides with tenant %q", metadata.TenantSeq, name)
		}
	}
	for name := range metadata.OAuthIdpConfigs {
		index := strings.LastIndex(name, "/oauthIdpConfigs/")
		if index < 0 {
			return fmt.Errorf("OAuth config %q has invalid parent hierarchy", name)
		}
		if _, ok := metadata.Tenants[name[:index]]; !ok {
			return fmt.Errorf("OAuth config %q references missing tenant", name)
		}
	}
	return nil
}

func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	tenantSnap := make(map[string]*Tenant, len(api.tenants))
	for k, v := range api.tenants {
		tenantSnap[k] = cloneTenant(v)
	}
	configSnap := make(map[string]*OAuthIdpConfig, len(api.oauthConfigs))
	for k, v := range api.oauthConfigs {
		configSnap[k] = cloneOAuthConfig(v)
	}
	projectConfigSnap := make(map[string]*ProjectConfig, len(api.projectConfigs))
	for k, v := range api.projectConfigs {
		projectConfigSnap[k] = cloneProjectConfig(v)
	}
	tenantConfigSnap := make(map[string]*TenantConfig, len(api.tenantConfigs))
	for k, v := range api.tenantConfigs {
		tenantConfigSnap[k] = cloneTenantConfig(v)
	}
	tenantSeq := api.tenantSeq
	api.mu.RUnlock()

	return api.stateStore.Save(identityPlatformStateEntry, identityPlatformMetadata{
		Tenants:         tenantSnap,
		OAuthIdpConfigs: configSnap,
		ProjectConfigs:  projectConfigSnap,
		TenantConfigs:   tenantConfigSnap,
		TenantSeq:       tenantSeq,
	})
}

func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta identityPlatformMetadata
	if err := api.stateStore.Load(identityPlatformStateEntry, &meta); err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	if meta.Tenants != nil {
		api.tenants = meta.Tenants
	}
	if meta.OAuthIdpConfigs != nil {
		api.oauthConfigs = meta.OAuthIdpConfigs
	}
	if meta.ProjectConfigs != nil {
		api.projectConfigs = meta.ProjectConfigs
	}
	if meta.TenantConfigs != nil {
		api.tenantConfigs = meta.TenantConfigs
	}
	api.tenantSeq = meta.TenantSeq
	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, state.ErrNotFound)
}

func cloneTenant(t *Tenant) *Tenant {
	raw, _ := json.Marshal(t)
	var c Tenant
	_ = json.Unmarshal(raw, &c)
	return &c
}

func cloneOAuthConfig(c *OAuthIdpConfig) *OAuthIdpConfig {
	raw, _ := json.Marshal(c)
	var clone OAuthIdpConfig
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
