package cloudkms

import (
	"errors"
	"fmt"

	"minisky/pkg/state"
)

const cloudKMSStateEntry = "cloudkms/metadata"

type cloudKMSMetadata struct {
	Locations map[string]map[string]persistedKeyRing `json:"locations"`
}

type persistedKeyRing struct {
	Name       string                        `json:"name"`
	CreateTime string                        `json:"createTime"`
	Keys       map[string]persistedCryptoKey `json:"keys"`
}

type persistedCryptoKey struct {
	Name            string                      `json:"name"`
	Purpose         string                      `json:"purpose"`
	CreateTime      string                      `json:"createTime"`
	VersionTemplate map[string]any              `json:"versionTemplate,omitempty"`
	Labels          map[string]string           `json:"labels,omitempty"`
	Versions        []persistedCryptoKeyVersion `json:"versions"`
}

type persistedCryptoKeyVersion struct {
	Name        string `json:"name"`
	State       string `json:"state"`
	CreateTime  string `json:"createTime"`
	DestroyTime string `json:"destroyTime,omitempty"`
	Algorithm   string `json:"algorithm"`
	AESKey      []byte `json:"aesKey,omitempty"`
}

func NewAPIWithStore(store *state.Store) (*API, error) {
	api := newAPI(store)
	if store == nil {
		return api, nil
	}
	var persisted cloudKMSMetadata
	if err := store.Load(cloudKMSStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load Cloud KMS metadata: %w", err)
	}
	for location, rings := range persisted.Locations {
		api.store[location] = make(map[string]*KeyRing, len(rings))
		for ringID, savedRing := range rings {
			ring := &KeyRing{Name: savedRing.Name, CreateTime: savedRing.CreateTime, keys: make(map[string]*CryptoKey, len(savedRing.Keys))}
			for keyID, savedKey := range savedRing.Keys {
				key := &CryptoKey{
					Name: savedKey.Name, Purpose: savedKey.Purpose, CreateTime: savedKey.CreateTime,
					VersionTemplate: savedKey.VersionTemplate, Labels: savedKey.Labels,
				}
				for _, savedVersion := range savedKey.Versions {
					key.versions = append(key.versions, &CryptoKeyVersion{
						Name: savedVersion.Name, State: savedVersion.State, CreateTime: savedVersion.CreateTime,
						DestroyTime: savedVersion.DestroyTime, Algorithm: savedVersion.Algorithm,
						aesKey: append([]byte(nil), savedVersion.AESKey...),
					})
				}
				ring.keys[keyID] = key
			}
			api.store[location][ringID] = ring
		}
	}
	return api, nil
}

func (api *API) persistMetadata() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	snapshot := cloudKMSMetadata{Locations: make(map[string]map[string]persistedKeyRing, len(api.store))}
	for location, rings := range api.store {
		snapshot.Locations[location] = make(map[string]persistedKeyRing, len(rings))
		for ringID, ring := range rings {
			ring.mu.Lock()
			savedRing := persistedKeyRing{Name: ring.Name, CreateTime: ring.CreateTime, Keys: make(map[string]persistedCryptoKey, len(ring.keys))}
			for keyID, key := range ring.keys {
				key.mu.Lock()
				savedKey := persistedCryptoKey{
					Name: key.Name, Purpose: key.Purpose, CreateTime: key.CreateTime,
					VersionTemplate: key.VersionTemplate, Labels: key.Labels,
				}
				for _, version := range key.versions {
					savedKey.Versions = append(savedKey.Versions, persistedCryptoKeyVersion{
						Name: version.Name, State: version.State, CreateTime: version.CreateTime,
						DestroyTime: version.DestroyTime, Algorithm: version.Algorithm,
						AESKey: append([]byte(nil), version.aesKey...),
					})
				}
				key.mu.Unlock()
				savedRing.Keys[keyID] = savedKey
			}
			ring.mu.Unlock()
			snapshot.Locations[location][ringID] = savedRing
		}
	}
	api.mu.RUnlock()
	return api.stateStore.Save(cloudKMSStateEntry, snapshot)
}
