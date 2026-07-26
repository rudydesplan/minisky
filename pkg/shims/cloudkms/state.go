package cloudkms

import (
	"encoding/json"
	"errors"
	"fmt"

	"minisky/pkg/state"
)

const cloudKMSStateEntry = "cloudkms/metadata"

type kmsStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

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
	return newAPIWithStore(store)
}

func newAPIWithStore(store kmsStateStore) (*API, error) {
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

	snapshot := api.snapshotMetadata()
	return api.saveMetadata(snapshot)
}

func (api *API) snapshotMetadata() cloudKMSMetadata {
	api.mu.RLock()
	defer api.mu.RUnlock()
	snapshot := cloudKMSMetadata{Locations: make(map[string]map[string]persistedKeyRing, len(api.store))}
	for location, rings := range api.store {
		snapshot.Locations[location] = make(map[string]persistedKeyRing, len(rings))
		for ringID, ring := range rings {
			ring.mu.Lock()
			savedRing := persistedKeyRing{Name: ring.Name, CreateTime: ring.CreateTime, Keys: make(map[string]persistedCryptoKey, len(ring.keys))}
			for keyID, key := range ring.keys {
				key.mu.Lock()
				savedKey := persistedCryptoKeyFromKeyLocked(key)
				key.mu.Unlock()
				savedRing.Keys[keyID] = savedKey
			}
			ring.mu.Unlock()
			snapshot.Locations[location][ringID] = savedRing
		}
	}
	return snapshot
}

func (api *API) saveMetadata(snapshot cloudKMSMetadata) error {
	if api.stateStore == nil {
		return nil
	}
	return api.stateStore.Save(cloudKMSStateEntry, snapshot)
}

func (api *API) saveMetadataTransaction(previous, next cloudKMSMetadata) error {
	if api.stateStore == nil {
		return nil
	}
	saveErr := api.stateStore.Save(cloudKMSStateEntry, next)
	if saveErr == nil {
		return nil
	}
	var durable cloudKMSMetadata
	loadErr := api.stateStore.Load(cloudKMSStateEntry, &durable)
	switch {
	case loadErr == nil && kmsMetadataEqual(durable, next):
		return nil
	case loadErr == nil && kmsMetadataEqual(durable, previous):
		return saveErr
	case errors.Is(loadErr, state.ErrNotFound) && len(previous.Locations) == 0:
		return saveErr
	default:
		ambiguous := fmt.Errorf("Cloud KMS save outcome is ambiguous: save: %w; read back: %v", saveErr, loadErr)
		api.markPersistenceDegraded(ambiguous)
		return ambiguous
	}
}

func cloneKMSMetadata(metadata cloudKMSMetadata) cloudKMSMetadata {
	payload, _ := json.Marshal(metadata)
	var clone cloudKMSMetadata
	_ = json.Unmarshal(payload, &clone)
	if clone.Locations == nil {
		clone.Locations = make(map[string]map[string]persistedKeyRing)
	}
	return clone
}

func kmsMetadataEqual(left, right cloudKMSMetadata) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func persistedCryptoKeyFromKeyLocked(key *CryptoKey) persistedCryptoKey {
	saved := persistedCryptoKey{
		Name: key.Name, Purpose: key.Purpose, CreateTime: key.CreateTime,
		VersionTemplate: cloneAnyMap(key.VersionTemplate), Labels: cloneStringMap(key.Labels),
	}
	for _, version := range key.versions {
		saved.Versions = append(saved.Versions, persistedCryptoKeyVersion{
			Name: version.Name, State: version.State, CreateTime: version.CreateTime,
			DestroyTime: version.DestroyTime, Algorithm: version.Algorithm,
			AESKey: append([]byte(nil), version.aesKey...),
		})
	}
	return saved
}

func persistedVersion(version *CryptoKeyVersion) persistedCryptoKeyVersion {
	return persistedCryptoKeyVersion{
		Name: version.Name, State: version.State, CreateTime: version.CreateTime,
		DestroyTime: version.DestroyTime, Algorithm: version.Algorithm,
		AESKey: append([]byte(nil), version.aesKey...),
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	result := make(map[string]any, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
