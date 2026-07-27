package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// EntryStore is the narrow persistence contract used by durable shims.
type EntryStore interface {
	Load(string, any) error
	Save(string, any) error
}

// GuardedEntryStore serializes saves and makes initialization, load, and save
// failures sticky. Once durability is uncertain, later writes fail closed.
type GuardedEntryStore struct {
	mu       sync.Mutex
	delegate EntryStore
	degraded error
}

// NewGuardedEntryStore wraps a store with sticky fail-closed semantics.
func NewGuardedEntryStore(delegate EntryStore, initializationErr error) *GuardedEntryStore {
	return &GuardedEntryStore{delegate: delegate, degraded: initializationErr}
}

func (s *GuardedEntryStore) Load(name string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.degraded != nil {
		return s.degraded
	}
	if s.delegate == nil {
		return nil
	}
	if err := s.delegate.Load(name, target); err != nil {
		if !errors.Is(err, ErrNotFound) {
			s.degraded = err
		}
		return err
	}
	return nil
}

func (s *GuardedEntryStore) Save(name string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.degraded != nil {
		return s.degraded
	}
	if s.delegate == nil {
		return nil
	}
	payload, err := json.Marshal(value)
	if err != nil {
		s.degraded = fmt.Errorf("snapshot state entry %q: %w", name, err)
		return s.degraded
	}
	if err := s.delegate.Save(name, json.RawMessage(payload)); err != nil {
		s.degraded = err
		return err
	}
	return nil
}

// Degraded reports the first durability failure.
func (s *GuardedEntryStore) Degraded() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.degraded
}

// ValidateResourceMaps applies common semantic checks to metadata maps. It
// rejects nil resources and mismatches between map keys and resource identity.
func ValidateResourceMaps(metadata any) error {
	return validateResourceValue(reflect.ValueOf(metadata), "")
}

func validateResourceValue(value reflect.Value, path string) error {
	if !value.IsValid() {
		return nil
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil
	}
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		fieldPath := value.Type().Field(i).Name
		if path != "" {
			fieldPath = path + "." + fieldPath
		}
		if field.Kind() != reflect.Map || field.Type().Key().Kind() != reflect.String {
			continue
		}
		iter := field.MapRange()
		for iter.Next() {
			key := iter.Key().String()
			resource := iter.Value()
			if (resource.Kind() == reflect.Pointer || resource.Kind() == reflect.Interface) && resource.IsNil() {
				return fmt.Errorf("%s[%q] is nil", fieldPath, key)
			}
			for resource.Kind() == reflect.Pointer || resource.Kind() == reflect.Interface {
				resource = resource.Elem()
			}
			if resource.Kind() != reflect.Struct {
				continue
			}
			if identity, ok := stringField(resource, "Name"); ok && strings.Contains(identity, "/") && identity != key {
				return fmt.Errorf("%s[%q].Name = %q", fieldPath, key, identity)
			}
		}
	}
	return nil
}

func stringField(value reflect.Value, name string) (string, bool) {
	field := value.FieldByName(name)
	if !field.IsValid() || field.Kind() != reflect.String || field.String() == "" {
		return "", false
	}
	return field.String(), true
}
