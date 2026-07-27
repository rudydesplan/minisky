// Package pagination provides deterministic opaque page tokens bound to the
// exact collection query and collection snapshot that produced them.
package pagination

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"
)

// ErrInvalidToken identifies malformed, stale, or cross-scope page tokens.
var ErrInvalidToken = errors.New("invalid page token")

// Scope identifies one stable list query.
type Scope struct {
	Service string
	Parent  string
	Filter  string
	OrderBy string
}

type tokenPayload struct {
	Version    int    `json:"v"`
	Scope      string `json:"s"`
	Generation string `json:"g"`
	After      string `json:"a"`
}

// Page returns a page from items. Items are sorted by key before paging. Tokens
// are deterministic, opaque, and invalidated when the collection or scope
// changes.
func Page[T any](items []T, pageSize int, pageToken string, scope Scope, key func(T) string) ([]T, string, error) {
	sorted := append([]T(nil), items...)
	sort.Slice(sorted, func(i, j int) bool { return key(sorted[i]) < key(sorted[j]) })
	keys := make([]string, len(sorted))
	for index := range sorted {
		keys[index] = key(sorted[index])
	}
	scopeDigest := digest([]string{scope.Service, scope.Parent, scope.Filter, scope.OrderBy})
	generation := digest(keys)

	start := 0
	if pageToken != "" {
		payload, err := decode(pageToken)
		if err != nil || payload.Version != 1 || payload.Scope != scopeDigest || payload.Generation != generation {
			return nil, "", ErrInvalidToken
		}
		index := sort.SearchStrings(keys, payload.After)
		if index >= len(keys) || keys[index] != payload.After {
			return nil, "", ErrInvalidToken
		}
		start = index + 1
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	end := start + pageSize
	if end > len(sorted) {
		end = len(sorted)
	}
	page := make([]T, end-start)
	copy(page, sorted[start:end])
	if end == len(sorted) {
		return page, "", nil
	}
	token, err := encode(tokenPayload{
		Version:    1,
		Scope:      scopeDigest,
		Generation: generation,
		After:      keys[end-1],
	})
	if err != nil {
		return nil, "", err
	}
	return page, token, nil
}

func digest(parts []string) string {
	encoded, _ := json.Marshal(parts)
	sum := sha256.Sum256(encoded)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func encode(payload tokenPayload) (string, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decode(token string) (tokenPayload, error) {
	var payload tokenPayload
	encoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return payload, err
	}
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return payload, err
	}
	return payload, nil
}
