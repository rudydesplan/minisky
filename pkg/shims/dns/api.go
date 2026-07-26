package dns

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/registry"
	"minisky/pkg/state"

	"golang.org/x/net/dns/dnsmessage"
)

func init() {
	registry.Register("dns.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types
// ─────────────────────────────────────────────────────────────────────────────

// ManagedZone mirrors the Cloud DNS ManagedZone resource.
type ManagedZone struct {
	Kind                    string                   `json:"kind"`
	Name                    string                   `json:"name"`
	DnsName                 string                   `json:"dnsName"`
	Description             string                   `json:"description,omitempty"`
	ID                      uint64                   `json:"id,string"`
	NameServers             []string                 `json:"nameServers"`
	CreationTime            string                   `json:"creationTime"`
	Visibility              string                   `json:"visibility"` // public, private
	DNSSECConfig            *DNSSECConfig            `json:"dnssecConfig,omitempty"`
	Labels                  map[string]string        `json:"labels,omitempty"`
	PrivateVisibilityConfig *PrivateVisibilityConfig `json:"privateVisibilityConfig,omitempty"`
}

type DNSSECConfig struct {
	State string `json:"state"` // off, on, transfer
}

type PrivateVisibilityConfig struct {
	Networks []PrivateNetwork `json:"networks"`
}

type PrivateNetwork struct {
	NetworkURL string `json:"networkUrl"`
	Kind       string `json:"kind"`
}

// ResourceRecordSet (RRSet) mirrors the Cloud DNS ResourceRecordSet.
// The key fields used by Terraform and gcloud are name, type, ttl, rrdatas.
type ResourceRecordSet struct {
	Kind    string   `json:"kind"`
	Name    string   `json:"name"` // FQDN, e.g. "www.example.com."
	Type    string   `json:"type"` // A, AAAA, CNAME, MX, TXT, NS, SOA, PTR, SRV, CAA
	TTL     int64    `json:"ttl"`
	Rrdatas []string `json:"rrdatas"`
}

// Change represents an atomic batch of DNS record additions/deletions.
type Change struct {
	Kind      string              `json:"kind"`
	ID        string              `json:"id"`
	Status    string              `json:"status"` // pending → done
	StartTime string              `json:"startTime"`
	Additions []ResourceRecordSet `json:"additions,omitempty"`
	Deletions []ResourceRecordSet `json:"deletions,omitempty"`
	IsServing bool                `json:"isServing"`
}

type rrsetMutationBody struct {
	Kind    string   `json:"kind"`
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	TTL     *int64   `json:"ttl"`
	Rrdatas []string `json:"rrdatas"`
}

type changeMutationBody struct {
	Kind      string              `json:"kind"`
	Additions []rrsetMutationBody `json:"additions"`
	Deletions []rrsetMutationBody `json:"deletions"`
}

// ─────────────────────────────────────────────────────────────────────────────
// In-memory store helpers
// ─────────────────────────────────────────────────────────────────────────────

// zoneStore holds all data for a single managed zone.
type zoneStore struct {
	zone      *ManagedZone
	rrsets    map[string]*ResourceRecordSet // key: name+":"+type
	changes   []*Change
	changeSeq int
}

// ─────────────────────────────────────────────────────────────────────────────
// API shim
// ─────────────────────────────────────────────────────────────────────────────

// API is the high-fidelity Cloud DNS v1 shim.
type API struct {
	mu         sync.RWMutex
	mutationMu sync.Mutex
	store      stateStore
	zones      map[string]*zoneStore // key: project:zoneName
	zoneSeq    uint64
	initErr    error
	resolver   *Resolver
}

type stateStore interface {
	Load(string, any) error
	Save(string, any) error
}

const dnsStateEntry = "dns/metadata"
const maxDNSMutationBodyBytes int64 = 1 << 20

// maxDNSChangeRRSets is a MiniSky-specific safety boundary, not a claim of
// Google Cloud DNS parity.
const maxDNSChangeRRSets = 1000

type dnsMetadata struct {
	Zones   map[string]persistedZone `json:"zones"`
	ZoneSeq uint64                   `json:"zoneSeq"`
}

type persistedZone struct {
	Zone      *ManagedZone                  `json:"zone"`
	RRSets    map[string]*ResourceRecordSet `json:"rrsets"`
	Changes   []*Change                     `json:"changes"`
	ChangeSeq int                           `json:"changeSeq"`
}

func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	if err != nil {
		log.Printf("[Shim: Cloud DNS] state disabled: %v", err)
		api := newAPI(nil)
		startConfiguredResolver(api)
		return api
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		log.Printf("[Shim: Cloud DNS] state rehydration failed: %v", err)
		api = newAPI(nil)
		api.initErr = err
		return api
	}
	startConfiguredResolver(api)
	return api
}

// NewAPIWithStore constructs a DNS shim backed by the supplied metadata store.
// It reports unreadable state instead of silently replacing it.
func NewAPIWithStore(store stateStore) (*API, error) {
	api := newAPI(store)
	if store == nil {
		return api, nil
	}
	var persisted dnsMetadata
	if err := store.Load(dnsStateEntry, &persisted); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return api, nil
		}
		return nil, fmt.Errorf("load DNS metadata: %w", err)
	}
	api.zoneSeq = persisted.ZoneSeq
	for key, zone := range persisted.Zones {
		normalized, err := normalizePersistedZone(zone)
		if err != nil {
			return nil, fmt.Errorf("load DNS metadata zone %q: %w", key, err)
		}
		api.zones[key] = &zoneStore{
			zone:      normalized.Zone,
			rrsets:    normalized.RRSets,
			changes:   normalized.Changes,
			changeSeq: normalized.ChangeSeq,
		}
	}
	return api, nil
}

func newAPI(store stateStore) *API {
	return &API{
		store: store,
		zones: make(map[string]*zoneStore),
	}
}

// Shutdown closes the optional UDP resolver through MiniSky's plugin lifecycle.
func (api *API) Shutdown(ctx context.Context) error {
	_ = ctx
	if api.resolver == nil {
		return nil
	}
	return api.resolver.Close()
}

func (api *API) persistMetadata() error {
	if api.store == nil {
		return nil
	}
	api.mu.RLock()
	metadata := snapshotDNSMetadata(api.zones, api.zoneSeq)
	api.mu.RUnlock()
	return api.store.Save(dnsStateEntry, metadata)
}

func (api *API) beginMutation() dnsMetadata {
	api.mutationMu.Lock()
	api.mu.RLock()
	before := snapshotDNSMetadata(api.zones, api.zoneSeq)
	api.mu.RUnlock()
	return before
}

func (api *API) abortMutation() {
	api.mutationMu.Unlock()
}

func (api *API) persistOrRollback(w http.ResponseWriter, before dnsMetadata) bool {
	if err := api.persistMetadata(); err != nil {
		log.Printf("[Shim: Cloud DNS] persist metadata: %v", err)
		api.mu.Lock()
		api.restoreMetadataLocked(before)
		api.mu.Unlock()
		api.mutationMu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
		writeError(w, http.StatusInternalServerError, "INTERNAL", "Failed to persist DNS metadata")
		return false
	}
	api.mutationMu.Unlock()
	return true
}

// ServeHTTP dispatches Cloud DNS v1 paths.
//
// Supported paths (dns.googleapis.com):
//
//	POST   /dns/v1/projects/{project}/managedZones
//	GET    /dns/v1/projects/{project}/managedZones
//	GET    /dns/v1/projects/{project}/managedZones/{zone}
//	PATCH  /dns/v1/projects/{project}/managedZones/{zone}
//	DELETE /dns/v1/projects/{project}/managedZones/{zone}
//	GET    /dns/v1/projects/{project}/managedZones/{zone}/rrsets
//	POST   /dns/v1/projects/{project}/managedZones/{zone}/rrsets
//	DELETE /dns/v1/projects/{project}/managedZones/{zone}/rrsets/{name}/{type}
//	POST   /dns/v1/projects/{project}/managedZones/{zone}/changes
//	GET    /dns/v1/projects/{project}/managedZones/{zone}/changes
//	GET    /dns/v1/projects/{project}/managedZones/{zone}/changes/{changeId}
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if api.initErr != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		writeError(w, http.StatusServiceUnavailable, "FAILED_PRECONDITION", "Cloud DNS state is unavailable")
		return
	}
	log.Printf("[Shim: Cloud DNS] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 5 || parts[0] != "dns" || parts[1] != "v1" ||
		parts[2] != "projects" || parts[3] == "" || parts[4] != "managedZones" {
		writeDNSNotFound(w, path)
		return
	}
	project := parts[3]
	switch {
	case len(parts) == 5 || len(parts) == 6 && parts[5] != "":
		api.routeZones(w, r, project, path)
	case len(parts) == 7 && parts[5] != "" && parts[6] == "rrsets",
		len(parts) == 9 && parts[5] != "" && parts[6] == "rrsets" && parts[7] != "" && parts[8] != "":
		api.routeRRSets(w, r, project, parts[5], path)
	case len(parts) == 7 && parts[5] != "" && parts[6] == "changes",
		len(parts) == 8 && parts[5] != "" && parts[6] == "changes" && parts[7] != "":
		api.routeChanges(w, r, project, parts[5], path)
	default:
		writeDNSNotFound(w, path)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Managed Zones
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeZones(w http.ResponseWriter, r *http.Request, project, path string) {
	zoneName := extractSegmentAfter(path, "managedZones")

	switch r.Method {
	case http.MethodPost:
		api.createZone(w, r, project)
	case http.MethodGet:
		if zoneName != "" {
			api.getZone(w, project, zoneName)
		} else {
			api.listZones(w, r, project)
		}
	case http.MethodPatch:
		api.patchZone(w, r, project, zoneName)
	case http.MethodDelete:
		api.deleteZone(w, project, zoneName)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (api *API) createZone(w http.ResponseWriter, r *http.Request, project string) {
	var body ManagedZone
	if err := decodeDNSMutation(w, r, &body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	if body.Name == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "'name' is required")
		return
	}
	if body.DnsName == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "'dnsName' is required")
		return
	}

	dnsName, err := canonicalDNSName(body.DnsName)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid 'dnsName': "+err.Error())
		return
	}
	visibility := strings.ToLower(strings.TrimSpace(body.Visibility))
	if visibility == "" {
		visibility = "public"
	}
	if visibility != "public" && visibility != "private" {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "managed-zone visibility must be public or private")
		return
	}

	before := api.beginMutation()
	key := zoneKey(project, body.Name)
	api.mu.Lock()
	if _, exists := api.zones[key]; exists {
		api.mu.Unlock()
		api.abortMutation()
		w.WriteHeader(http.StatusConflict)
		writeError(w, http.StatusConflict, "ALREADY_EXISTS",
			fmt.Sprintf("ManagedZone '%s' already exists in project '%s'", body.Name, project))
		return
	}
	api.zoneSeq++
	id := api.zoneSeq
	api.mu.Unlock()

	zone := &ManagedZone{
		Kind:                    "dns#managedZone",
		Name:                    body.Name,
		DnsName:                 dnsName,
		Description:             body.Description,
		ID:                      id,
		Labels:                  body.Labels,
		Visibility:              visibility,
		DNSSECConfig:            body.DNSSECConfig,
		PrivateVisibilityConfig: body.PrivateVisibilityConfig,
		CreationTime:            time.Now().UTC().Format(time.RFC3339),
		// Return realistic-looking MiniSky name servers
		NameServers: []string{
			fmt.Sprintf("ns-cloud-a1.minisky.dev."),
			fmt.Sprintf("ns-cloud-a2.minisky.dev."),
			fmt.Sprintf("ns-cloud-a3.minisky.dev."),
			fmt.Sprintf("ns-cloud-a4.minisky.dev."),
		},
	}

	// Seed the zone with mandatory SOA and NS records (mirrors GCP behaviour)
	soaRdata := fmt.Sprintf("ns-cloud-a1.minisky.dev. cloud-dns-hostmaster.google.com. 1 21600 3600 259200 300")
	nsRdatas := zone.NameServers

	store := &zoneStore{
		zone: zone,
		rrsets: map[string]*ResourceRecordSet{
			rrKey(dnsName, "SOA"): {
				Kind:    "dns#resourceRecordSet",
				Name:    dnsName,
				Type:    "SOA",
				TTL:     21600,
				Rrdatas: []string{soaRdata},
			},
			rrKey(dnsName, "NS"): {
				Kind:    "dns#resourceRecordSet",
				Name:    dnsName,
				Type:    "NS",
				TTL:     21600,
				Rrdatas: nsRdatas,
			},
		},
	}

	api.mu.Lock()
	api.zones[key] = store
	api.mu.Unlock()

	if !api.persistOrRollback(w, before) {
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(zone)
}

func (api *API) getZone(w http.ResponseWriter, project, zoneName string) {
	key := zoneKey(project, zoneName)
	api.mu.RLock()
	store, ok := api.zones[key]
	api.mu.RUnlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", fmt.Sprintf("ManagedZone '%s' not found in project '%s'", zoneName, project))
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(store.zone)
}

func (api *API) listZones(w http.ResponseWriter, r *http.Request, project string) {
	// Optional ?dnsName= filter
	filterDNS := r.URL.Query().Get("dnsName")

	prefix := project + ":"
	api.mu.RLock()
	items := []*ManagedZone{}
	for k, v := range api.zones {
		if strings.HasPrefix(k, prefix) {
			if filterDNS == "" || strings.EqualFold(v.zone.DnsName, filterDNS) {
				items = append(items, v.zone)
			}
		}
	}
	api.mu.RUnlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"kind":         "dns#managedZonesListResponse",
		"managedZones": items,
	})
}

func (api *API) patchZone(w http.ResponseWriter, r *http.Request, project, zoneName string) {
	key := zoneKey(project, zoneName)
	var patch ManagedZone
	if err := decodeDNSMutation(w, r, &patch); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	before := api.beginMutation()
	api.mu.Lock()
	store, ok := api.zones[key]
	if !ok {
		api.mu.Unlock()
		api.abortMutation()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "ManagedZone "+zoneName+" not found")
		return
	}

	if patch.Description != "" {
		store.zone.Description = patch.Description
	}
	if patch.Labels != nil {
		store.zone.Labels = patch.Labels
	}
	zone := store.zone
	api.mu.Unlock()

	if !api.persistOrRollback(w, before) {
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(zone)
}

func (api *API) deleteZone(w http.ResponseWriter, project, zoneName string) {
	key := zoneKey(project, zoneName)
	before := api.beginMutation()
	api.mu.Lock()
	store, ok := api.zones[key]
	if ok {
		// GCP refuses to delete a zone that has non-SOA/NS records
		nonSystem := 0
		for _, rr := range store.rrsets {
			if rr.Type != "SOA" && rr.Type != "NS" {
				nonSystem++
			}
		}
		if nonSystem > 0 {
			api.mu.Unlock()
			api.abortMutation()
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, 400, "FAILED_PRECONDITION",
				fmt.Sprintf("Zone '%s' cannot be deleted because it still contains non-NS/SOA resource record sets", zoneName))
			return
		}
		delete(api.zones, key)
	}
	api.mu.Unlock()

	if !ok {
		api.abortMutation()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "ManagedZone "+zoneName+" not found")
		return
	}
	if !api.persistOrRollback(w, before) {
		return
	}
	// GCP returns 204 No Content on delete
	w.WriteHeader(http.StatusNoContent)
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource Record Sets
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeRRSets(w http.ResponseWriter, r *http.Request, project, zoneName, path string) {
	rrName, rrType, item, valid := parseRRSetPath(path)
	if !valid {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Cloud DNS resource not found: "+path)
		return
	}

	switch r.Method {
	case http.MethodPost:
		if item {
			w.WriteHeader(http.StatusMethodNotAllowed)
			writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
			return
		}
		api.createRRSet(w, r, project, zoneName)
	case http.MethodGet:
		if item {
			api.getRRSet(w, project, zoneName, rrName, rrType)
		} else {
			api.listRRSets(w, r, project, zoneName)
		}
	case http.MethodDelete:
		if !item {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, 400, "INVALID_ARGUMENT", "Resource record set name and type are required for delete")
			return
		}
		api.deleteRRSet(w, project, zoneName, rrName, rrType)
	case http.MethodPut:
		if !item {
			w.WriteHeader(http.StatusMethodNotAllowed)
			writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
			return
		}
		// PATCH/PUT updates a single RRSet
		api.putRRSet(w, r, project, zoneName, rrName, rrType)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed")
	}
}

func (api *API) createRRSet(w http.ResponseWriter, r *http.Request, project, zoneName string) {
	key := zoneKey(project, zoneName)
	rr, ttlSupplied, err := decodeRRSetMutation(w, r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	before := api.beginMutation()
	api.mu.Lock()
	store, ok := api.zones[key]
	api.mu.Unlock()
	if !ok {
		api.abortMutation()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "ManagedZone "+zoneName+" not found")
		return
	}
	if err := validateAndNormalizeRRSet(&rr, ttlSupplied, store.zone.DnsName); err != nil {
		api.abortMutation()
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	rr.Kind = "dns#resourceRecordSet"

	rrk := rrKey(rr.Name, rr.Type)
	api.mu.Lock()
	// Check for duplicate
	if _, exists := store.rrsets[rrk]; exists {
		api.mu.Unlock()
		api.abortMutation()
		w.WriteHeader(http.StatusConflict)
		writeError(w, 409, "ALREADY_EXISTS",
			fmt.Sprintf("ResourceRecordSet '%s' of type '%s' already exists", rr.Name, rr.Type))
		return
	}
	store.rrsets[rrk] = &rr
	api.mu.Unlock()

	if !api.persistOrRollback(w, before) {
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(&rr)
}

func (api *API) listRRSets(w http.ResponseWriter, r *http.Request, project, zoneName string) {
	key := zoneKey(project, zoneName)
	api.mu.RLock()
	store, ok := api.zones[key]
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "ManagedZone "+zoneName+" not found")
		return
	}

	// Optional filters: ?name=, ?type=
	filterName := r.URL.Query().Get("name")
	if filterName != "" {
		var err error
		filterName, err = canonicalDNSName(filterName)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid record name filter")
			return
		}
	}
	filterType := strings.ToUpper(r.URL.Query().Get("type"))

	api.mu.RLock()
	items := []*ResourceRecordSet{}
	for _, v := range store.rrsets {
		if filterName != "" && v.Name != filterName {
			continue
		}
		if filterType != "" && v.Type != filterType {
			continue
		}
		items = append(items, v)
	}
	api.mu.RUnlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"kind":   "dns#resourceRecordSetsListResponse",
		"rrsets": items,
	})
}

func (api *API) getRRSet(w http.ResponseWriter, project, zoneName, name, rrType string) {
	key := zoneKey(project, zoneName)
	api.mu.RLock()
	store, zoneExists := api.zones[key]
	var rrset *ResourceRecordSet
	if zoneExists {
		rrset = cloneRRSet(store.rrsets[rrKey(name, rrType)])
	}
	api.mu.RUnlock()
	if rrset == nil {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, http.StatusNotFound, "NOT_FOUND",
			fmt.Sprintf("ResourceRecordSet '%s/%s' not found in zone '%s'", name, rrType, zoneName))
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(rrset)
}

func (api *API) deleteRRSet(w http.ResponseWriter, project, zoneName, name, rrType string) {
	key := zoneKey(project, zoneName)
	before := api.beginMutation()
	api.mu.Lock()
	store, ok := api.zones[key]
	if ok {
		rrk := rrKey(name, rrType)
		if _, exists := store.rrsets[rrk]; !exists {
			ok = false
		} else {
			delete(store.rrsets, rrk)
		}
	}
	api.mu.Unlock()

	if !ok {
		api.abortMutation()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", fmt.Sprintf("ResourceRecordSet '%s/%s' not found in zone '%s'", name, rrType, zoneName))
		return
	}
	if !api.persistOrRollback(w, before) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (api *API) putRRSet(w http.ResponseWriter, r *http.Request, project, zoneName, name, rrType string) {
	key := zoneKey(project, zoneName)
	rr, ttlSupplied, err := decodeRRSetMutation(w, r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	before := api.beginMutation()
	api.mu.Lock()
	store, ok := api.zones[key]
	if !ok {
		api.mu.Unlock()
		api.abortMutation()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "ManagedZone "+zoneName+" not found")
		return
	}
	if err := validateAndNormalizeRRSet(&rr, ttlSupplied, store.zone.DnsName); err != nil {
		api.mu.Unlock()
		api.abortMutation()
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	rr.Kind = "dns#resourceRecordSet"

	store.rrsets[rrKey(rr.Name, rr.Type)] = &rr
	api.mu.Unlock()

	if !api.persistOrRollback(w, before) {
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(&rr)
}

// ─────────────────────────────────────────────────────────────────────────────
// Changes  (atomic batches of additions + deletions)
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeChanges(w http.ResponseWriter, r *http.Request, project, zoneName, path string) {
	changeID := extractSegmentAfter(path, "changes")

	switch r.Method {
	case http.MethodPost:
		api.createChange(w, r, project, zoneName)
	case http.MethodGet:
		if changeID != "" {
			api.getChange(w, project, zoneName, changeID)
		} else {
			api.listChanges(w, project, zoneName)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// createChange applies an atomic set of DNS additions and deletions.
// The GCP spec says: deletions are applied before additions in the same request.
func (api *API) createChange(w http.ResponseWriter, r *http.Request, project, zoneName string) {
	zKey := zoneKey(project, zoneName)
	var request changeMutationBody
	if err := decodeDNSMutation(w, r, &request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, 400, "INVALID_ARGUMENT", "Parse error: "+err.Error())
		return
	}
	if len(request.Additions)+len(request.Deletions) > maxDNSChangeRRSets {
		w.WriteHeader(http.StatusBadRequest)
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
			fmt.Sprintf("changes are limited to %d total additions and deletions in MiniSky", maxDNSChangeRRSets))
		return
	}
	body := Change{
		Kind:      request.Kind,
		Additions: make([]ResourceRecordSet, len(request.Additions)),
		Deletions: make([]ResourceRecordSet, len(request.Deletions)),
	}
	before := api.beginMutation()
	api.mu.Lock()
	store, ok := api.zones[zKey]
	if !ok {
		api.mu.Unlock()
		api.abortMutation()
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "ManagedZone "+zoneName+" not found")
		return
	}
	for i, addition := range request.Additions {
		body.Additions[i] = addition.resourceRecordSet()
		if err := validateAndNormalizeRRSet(&body.Additions[i], addition.TTL != nil, store.zone.DnsName); err != nil {
			api.mu.Unlock()
			api.abortMutation()
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid addition: "+err.Error())
			return
		}
	}
	for i, deletion := range request.Deletions {
		body.Deletions[i] = deletion.resourceRecordSet()
		if err := validateAndNormalizeRRSet(&body.Deletions[i], deletion.TTL != nil, store.zone.DnsName); err != nil {
			api.mu.Unlock()
			api.abortMutation()
			w.WriteHeader(http.StatusBadRequest)
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid deletion: "+err.Error())
			return
		}
	}

	// Validate: deletions must exist
	for _, del := range body.Deletions {
		rrk := rrKey(del.Name, del.Type)
		if _, exists := store.rrsets[rrk]; !exists {
			api.mu.Unlock()
			api.abortMutation()
			w.WriteHeader(http.StatusNotFound)
			writeError(w, 404, "NOT_FOUND",
				fmt.Sprintf("Cannot delete non-existent ResourceRecordSet '%s' of type '%s'", del.Name, del.Type))
			return
		}
	}

	// Apply deletions first
	for _, del := range body.Deletions {
		delete(store.rrsets, rrKey(del.Name, del.Type))
	}

	// Apply additions
	for i, add := range body.Additions {
		body.Additions[i].Kind = "dns#resourceRecordSet"
		store.rrsets[rrKey(add.Name, add.Type)] = &body.Additions[i]
	}

	// Record the change
	store.changeSeq++
	changeID := fmt.Sprintf("%d", store.changeSeq)
	change := &Change{
		Kind:      "dns#change",
		ID:        changeID,
		Status:    "done", // Cloud DNS changes are synchronous; status flips to done immediately
		StartTime: time.Now().UTC().Format(time.RFC3339),
		Additions: body.Additions,
		Deletions: body.Deletions,
		IsServing: true,
	}
	store.changes = append(store.changes, change)
	api.mu.Unlock()

	if !api.persistOrRollback(w, before) {
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(change)
}

func (api *API) getChange(w http.ResponseWriter, project, zoneName, changeID string) {
	zKey := zoneKey(project, zoneName)
	api.mu.RLock()
	store, ok := api.zones[zKey]
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "ManagedZone "+zoneName+" not found")
		return
	}

	api.mu.RLock()
	var found *Change
	for _, c := range store.changes {
		if c.ID == changeID {
			found = c
			break
		}
	}
	api.mu.RUnlock()

	if found == nil {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "Change "+changeID+" not found in zone "+zoneName)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(found)
}

func (api *API) listChanges(w http.ResponseWriter, project, zoneName string) {
	zKey := zoneKey(project, zoneName)
	api.mu.RLock()
	store, ok := api.zones[zKey]
	api.mu.RUnlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		writeError(w, 404, "NOT_FOUND", "ManagedZone "+zoneName+" not found")
		return
	}

	api.mu.RLock()
	changes := make([]*Change, len(store.changes))
	copy(changes, store.changes)
	api.mu.RUnlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"kind":    "dns#changesListResponse",
		"changes": changes,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

func zoneKey(project, zoneName string) string { return project + ":" + zoneName }
func rrKey(name, rrType string) string {
	return strings.ToLower(ensureFQDN(name)) + ":" + strings.ToUpper(rrType)
}

// extractSegmentAfter returns the path segment immediately after keyword.
func extractSegmentAfter(path, keyword string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == keyword && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// parseRRSetPath distinguishes the exact collection path from the generated
// client's exact item path /rrsets/{name}/{type}.
func parseRRSetPath(path string) (name, rrType string, item, valid bool) {
	idx := strings.Index(path, "/rrsets/")
	if idx == -1 {
		return "", "", false, strings.HasSuffix(path, "/rrsets")
	}
	rest := path[idx+len("/rrsets/"):]
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false, false
	}
	return parts[0], parts[1], true, true
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"code":    code,
			"status":  status,
			"message": message,
		},
	})
}

func writeDNSNotFound(w http.ResponseWriter, path string) {
	w.WriteHeader(http.StatusNotFound)
	writeError(w, http.StatusNotFound, "NOT_FOUND", "Cloud DNS resource not found: "+path)
}

func decodeDNSMutation(w http.ResponseWriter, r *http.Request, target any) error {
	body := http.MaxBytesReader(w, r.Body, maxDNSMutationBodyBytes)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain exactly one JSON value")
		}
		return err
	}
	return nil
}

func decodeRRSetMutation(w http.ResponseWriter, r *http.Request) (ResourceRecordSet, bool, error) {
	var body rrsetMutationBody
	if err := decodeDNSMutation(w, r, &body); err != nil {
		return ResourceRecordSet{}, false, err
	}
	return body.resourceRecordSet(), body.TTL != nil, nil
}

func (body rrsetMutationBody) resourceRecordSet() ResourceRecordSet {
	rrset := ResourceRecordSet{
		Kind:    body.Kind,
		Name:    body.Name,
		Type:    body.Type,
		Rrdatas: body.Rrdatas,
	}
	if body.TTL != nil {
		rrset.TTL = *body.TTL
	}
	return rrset
}

func validateAndNormalizeRRSet(rrset *ResourceRecordSet, ttlSupplied bool, zoneDNSName string) error {
	if rrset.Name == "" || rrset.Type == "" {
		return errors.New("'name' and 'type' are required")
	}
	name, err := canonicalDNSName(rrset.Name)
	if err != nil {
		return fmt.Errorf("invalid record name: %w", err)
	}
	zoneName, err := canonicalDNSName(zoneDNSName)
	if err != nil {
		return fmt.Errorf("managed zone has invalid DNS name: %w", err)
	}
	if !dnsNameWithinZone(name, zoneName) {
		return fmt.Errorf("record name %q is outside managed zone %q", name, zoneName)
	}
	rrset.Name = name
	rrset.Type = strings.ToUpper(rrset.Type)
	switch {
	case ttlSupplied && rrset.TTL <= 0:
		return errors.New("'ttl' must be positive when supplied")
	case !ttlSupplied:
		rrset.TTL = 300
	case rrset.TTL > math.MaxUint32:
		return errors.New("'ttl' exceeds the resolver-supported maximum")
	}
	if len(rrset.Rrdatas) == 0 {
		return errors.New("'rrdatas' must contain at least one value")
	}
	if len(rrset.Rrdatas) > 1000 {
		return errors.New("'rrdatas' is limited to 1000 values")
	}
	for i, value := range rrset.Rrdatas {
		value = strings.TrimSpace(value)
		if value == "" {
			return errors.New("'rrdatas' values must be nonempty")
		}
		switch rrset.Type {
		case "A":
			address, err := netip.ParseAddr(value)
			if err != nil || !address.Is4() {
				return fmt.Errorf("invalid A record data %q", value)
			}
		case "AAAA":
			address, err := netip.ParseAddr(value)
			if err != nil || !address.Is6() || address.Is4In6() {
				return fmt.Errorf("invalid AAAA record data %q", value)
			}
		case "CNAME":
			if !strings.HasSuffix(value, ".") {
				return fmt.Errorf("CNAME record data %q must be an absolute dot-terminated DNS name", value)
			}
			value, err = canonicalDNSName(value)
			if err != nil {
				return fmt.Errorf("invalid CNAME record data %q", value)
			}
		}
		rrset.Rrdatas[i] = value
	}
	return nil
}

func canonicalDNSName(value string) (string, error) {
	if value == "" || strings.TrimSpace(value) != value {
		return "", errors.New("DNS name must be nonempty and contain no surrounding whitespace")
	}
	name := strings.ToLower(ensureFQDN(value))
	labels := strings.Split(strings.TrimSuffix(name, "."), ".")
	for _, label := range labels {
		if label == "" {
			return "", errors.New("DNS name contains an empty label")
		}
	}
	if _, err := dnsmessage.NewName(name); err != nil {
		return "", err
	}
	return name, nil
}

func dnsNameWithinZone(name, zone string) bool {
	if name == zone {
		return true
	}
	if !strings.HasSuffix(name, zone) {
		return false
	}
	prefix := strings.TrimSuffix(name, zone)
	return strings.HasSuffix(prefix, ".")
}

func clampLegacyTTL(ttl int64) int64 {
	if ttl < 0 {
		return 0
	}
	if ttl > math.MaxUint32 {
		return math.MaxUint32
	}
	return ttl
}

func normalizePersistedZone(persisted persistedZone) (persistedZone, error) {
	normalized := persistedZone{
		Zone:      cloneManagedZone(persisted.Zone),
		RRSets:    make(map[string]*ResourceRecordSet, len(persisted.RRSets)),
		Changes:   make([]*Change, len(persisted.Changes)),
		ChangeSeq: persisted.ChangeSeq,
	}
	if normalized.Zone == nil {
		return persistedZone{}, errors.New("managed zone metadata is missing")
	}
	dnsName, err := canonicalDNSName(normalized.Zone.DnsName)
	if err != nil {
		return persistedZone{}, fmt.Errorf("invalid managed-zone DNS name: %w", err)
	}
	normalized.Zone.DnsName = dnsName
	visibility := strings.ToLower(strings.TrimSpace(normalized.Zone.Visibility))
	if visibility == "" {
		visibility = "public"
	}
	if visibility != "public" && visibility != "private" {
		return persistedZone{}, fmt.Errorf("unsupported managed-zone visibility %q", normalized.Zone.Visibility)
	}
	normalized.Zone.Visibility = visibility
	for _, rrset := range persisted.RRSets {
		clone, err := normalizePersistedRRSet(rrset, dnsName)
		if err != nil {
			return persistedZone{}, err
		}
		canonicalKey := rrKey(clone.Name, clone.Type)
		if _, exists := normalized.RRSets[canonicalKey]; exists {
			return persistedZone{}, fmt.Errorf("canonical resource record key %q is duplicated", canonicalKey)
		}
		normalized.RRSets[canonicalKey] = clone
	}
	for i, change := range persisted.Changes {
		if change == nil {
			continue
		}
		clone := *change
		clone.Additions = cloneRRSets(change.Additions)
		clone.Deletions = cloneRRSets(change.Deletions)
		for j := range clone.Additions {
			rrset, err := normalizePersistedRRSet(&clone.Additions[j], dnsName)
			if err != nil {
				return persistedZone{}, err
			}
			clone.Additions[j] = *rrset
		}
		for j := range clone.Deletions {
			rrset, err := normalizePersistedRRSet(&clone.Deletions[j], dnsName)
			if err != nil {
				return persistedZone{}, err
			}
			clone.Deletions[j] = *rrset
		}
		normalized.Changes[i] = &clone
	}
	return normalized, nil
}

func normalizePersistedRRSet(rrset *ResourceRecordSet, zoneDNSName string) (*ResourceRecordSet, error) {
	if rrset == nil {
		return nil, errors.New("resource record set metadata is missing")
	}
	clone := cloneRRSet(rrset)
	name, err := canonicalDNSName(clone.Name)
	if err != nil {
		return nil, fmt.Errorf("invalid persisted record name: %w", err)
	}
	if !dnsNameWithinZone(name, zoneDNSName) {
		return nil, fmt.Errorf("persisted record name %q is outside managed zone %q", name, zoneDNSName)
	}
	clone.Name = name
	clone.Type = strings.ToUpper(clone.Type)
	clone.TTL = clampLegacyTTL(clone.TTL)
	return clone, nil
}

func snapshotDNSMetadata(zones map[string]*zoneStore, zoneSeq uint64) dnsMetadata {
	metadata := dnsMetadata{
		Zones:   make(map[string]persistedZone, len(zones)),
		ZoneSeq: zoneSeq,
	}
	for key, store := range zones {
		persisted := persistedZone{
			Zone:      cloneManagedZone(store.zone),
			RRSets:    make(map[string]*ResourceRecordSet, len(store.rrsets)),
			Changes:   make([]*Change, len(store.changes)),
			ChangeSeq: store.changeSeq,
		}
		for rrKey, rrset := range store.rrsets {
			persisted.RRSets[rrKey] = cloneRRSet(rrset)
		}
		for i, change := range store.changes {
			clone := *change
			clone.Additions = cloneRRSets(change.Additions)
			clone.Deletions = cloneRRSets(change.Deletions)
			persisted.Changes[i] = &clone
		}
		metadata.Zones[key] = persisted
	}
	return metadata
}

func (api *API) restoreMetadataLocked(metadata dnsMetadata) {
	api.zoneSeq = metadata.ZoneSeq
	api.zones = make(map[string]*zoneStore, len(metadata.Zones))
	for key, persisted := range metadata.Zones {
		rrsets := make(map[string]*ResourceRecordSet, len(persisted.RRSets))
		for _, rrset := range persisted.RRSets {
			clone := cloneRRSet(rrset)
			if clone == nil {
				continue
			}
			clone.Name = strings.ToLower(ensureFQDN(clone.Name))
			clone.Type = strings.ToUpper(clone.Type)
			clone.TTL = clampLegacyTTL(clone.TTL)
			rrsets[rrKey(clone.Name, clone.Type)] = clone
		}
		changes := make([]*Change, len(persisted.Changes))
		for i, change := range persisted.Changes {
			clone := *change
			clone.Additions = cloneRRSets(change.Additions)
			clone.Deletions = cloneRRSets(change.Deletions)
			changes[i] = &clone
		}
		api.zones[key] = &zoneStore{
			zone:      cloneManagedZone(persisted.Zone),
			rrsets:    rrsets,
			changes:   changes,
			changeSeq: persisted.ChangeSeq,
		}
	}
}

func cloneManagedZone(zone *ManagedZone) *ManagedZone {
	if zone == nil {
		return nil
	}
	clone := *zone
	clone.NameServers = append([]string(nil), zone.NameServers...)
	if zone.Labels != nil {
		clone.Labels = make(map[string]string, len(zone.Labels))
		for key, value := range zone.Labels {
			clone.Labels[key] = value
		}
	}
	if zone.DNSSECConfig != nil {
		config := *zone.DNSSECConfig
		clone.DNSSECConfig = &config
	}
	if zone.PrivateVisibilityConfig != nil {
		config := *zone.PrivateVisibilityConfig
		config.Networks = append([]PrivateNetwork(nil), config.Networks...)
		clone.PrivateVisibilityConfig = &config
	}
	return &clone
}

func cloneRRSet(rrset *ResourceRecordSet) *ResourceRecordSet {
	if rrset == nil {
		return nil
	}
	clone := *rrset
	clone.Rrdatas = append([]string(nil), rrset.Rrdatas...)
	return &clone
}

func cloneRRSets(rrsets []ResourceRecordSet) []ResourceRecordSet {
	clones := make([]ResourceRecordSet, len(rrsets))
	for i := range rrsets {
		clones[i] = *cloneRRSet(&rrsets[i])
	}
	return clones
}
