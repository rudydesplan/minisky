package dns

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/state"
)

func TestRRSetItemGetUsesGeneratedClientShape(t *testing.T) {
	api, err := NewAPIWithStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	seedDNSZoneAndRRSet(t, api)

	response := dnsRequest(api, http.MethodGet,
		"/dns/v1/projects/test/managedZones/example/rrsets/www.example.test./A?alt=json&prettyPrint=false", "")
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var got ResourceRecordSet
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "www.example.test." || got.Type != "A" || got.TTL != 300 ||
		!reflect.DeepEqual(got.Rrdatas, []string{"192.0.2.1"}) {
		t.Fatalf("rrset=%#v", got)
	}

	for _, test := range []struct {
		name   string
		method string
		path   string
		status int
	}{
		{"missing", http.MethodGet, "/dns/v1/projects/test/managedZones/example/rrsets/missing.example.test./A", http.StatusNotFound},
		{"extra segment", http.MethodGet, "/dns/v1/projects/test/managedZones/example/rrsets/www.example.test./A/extra", http.StatusNotFound},
		{"trailing slash", http.MethodGet, "/dns/v1/projects/test/managedZones/example/rrsets/www.example.test./A/", http.StatusNotFound},
		{"noncanonical prefix", http.MethodGet, "/v1/projects/test/managedZones/example/rrsets/www.example.test./A", http.StatusNotFound},
		{"unsupported verb", http.MethodPatch, "/dns/v1/projects/test/managedZones/example/rrsets/www.example.test./A", http.StatusMethodNotAllowed},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := dnsRequest(api, test.method, test.path, `{}`)
			if response.Code != test.status {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.status, response.Body.String())
			}
			if response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("content-type=%q", response.Header().Get("Content-Type"))
			}
			var envelope map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("error body is not JSON: %v body=%q", err, response.Body.String())
			}
		})
	}
}

func TestDNSNamesAreCanonicalAcrossRestartAndRoutes(t *testing.T) {
	store, err := state.New(t.TempDir(), "legacy-case")
	if err != nil {
		t.Fatal(err)
	}
	legacy := dnsMetadata{
		ZoneSeq: 1,
		Zones: map[string]persistedZone{
			"test:legacy": {
				Zone: &ManagedZone{Name: "legacy", DnsName: "Example.Test.", Visibility: "public"},
				RRSets: map[string]*ResourceRecordSet{
					"stale-key": {
						Name: "WWW.Example.Test.", Type: "a", TTL: 60, Rrdatas: []string{"192.0.2.20"},
					},
				},
			},
		},
	}
	if err := store.Save(dnsStateEntry, legacy); err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}

	response := dnsRequest(api, http.MethodGet,
		"/dns/v1/projects/test/managedZones/legacy/rrsets/WwW.Example.Test./a", "")
	if response.Code != http.StatusOK {
		t.Fatalf("case-variant get status=%d body=%s", response.Code, response.Body.String())
	}
	var rrset ResourceRecordSet
	if err := json.Unmarshal(response.Body.Bytes(), &rrset); err != nil {
		t.Fatal(err)
	}
	if rrset.Name != "www.example.test." || rrset.Type != "A" {
		t.Fatalf("canonical rrset=%#v", rrset)
	}
	response = dnsRequest(api, http.MethodGet,
		"/dns/v1/projects/test/managedZones/legacy/rrsets?name=WWW.EXAMPLE.TEST.&type=a", "")
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"name":"www.example.test."`)) {
		t.Fatalf("case-variant list status=%d body=%s", response.Code, response.Body.String())
	}

	response = dnsRequest(api, http.MethodPost,
		"/dns/v1/projects/test/managedZones/legacy/rrsets",
		`{"name":"API.Example.Test.","type":"a","ttl":60,"rrdatas":["192.0.2.21"]}`)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"name":"api.example.test."`)) {
		t.Fatalf("canonical create status=%d body=%s", response.Code, response.Body.String())
	}
	response = dnsRequest(api, http.MethodDelete,
		"/dns/v1/projects/test/managedZones/legacy/rrsets/ApI.Example.Test./a", "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("case-variant delete status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestLegacyCanonicalRRSetCollisionFailsWithoutSaving(t *testing.T) {
	store := &failingDNSStore{}
	legacy := dnsMetadata{
		ZoneSeq: 1,
		Zones: map[string]persistedZone{
			"test:legacy": {
				Zone: &ManagedZone{Name: "legacy", DnsName: "example.test.", Visibility: "public"},
				RRSets: map[string]*ResourceRecordSet{
					"legacy-one": {
						Name: "WWW.Example.Test.", Type: "a", TTL: 60, Rrdatas: []string{"192.0.2.40"},
					},
					"legacy-two": {
						Name: "www.example.test.", Type: "A", TTL: 60, Rrdatas: []string{"192.0.2.41"},
					},
				},
			},
		},
	}
	if err := store.Save(dnsStateEntry, legacy); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	beforeData := append([]byte(nil), store.data...)
	beforeSaves := store.saves
	store.mu.Unlock()

	_, err := NewAPIWithStore(store)
	if err == nil {
		t.Fatal("canonical RRSet collision was accepted")
	}
	message := err.Error()
	if !strings.Contains(message, `canonical resource record key "www.example.test.:A" is duplicated`) {
		t.Fatalf("error=%q", message)
	}
	for _, unrelated := range []string{"legacy-one", "legacy-two", "192.0.2.40", "192.0.2.41"} {
		if strings.Contains(message, unrelated) {
			t.Fatalf("error exposed unrelated data %q: %q", unrelated, message)
		}
	}
	store.mu.Lock()
	afterData := append([]byte(nil), store.data...)
	afterSaves := store.saves
	store.mu.Unlock()
	if afterSaves != beforeSaves || !bytes.Equal(afterData, beforeData) {
		t.Fatalf("rehydration modified state: saves %d->%d dataEqual=%t",
			beforeSaves, afterSaves, bytes.Equal(afterData, beforeData))
	}
}

func TestManagedZoneDNSNameAndVisibilityValidation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"malformed name", `{"name":"bad","dnsName":"bad..example.test.","visibility":"public"}`},
		{"unsupported visibility", `{"name":"bad","dnsName":"valid.example.test.","visibility":"internal"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api, err := NewAPIWithStore(nil)
			if err != nil {
				t.Fatal(err)
			}
			response := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones", test.body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if len(api.zones) != 0 || api.zoneSeq != 0 {
				t.Fatalf("invalid zone mutated state: zones=%d sequence=%d", len(api.zones), api.zoneSeq)
			}
		})
	}
	api, err := NewAPIWithStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	response := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones",
		`{"name":"canonical","dnsName":"MiXeD.Example.Test"}`)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"dnsName":"mixed.example.test."`)) {
		t.Fatalf("canonical zone status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestChangeAndPutCanonicalizeNamesWithinZone(t *testing.T) {
	api, err := NewAPIWithStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	seedDNSZone(t, api)
	put := dnsRequest(api, http.MethodPut,
		"/dns/v1/projects/test/managedZones/example/rrsets/put.example.test./A",
		`{"name":"PuT.Example.Test.","type":"a","ttl":60,"rrdatas":["192.0.2.30"]}`)
	if put.Code != http.StatusOK || !bytes.Contains(put.Body.Bytes(), []byte(`"name":"put.example.test."`)) {
		t.Fatalf("put status=%d body=%s", put.Code, put.Body.String())
	}
	change := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones/example/changes",
		`{"additions":[{"name":"ChAnGe.Example.Test.","type":"a","ttl":60,"rrdatas":["192.0.2.31"]}]}`)
	if change.Code != http.StatusOK || !bytes.Contains(change.Body.Bytes(), []byte(`"name":"change.example.test."`)) {
		t.Fatalf("change status=%d body=%s", change.Code, change.Body.String())
	}
	outside := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones/example/changes",
		`{"additions":[{"name":"outside.other.test.","type":"TXT","ttl":60,"rrdatas":["value"]}]}`)
	if outside.Code != http.StatusBadRequest {
		t.Fatalf("outside change status=%d body=%s", outside.Code, outside.Body.String())
	}
}

func TestCanonicalMutationRollbackRestoresCanonicalKeys(t *testing.T) {
	store := &failingDNSStore{}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	seedDNSZone(t, api)
	api.mu.RLock()
	before := snapshotDNSMetadata(api.zones, api.zoneSeq)
	api.mu.RUnlock()
	store.setFail(true)
	response := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones/example/rrsets",
		`{"name":"MiXeD.Example.Test.","type":"a","ttl":60,"rrdatas":["192.0.2.32"]}`)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	api.mu.RLock()
	after := snapshotDNSMetadata(api.zones, api.zoneSeq)
	api.mu.RUnlock()
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rollback mismatch:\nbefore=%#v\nafter=%#v", before, after)
	}
	for key, zone := range after.Zones {
		for storedKey, rrset := range zone.RRSets {
			if storedKey != rrKey(rrset.Name, rrset.Type) {
				t.Fatalf("zone=%s stale key=%q rrset=%#v", key, storedKey, rrset)
			}
		}
	}
}

func TestDuplicateManagedZoneIsProjectScopedAndStable(t *testing.T) {
	store, err := state.New(t.TempDir(), "duplicate-zone")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	first := dnsRequest(api, http.MethodPost, "/dns/v1/projects/one/managedZones",
		`{"name":"shared","dnsName":"first.example.test.","description":"original"}`)
	if first.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	var created ManagedZone
	if err := json.Unmarshal(first.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	duplicate := dnsRequest(api, http.MethodPost, "/dns/v1/projects/one/managedZones",
		`{"name":"shared","dnsName":"replacement.example.test.","description":"replacement"}`)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	api.mu.RLock()
	sequence := api.zoneSeq
	api.mu.RUnlock()
	if sequence != created.ID {
		t.Fatalf("zone sequence=%d want=%d", sequence, created.ID)
	}
	got := dnsRequest(api, http.MethodGet, "/dns/v1/projects/one/managedZones/shared", "")
	if got.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", got.Code, got.Body.String())
	}
	var existing ManagedZone
	if err := json.Unmarshal(got.Body.Bytes(), &existing); err != nil {
		t.Fatal(err)
	}
	if existing.ID != created.ID || existing.DnsName != "first.example.test." || existing.Description != "original" {
		t.Fatalf("existing=%#v created=%#v", existing, created)
	}

	other := dnsRequest(api, http.MethodPost, "/dns/v1/projects/two/managedZones",
		`{"name":"shared","dnsName":"other.example.test."}`)
	if other.Code != http.StatusOK {
		t.Fatalf("other project status=%d body=%s", other.Code, other.Body.String())
	}
	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range []string{"one", "two"} {
		response := dnsRequest(restarted, http.MethodGet, "/dns/v1/projects/"+project+"/managedZones/shared", "")
		if response.Code != http.StatusOK {
			t.Fatalf("restart project=%s status=%d body=%s", project, response.Code, response.Body.String())
		}
	}

	failingStore := &failingDNSStore{}
	withFailure, err := NewAPIWithStore(failingStore)
	if err != nil {
		t.Fatal(err)
	}
	seedDNSZone(t, withFailure)
	withFailure.mu.RLock()
	before := snapshotDNSMetadata(withFailure.zones, withFailure.zoneSeq)
	withFailure.mu.RUnlock()
	failingStore.setFail(true)
	response := dnsRequest(withFailure, http.MethodPost, "/dns/v1/projects/test/managedZones",
		`{"name":"example","dnsName":"replacement.example.test."}`)
	if response.Code != http.StatusConflict {
		t.Fatalf("duplicate with failing store status=%d body=%s", response.Code, response.Body.String())
	}
	withFailure.mu.RLock()
	after := snapshotDNSMetadata(withFailure.zones, withFailure.zoneSeq)
	withFailure.mu.RUnlock()
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("duplicate changed state with failing store:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestDNSMutationBodiesUseStrictOneMiBLimit(t *testing.T) {
	const limit = 1 << 20
	prefix := `{"name":"exact","dnsName":"exact.example.test.","description":"`
	suffix := `"}`
	exactBody := prefix + strings.Repeat("x", limit-len(prefix)-len(suffix)) + suffix
	if len(exactBody) != limit {
		t.Fatalf("exact body length=%d", len(exactBody))
	}

	tests := []struct {
		name   string
		body   string
		status int
	}{
		{"exact limit", exactBody, http.StatusOK},
		{"over limit", exactBody + " ", http.StatusBadRequest},
		{"unknown field", `{"name":"unknown","dnsName":"unknown.example.test.","unsupported":true}`, http.StatusBadRequest},
		{"trailing JSON", `{"name":"trailing","dnsName":"trailing.example.test."} {}`, http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api, err := NewAPIWithStore(nil)
			if err != nil {
				t.Fatal(err)
			}
			response := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones", test.body)
			if response.Code != test.status {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.status, response.Body.String())
			}
			api.mu.RLock()
			zoneCount, sequence := len(api.zones), api.zoneSeq
			api.mu.RUnlock()
			wantZones := 0
			var wantSequence uint64
			if test.status == http.StatusOK {
				wantZones, wantSequence = 1, 1
			}
			if zoneCount != wantZones || sequence != wantSequence {
				t.Fatalf("zones=%d sequence=%d want zones=%d sequence=%d",
					zoneCount, sequence, wantZones, wantSequence)
			}
		})
	}
}

func TestRRSetMutationValidation(t *testing.T) {
	tests := []struct {
		name string
		body any
	}{
		{"negative TTL", ResourceRecordSet{Name: "bad.example.test.", Type: "A", TTL: -1, Rrdatas: []string{"192.0.2.1"}}},
		{"zero TTL", ResourceRecordSet{Name: "bad.example.test.", Type: "A", TTL: 0, Rrdatas: []string{"192.0.2.1"}}},
		{"empty rrdatas", ResourceRecordSet{Name: "bad.example.test.", Type: "A", TTL: 60}},
		{"too many rrdatas", ResourceRecordSet{Name: "bad.example.test.", Type: "TXT", TTL: 60, Rrdatas: make([]string, 1001)}},
		{"invalid A", ResourceRecordSet{Name: "bad.example.test.", Type: "A", TTL: 60, Rrdatas: []string{"not-an-ip"}}},
		{"wrong A family", ResourceRecordSet{Name: "bad.example.test.", Type: "A", TTL: 60, Rrdatas: []string{"2001:db8::1"}}},
		{"invalid AAAA", ResourceRecordSet{Name: "bad.example.test.", Type: "AAAA", TTL: 60, Rrdatas: []string{"not-an-ip"}}},
		{"wrong AAAA family", ResourceRecordSet{Name: "bad.example.test.", Type: "AAAA", TTL: 60, Rrdatas: []string{"192.0.2.1"}}},
		{"mapped IPv6 A", ResourceRecordSet{Name: "bad.example.test.", Type: "A", TTL: 60, Rrdatas: []string{"::ffff:192.0.2.1"}}},
		{"relative CNAME", ResourceRecordSet{Name: "bad.example.test.", Type: "CNAME", TTL: 60, Rrdatas: []string{"target.example.test"}}},
		{"empty rrdata value", ResourceRecordSet{Name: "bad.example.test.", Type: "TXT", TTL: 60, Rrdatas: []string{""}}},
		{"malformed owner name", ResourceRecordSet{Name: "bad..example.test.", Type: "TXT", TTL: 60, Rrdatas: []string{"value"}}},
		{"owner outside zone", ResourceRecordSet{Name: "bad.other.test.", Type: "TXT", TTL: 60, Rrdatas: []string{"value"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api, err := NewAPIWithStore(nil)
			if err != nil {
				t.Fatal(err)
			}
			seedDNSZone(t, api)
			body, err := json.Marshal(test.body)
			if err != nil {
				t.Fatal(err)
			}
			response := dnsRequest(api, http.MethodPost,
				"/dns/v1/projects/test/managedZones/example/rrsets", string(body))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			api.mu.RLock()
			recordCount := len(api.zones[zoneKey("test", "example")].rrsets)
			api.mu.RUnlock()
			if recordCount != 2 {
				t.Fatalf("record count=%d want system records only", recordCount)
			}
		})
	}

	api, err := NewAPIWithStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	seedDNSZone(t, api)
	response := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones/example/rrsets",
		`{"name":"default.example.test.","type":"A","rrdatas":["192.0.2.1"]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("default TTL status=%d body=%s", response.Code, response.Body.String())
	}
	var rrset ResourceRecordSet
	if err := json.Unmarshal(response.Body.Bytes(), &rrset); err != nil {
		t.Fatal(err)
	}
	if rrset.TTL != 300 {
		t.Fatalf("default TTL=%d want=300", rrset.TTL)
	}
}

func TestChangeBatchIsLimitedToOneThousandRRSets(t *testing.T) {
	api, err := NewAPIWithStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	seedDNSZone(t, api)
	additions := make([]ResourceRecordSet, 1001)
	for i := range additions {
		additions[i] = ResourceRecordSet{
			Name:    "batch.example.test.",
			Type:    "TXT",
			TTL:     60,
			Rrdatas: []string{"value"},
		}
	}
	body, err := json.Marshal(Change{Additions: additions})
	if err != nil {
		t.Fatal(err)
	}
	response := dnsRequest(api, http.MethodPost,
		"/dns/v1/projects/test/managedZones/example/changes", string(body))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	api.mu.RLock()
	zone := api.zones[zoneKey("test", "example")]
	recordCount, changeCount, sequence := len(zone.rrsets), len(zone.changes), zone.changeSeq
	api.mu.RUnlock()
	if recordCount != 2 || changeCount != 0 || sequence != 0 {
		t.Fatalf("records=%d changes=%d sequence=%d", recordCount, changeCount, sequence)
	}
}

func TestDNSMetadataSurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "dns-profile")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}

	response := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones",
		`{"name":"example","dnsName":"example.com."}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create zone status = %d, body = %s", response.Code, response.Body.String())
	}
	response = dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones/example/rrsets",
		`{"name":"www.example.com.","type":"A","ttl":60,"rrdatas":["192.0.2.1"]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("create record status = %d, body = %s", response.Code, response.Body.String())
	}

	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	response = dnsRequest(restarted, http.MethodGet, "/dns/v1/projects/test/managedZones/example/rrsets", "")
	if response.Code != http.StatusOK {
		t.Fatalf("list after restart status = %d, body = %s", response.Code, response.Body.String())
	}
	if !bytes.Contains(response.Body.Bytes(), []byte("192.0.2.1")) {
		t.Fatalf("restarted records = %s", response.Body.String())
	}
}

func TestDNSMissingStateIsEmptyAndCorruptStateIsReported(t *testing.T) {
	store, err := state.New(t.TempDir(), "dns-profile")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatalf("missing state: %v", err)
	}
	if len(api.zones) != 0 {
		t.Fatalf("missing state loaded zones: %#v", api.zones)
	}

	if err := store.Save(dnsStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAPIWithStore(store); err == nil {
		t.Fatal("corrupt state was not reported")
	}
	var persisted string
	if err := store.Load(dnsStateEntry, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != "corrupt" {
		t.Fatalf("corrupt state was overwritten with %q", persisted)
	}
}

func TestCorruptStateDisablesDNSRuntime(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "corrupt-dns")
	store, err := state.New(root, "corrupt-dns")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(dnsStateEntry, "corrupt"); err != nil {
		t.Fatal(err)
	}
	api := NewAPI()
	response := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones",
		`{"name":"blocked","dnsName":"blocked.test."}`)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var persisted string
	if err := store.Load(dnsStateEntry, &persisted); err != nil || persisted != "corrupt" {
		t.Fatalf("corrupt state changed: %q err=%v", persisted, err)
	}
}

func TestDNSPersistenceDoesNotHoldAPILock(t *testing.T) {
	store := &checkingDNSStore{}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	store.api = api

	response := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones",
		`{"name":"example","dnsName":"example.com."}`)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if !store.saved {
		t.Fatal("mutation did not save state")
	}
}

func TestDNSMutationsRollbackOnSaveFailure(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *API)
		run   func(*API) *httptest.ResponseRecorder
	}{
		{
			name:  "create zone",
			setup: func(*testing.T, *API) {},
			run: func(api *API) *httptest.ResponseRecorder {
				return dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones",
					`{"name":"new-zone","dnsName":"new.test."}`)
			},
		},
		{
			name:  "patch zone",
			setup: seedDNSZone,
			run: func(api *API) *httptest.ResponseRecorder {
				return dnsRequest(api, http.MethodPatch, "/dns/v1/projects/test/managedZones/example",
					`{"description":"changed"}`)
			},
		},
		{
			name:  "delete zone",
			setup: seedDNSZone,
			run: func(api *API) *httptest.ResponseRecorder {
				return dnsRequest(api, http.MethodDelete, "/dns/v1/projects/test/managedZones/example", "")
			},
		},
		{
			name:  "create rrset",
			setup: seedDNSZone,
			run: func(api *API) *httptest.ResponseRecorder {
				return dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones/example/rrsets",
					`{"name":"www.example.test.","type":"A","rrdatas":["192.0.2.1"]}`)
			},
		},
		{
			name:  "delete rrset",
			setup: seedDNSZoneAndRRSet,
			run: func(api *API) *httptest.ResponseRecorder {
				return dnsRequest(api, http.MethodDelete,
					"/dns/v1/projects/test/managedZones/example/rrsets/www.example.test./A", "")
			},
		},
		{
			name:  "put rrset",
			setup: seedDNSZone,
			run: func(api *API) *httptest.ResponseRecorder {
				return dnsRequest(api, http.MethodPut,
					"/dns/v1/projects/test/managedZones/example/rrsets/www.example.test./A",
					`{"name":"www.example.test.","type":"A","ttl":60,"rrdatas":["192.0.2.2"]}`)
			},
		},
		{
			name:  "create change",
			setup: seedDNSZoneAndRRSet,
			run: func(api *API) *httptest.ResponseRecorder {
				return dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones/example/changes",
					`{"deletions":[{"name":"www.example.test.","type":"A","rrdatas":["192.0.2.1"]}],"additions":[{"name":"api.example.test.","type":"A","rrdatas":["192.0.2.2"]}]}`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &failingDNSStore{}
			api, err := NewAPIWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			test.setup(t, api)
			api.mu.RLock()
			before := snapshotDNSMetadata(api.zones, api.zoneSeq)
			api.mu.RUnlock()
			store.setFail(true)

			response := test.run(api)

			if response.Code != http.StatusInternalServerError {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			api.mu.RLock()
			after := snapshotDNSMetadata(api.zones, api.zoneSeq)
			api.mu.RUnlock()
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("mutation was not rolled back:\nbefore=%#v\nafter=%#v", before, after)
			}
		})
	}
}

func TestDNSConcurrentWritesCannotPersistStaleSnapshot(t *testing.T) {
	store := newBlockingDNSStore()
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	responses := make(chan *httptest.ResponseRecorder, 2)
	go func() {
		responses <- dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones",
			`{"name":"first","dnsName":"first.test."}`)
	}()
	select {
	case <-store.firstSave:
	case <-time.After(time.Second):
		t.Fatal("first save did not start")
	}
	go func() {
		responses <- dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones",
			`{"name":"second","dnsName":"second.test."}`)
	}()
	time.Sleep(10 * time.Millisecond)
	close(store.releaseFirst)
	for range 2 {
		if response := <-responses; response.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	}

	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted.zones) != 2 {
		t.Fatalf("persisted zones=%d, want 2", len(restarted.zones))
	}
}

type checkingDNSStore struct {
	api   *API
	saved bool
}

type failingDNSStore struct {
	mu    sync.Mutex
	data  []byte
	fail  bool
	saves int
}

func (s *failingDNSStore) setFail(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

func (s *failingDNSStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *failingDNSStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("injected save failure")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.data = data
	s.saves++
	return nil
}

type blockingDNSStore struct {
	mu           sync.Mutex
	data         []byte
	saves        int
	firstSave    chan struct{}
	releaseFirst chan struct{}
}

func newBlockingDNSStore() *blockingDNSStore {
	return &blockingDNSStore{firstSave: make(chan struct{}), releaseFirst: make(chan struct{})}
}

func (s *blockingDNSStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.data) == 0 {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.data, target)
}

func (s *blockingDNSStore) Save(_ string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.saves++
	first := s.saves == 1
	if first {
		close(s.firstSave)
	}
	s.mu.Unlock()
	if first {
		<-s.releaseFirst
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
}

func seedDNSZone(t *testing.T, api *API) {
	t.Helper()
	response := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones",
		`{"name":"example","dnsName":"example.test."}`)
	if response.Code != http.StatusOK {
		t.Fatalf("seed zone status=%d body=%s", response.Code, response.Body.String())
	}
}

func seedDNSZoneAndRRSet(t *testing.T, api *API) {
	t.Helper()
	seedDNSZone(t, api)
	response := dnsRequest(api, http.MethodPost, "/dns/v1/projects/test/managedZones/example/rrsets",
		`{"name":"www.example.test.","type":"A","rrdatas":["192.0.2.1"]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("seed rrset status=%d body=%s", response.Code, response.Body.String())
	}
}

func (s *checkingDNSStore) Load(string, any) error { return state.ErrNotFound }

func (s *checkingDNSStore) Save(string, any) error {
	if !s.api.mu.TryRLock() {
		return errors.New("DNS API lock held during save")
	}
	s.api.mu.RUnlock()
	s.saved = true
	return nil
}

func dnsRequest(api *API, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}
