package dns

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"minisky/pkg/state"
)

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
					`{"deletions":[{"name":"www.example.test.","type":"A"}],"additions":[{"name":"api.example.test.","type":"A","rrdatas":["192.0.2.2"]}]}`)
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
	mu   sync.Mutex
	data []byte
	fail bool
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
