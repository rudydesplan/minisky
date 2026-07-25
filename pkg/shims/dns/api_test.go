package dns

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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

type checkingDNSStore struct {
	api   *API
	saved bool
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
