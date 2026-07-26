package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"google.golang.org/api/compute/v1"
)

func TestRunModesUseGeneratedComputeClient(t *testing.T) {
	const (
		project = "phase16-test-project"
		region  = "us-central1"
		cidr    = "172.29.16.0/24"
	)
	state := &fakeComputeState{project: project, region: region, cidr: cidr}
	server := httptest.NewServer(http.HandlerFunc(state.serveHTTP))
	defer server.Close()

	baselinePath := filepath.Join(t.TempDir(), "baseline.json")
	t.Setenv("MINISKY_ENDPOINT", server.URL)
	t.Setenv("MINISKY_PROJECT_ID", project)
	t.Setenv("MINISKY_REGION", region)
	t.Setenv("MINISKY_SUBNETWORK_CIDR", cidr)
	t.Setenv("MINISKY_PHASE16_SUBNETWORK_BASELINE", baselinePath)

	modes := []struct {
		name             string
		expectedRequests []string
	}{
		{
			name: "seed",
			expectedRequests: []string{
				"POST " + state.networkCollection(),
				"GET " + state.globalOperation("network-insert"),
				"GET " + state.networkItem(),
				"POST " + state.subnetworkCollection(),
				"GET " + state.regionalOperation("subnetwork-insert"),
				"GET " + state.subnetworkItem(),
				"GET " + state.subnetworkCollection() + "?maxResults=1",
			},
		},
		{
			name: "verify",
			expectedRequests: []string{
				"GET " + state.networkItem(),
				"GET " + state.subnetworkItem(),
				"GET " + state.subnetworkCollection() + "?maxResults=1",
			},
		},
		{
			name: "cleanup",
			expectedRequests: []string{
				"DELETE " + state.subnetworkItem(),
				"GET " + state.regionalOperation("subnetwork-delete"),
				"GET " + state.subnetworkItem(),
				"DELETE " + state.networkItem(),
				"GET " + state.globalOperation("network-delete"),
				"GET " + state.networkItem(),
			},
		},
		{
			name: "verify-cleanup",
			expectedRequests: []string{
				"GET " + state.subnetworkItem(),
				"GET " + state.networkItem(),
				"GET " + state.subnetworkCollection() + "?maxResults=1",
				"GET " + state.networkCollection(),
			},
		},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			state.resetRequests()
			t.Setenv("MINISKY_PHASE16_SUBNETWORK_MODE", mode.name)
			if err := run(); err != nil {
				t.Fatal(err)
			}
			if actual := state.requestsSnapshot(); !reflect.DeepEqual(actual, mode.expectedRequests) {
				t.Fatalf("requests:\n got: %q\nwant: %q", actual, mode.expectedRequests)
			}
		})
	}

	info, err := os.Stat(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("baseline permissions=%#o want=0600", info.Mode().Perm())
	}
}

type fakeComputeState struct {
	mu               sync.Mutex
	project          string
	region           string
	cidr             string
	networkExists    bool
	subnetworkExists bool
	requests         []string
}

func (state *fakeComputeState) serveHTTP(w http.ResponseWriter, request *http.Request) {
	state.mu.Lock()
	defer state.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if request.Header.Get("Authorization") != "" {
		http.Error(w, "unexpected authentication", http.StatusBadRequest)
		return
	}
	query := request.URL.Query()
	if query.Get("alt") != "json" || query.Get("prettyPrint") != "false" {
		http.Error(w, "missing generated-client query", http.StatusBadRequest)
		return
	}
	visibleQuery := url.Values{}
	if maxResults := query.Get("maxResults"); maxResults != "" {
		visibleQuery.Set("maxResults", maxResults)
	}
	record := request.Method + " " + request.URL.Path
	if encoded := visibleQuery.Encode(); encoded != "" {
		record += "?" + encoded
	}
	state.requests = append(state.requests, record)

	switch {
	case request.Method == http.MethodPost && request.URL.Path == state.networkCollection():
		state.assertBody(w, request, map[string]any{
			"name":                  networkName,
			"autoCreateSubnetworks": false,
		})
		state.networkExists = true
		writeJSON(w, operationJSON("network-insert", "PENDING"))
	case request.Method == http.MethodGet && request.URL.Path == state.globalOperation("network-insert"):
		writeJSON(w, operationJSON("network-insert", "DONE"))
	case request.Method == http.MethodGet && request.URL.Path == state.networkItem():
		if !state.networkExists {
			writeNotFound(w)
			return
		}
		writeJSON(w, state.networkJSON())
	case request.Method == http.MethodPost && request.URL.Path == state.subnetworkCollection():
		state.assertBody(w, request, map[string]any{
			"name":        subnetworkName,
			"ipCidrRange": state.cidr,
			"network":     expectedNetworkSelfLink(state.project),
		})
		if !state.networkExists {
			http.Error(w, "network missing", http.StatusBadRequest)
			return
		}
		state.subnetworkExists = true
		writeJSON(w, operationJSON("subnetwork-insert", "PENDING"))
	case request.Method == http.MethodGet && request.URL.Path == state.regionalOperation("subnetwork-insert"):
		writeJSON(w, operationJSON("subnetwork-insert", "DONE"))
	case request.Method == http.MethodGet && request.URL.Path == state.subnetworkItem():
		if !state.subnetworkExists {
			writeNotFound(w)
			return
		}
		writeJSON(w, state.subnetworkJSON())
	case request.Method == http.MethodGet && request.URL.Path == state.subnetworkCollection():
		if query.Get("maxResults") != "1" {
			http.Error(w, "maxResults must be 1", http.StatusBadRequest)
			return
		}
		items := []any{}
		if state.subnetworkExists {
			items = append(items, state.subnetworkJSON())
		}
		writeJSON(w, map[string]any{"kind": "compute#subnetworkList", "items": items})
	case request.Method == http.MethodDelete && request.URL.Path == state.subnetworkItem():
		state.subnetworkExists = false
		writeJSON(w, operationJSON("subnetwork-delete", "PENDING"))
	case request.Method == http.MethodGet && request.URL.Path == state.regionalOperation("subnetwork-delete"):
		writeJSON(w, operationJSON("subnetwork-delete", "DONE"))
	case request.Method == http.MethodDelete && request.URL.Path == state.networkItem():
		if state.subnetworkExists {
			http.Error(w, "subnetwork still exists", http.StatusBadRequest)
			return
		}
		state.networkExists = false
		writeJSON(w, operationJSON("network-delete", "PENDING"))
	case request.Method == http.MethodGet && request.URL.Path == state.globalOperation("network-delete"):
		writeJSON(w, operationJSON("network-delete", "DONE"))
	case request.Method == http.MethodGet && request.URL.Path == state.networkCollection():
		writeJSON(w, map[string]any{
			"kind": "compute#networkList",
			"items": []any{map[string]any{
				"kind": "compute#network", "id": "0", "name": "default",
			}},
		})
	default:
		http.Error(w, "unexpected request "+record, http.StatusNotFound)
	}
}

func (state *fakeComputeState) assertBody(
	w http.ResponseWriter,
	request *http.Request,
	expected map[string]any,
) {
	var actual map[string]any
	if err := json.NewDecoder(request.Body).Decode(&actual); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !reflect.DeepEqual(actual, expected) {
		http.Error(w, fmt.Sprintf("body=%#v want=%#v", actual, expected), http.StatusBadRequest)
	}
}

func (state *fakeComputeState) resetRequests() {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.requests = nil
}

func (state *fakeComputeState) requestsSnapshot() []string {
	state.mu.Lock()
	defer state.mu.Unlock()
	return append([]string(nil), state.requests...)
}

func (state *fakeComputeState) basePath() string {
	return "/_minisky/compute/compute/v1/projects/" + state.project
}

func (state *fakeComputeState) networkCollection() string {
	return state.basePath() + "/global/networks"
}

func (state *fakeComputeState) networkItem() string {
	return state.networkCollection() + "/" + networkName
}

func (state *fakeComputeState) subnetworkCollection() string {
	return state.basePath() + "/regions/" + state.region + "/subnetworks"
}

func (state *fakeComputeState) subnetworkItem() string {
	return state.subnetworkCollection() + "/" + subnetworkName
}

func (state *fakeComputeState) globalOperation(name string) string {
	return state.basePath() + "/global/operations/" + name
}

func (state *fakeComputeState) regionalOperation(name string) string {
	return state.basePath() + "/regions/" + state.region + "/operations/" + name
}

func (state *fakeComputeState) networkJSON() map[string]any {
	return map[string]any{
		"kind": "compute#network", "id": "1001", "name": networkName,
		"selfLink":              expectedNetworkSelfLink(state.project),
		"autoCreateSubnetworks": false,
		"creationTimestamp":     "2026-07-26T10:00:00Z",
	}
}

func (state *fakeComputeState) subnetworkJSON() map[string]any {
	regionLink := "https://www.googleapis.com/compute/v1/projects/" +
		state.project + "/regions/" + state.region
	return map[string]any{
		"kind": "compute#subnetwork", "id": "1", "name": subnetworkName,
		"ipCidrRange": state.cidr, "network": expectedNetworkSelfLink(state.project),
		"region": regionLink, "selfLink": regionLink + "/subnetworks/" + subnetworkName,
		"gatewayAddress": "172.29.16.1", "fingerprint": "stable-fingerprint",
		"privateIpGoogleAccess": false, "purpose": "PRIVATE", "stackType": "IPV4_ONLY",
		"state": "READY", "creationTimestamp": "2026-07-26T10:00:01Z",
	}
}

func operationJSON(name, status string) map[string]any {
	return map[string]any{
		"kind": "compute#operation", "id": "11", "name": name,
		"operationType": "test", "status": status,
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	_ = json.NewEncoder(w).Encode(value)
}

func writeNotFound(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	writeJSON(w, map[string]any{
		"error": map[string]any{"code": 404, "status": "NOT_FOUND", "message": "missing"},
	})
}

func TestOperationErrorsAreRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		writeJSON(w, map[string]any{
			"kind": "compute#operation", "id": "1", "name": "failed-operation", "status": "DONE",
			"error": map[string]any{"errors": []any{
				map[string]any{"code": "RESOURCE_ERROR", "message": "backend rejected request"},
			}},
		})
	}))
	defer server.Close()
	service, err := computeService(t.Context(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = waitGlobalOperation(t.Context(), service, "valid-project", operationForTest("failed-operation"))
	if err == nil || !strings.Contains(err.Error(), "RESOURCE_ERROR") {
		t.Fatalf("operation error=%v", err)
	}
}

func operationForTest(name string) *compute.Operation {
	return &compute.Operation{Name: name}
}

func TestValidateInputs(t *testing.T) {
	valid := inputs{
		mode: "seed", endpoint: "http://127.0.0.1:8080", project: "valid-project",
		region: "us-central1", cidr: "172.29.16.0/24",
		baselinePath: filepath.Join(t.TempDir(), "baseline.json"),
	}
	if err := validateInputs(valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*inputs)
	}{
		{"non-loopback endpoint", func(cfg *inputs) { cfg.endpoint = "https://example.com" }},
		{"endpoint path", func(cfg *inputs) { cfg.endpoint += "/api" }},
		{"project", func(cfg *inputs) { cfg.project = "Bad_Project" }},
		{"region", func(cfg *inputs) { cfg.region = "US Central" }},
		{"noncanonical CIDR", func(cfg *inputs) { cfg.cidr = "172.29.16.1/24" }},
		{"CIDR size", func(cfg *inputs) { cfg.cidr = "172.0.0.0/7" }},
		{"broader link-local overlap", func(cfg *inputs) { cfg.cidr = "169.0.0.0/8" }},
		{"link-local", func(cfg *inputs) { cfg.cidr = "169.254.0.0/16" }},
		{"class E", func(cfg *inputs) { cfg.cidr = "240.0.0.0/4" }},
		{"broadcast-containing", func(cfg *inputs) { cfg.cidr = "255.255.255.248/29" }},
		{"relative baseline", func(cfg *inputs) { cfg.baselinePath = "baseline.json" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			test.mutate(&cfg)
			if err := validateInputs(cfg); err == nil {
				t.Fatal("invalid inputs accepted")
			}
		})
	}
}
