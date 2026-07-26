package validator

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGlobMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		glob string
		path string
		want bool
	}{
		{name: "exact", glob: "/v1/projects", path: "/v1/projects", want: true},
		{name: "single segment wildcard", glob: "/v1/projects/*/instances", path: "/v1/projects/demo/instances", want: true},
		{name: "method suffix wildcard", glob: "/v1/models/*:predict", path: "/v1/models/demo:predict", want: true},
		{name: "method suffix required", glob: "/v1/models/*:predict", path: "/v1/models/demo", want: false},
		{name: "wildcard cannot be empty", glob: "/v1/projects/*/instances", path: "/v1/projects//instances", want: false},
		{name: "wildcard cannot cross slash", glob: "/v1/projects/*/instances", path: "/v1/projects/demo/zones/us/instances", want: false},
		{name: "different literal", glob: "/v1/projects/*/instances", path: "/v1/folders/demo/instances", want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := globMatch(tt.glob, tt.path); got != tt.want {
				t.Fatalf("globMatch(%q, %q) = %v, want %v", tt.glob, tt.path, got, tt.want)
			}
		})
	}
}

func TestCheckFieldTypeRejectsFractionalInteger(t *testing.T) {
	t.Parallel()

	if got := checkFieldType("replicas", 1.5, "integer"); got == "" {
		t.Fatal("checkFieldType accepted a fractional number as an integer")
	}
	if got := checkFieldType("replicas", 2.0, "integer"); got != "" {
		t.Fatalf("checkFieldType rejected a whole JSON number: %s", got)
	}
}

func TestValidateRequestRestoresBody(t *testing.T) {
	t.Parallel()

	body := `{"name":"vm-1"}`
	req := httptest.NewRequest(http.MethodPost, "http://compute.googleapis.com/compute/v1/projects/demo/zones/us-central1-a/instances", strings.NewReader(body))
	req.Host = "compute.googleapis.com"
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	rec := httptest.NewRecorder()

	if ok := NewValidator().ValidateRequest(rec, req); !ok {
		t.Fatalf("valid request was rejected: %s", rec.Body.String())
	}

	decoded := map[string]any{}
	if err := json.NewDecoder(req.Body).Decode(&decoded); err != nil {
		t.Fatalf("downstream body was not restored: %v", err)
	}
	if decoded["name"] != "vm-1" {
		t.Fatalf("restored body name = %v, want vm-1", decoded["name"])
	}
}

func TestValidateRequestEmitsGCPError(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodPost, "http://compute.googleapis.com/compute/v1/projects/demo/zones/us-central1-a/instances", strings.NewReader(`{}`))
	req.Host = "compute.googleapis.com"
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if ok := NewValidator().ValidateRequest(rec, req); ok {
		t.Fatal("invalid request unexpectedly passed validation")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	var response struct {
		Error struct {
			Code   int    `json:"code"`
			Status string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if response.Error.Code != http.StatusBadRequest || response.Error.Status != "INVALID_ARGUMENT" {
		t.Fatalf("unexpected error response: %+v", response.Error)
	}
}

func TestValidateRequestRejectsBoundedSubnetworkBodyBeforeAllocation(t *testing.T) {
	body := `{"name":"large","ipCidrRange":"10.0.0.0/24","network":"custom","description":"` +
		strings.Repeat("x", (1<<20)+1) + `"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"http://compute.googleapis.com/compute/v1/projects/demo/regions/us-central1/subnetworks",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if ok := NewValidator().ValidateRequestForDomain(rec, req, "compute.googleapis.com"); ok {
		t.Fatal("oversized bounded request passed validation")
	}
	if rec.Code != http.StatusRequestEntityTooLarge ||
		!strings.Contains(rec.Body.String(), `"INVALID_ARGUMENT"`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	remaining, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) == len(body) {
		t.Fatal("oversized request body was restored")
	}
}

func TestEveryJSONMutationRuleHasBodyLimit(t *testing.T) {
	t.Parallel()

	validator := NewValidator()
	for _, service := range embeddedRules {
		for _, rule := range service.Methods {
			if rule.ContentType != "application/json" || !isMutationMethod(rule.HTTPMethod) {
				continue
			}
			if got := validator.RequestBodyLimit(service.Domain, rule.HTTPMethod, rule.PathGlob); got <= 0 {
				t.Errorf("%s %s %s body limit = %d, want nonzero", service.Domain, rule.HTTPMethod, rule.PathGlob, got)
			}
		}
	}
}

func TestValidateRequestAppliesInheritedBodyLimit(t *testing.T) {
	body := `{"name":"network","description":"` + strings.Repeat("x", (1<<20)+1) + `"}`
	req := httptest.NewRequest(
		http.MethodPost,
		"http://compute.googleapis.com/compute/v1/projects/demo/global/networks",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if ok := NewValidator().ValidateRequestForDomain(rec, req, "compute.googleapis.com"); ok {
		t.Fatal("oversized request passed inherited body limit")
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d want=%d body=%s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
	}
}

func TestValidateRequestBoundsDecodedBase64Field(t *testing.T) {
	t.Parallel()

	payload := base64.StdEncoding.EncodeToString(bytesOfSize((64 << 10) + 1))
	body := `{"payload":{"data":"` + payload + `"}}`
	req := httptest.NewRequest(
		http.MethodPost,
		"http://secretmanager.googleapis.com/v1/projects/demo/secrets/test:addVersion",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if ok := NewValidator().ValidateRequestForDomain(rec, req, "secretmanager.googleapis.com"); ok {
		t.Fatal("oversized decoded payload passed validation")
	}
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "decoded") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestValidateRequestBoundsCollectionItems(t *testing.T) {
	t.Parallel()

	entries := strings.Repeat(`{"logName":"projects/demo/logs/app"},`, 1_001)
	body := `{"entries":[` + strings.TrimSuffix(entries, ",") + `]}`
	req := httptest.NewRequest(http.MethodPost, "http://logging.googleapis.com/v2/entries:write", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if ok := NewValidator().ValidateRequestForDomain(rec, req, "logging.googleapis.com"); ok {
		t.Fatal("oversized collection passed validation")
	}
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "at most 1000") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRequestBodyLimitRouteOverrides(t *testing.T) {
	t.Parallel()

	validator := NewValidator()
	for _, test := range []struct {
		name, domain, method, path string
		want                       int64
	}{
		{
			name: "inherited JSON mutation", domain: "compute.googleapis.com", method: http.MethodPost,
			path: "/compute/v1/projects/demo/global/networks", want: DefaultMaxBodyBytes,
		},
		{
			name: "BigQuery streaming insert", domain: "bigquery.googleapis.com", method: http.MethodPost,
			path: "/bigquery/v2/projects/demo/datasets/data/tables/events/insertAll", want: 10 << 20,
		},
		{
			name: "BigQuery media upload", domain: "bigquery.googleapis.com", method: http.MethodPost,
			path: "/upload/bigquery/v2/projects/demo/jobs", want: 50 << 20,
		},
		{
			name: "PubSub publish", domain: "pubsub.googleapis.com", method: http.MethodPost,
			path: "/v1/projects/demo/topics/events:publish", want: 10 << 20,
		},
		{
			name: "Storage resumable chunk", domain: "storage.googleapis.com", method: http.MethodPut,
			path: "/upload/storage/v1/b/photos/o", want: 64 << 20,
		},
		{
			name: "unmatched gateway fallback", domain: "custom.googleapis.com", method: http.MethodPost,
			path: "/v1/upload", want: DefaultMaxBodyBytes,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := validator.RequestBodyLimit(test.domain, test.method, test.path); got != test.want {
				t.Fatalf("limit=%d want=%d", got, test.want)
			}
		})
	}
}

func bytesOfSize(size int) []byte {
	return []byte(strings.Repeat("x", size))
}

func isMutationMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	default:
		return true
	}
}
