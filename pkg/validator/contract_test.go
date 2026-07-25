package validator

import (
	"encoding/json"
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
