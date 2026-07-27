package cloudprofiler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestAPI() *API {
	return &API{profiles: make(map[string]*Profile)}
}

func TestCreateProfile(t *testing.T) {
	api := newTestAPI()
	body := `{"profileType":["CPU","HEAP"],"deployment":{"projectId":"p1","target":"my-service","labels":{"zone":"us-central1-a"}}}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/p1/profiles", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-MiniSky-Simulated") != "true" {
		t.Error("missing X-MiniSky-Simulated header")
	}

	var profile Profile
	if err := json.NewDecoder(rec.Body).Decode(&profile); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if profile.Name == "" {
		t.Fatal("name is empty")
	}
	if !strings.HasPrefix(profile.Name, "projects/p1/profiles/") {
		t.Fatalf("name = %q, want projects/p1/profiles/* prefix", profile.Name)
	}
	// Server assigns exactly ONE type from the requested list
	if profile.ProfileType != "CPU" {
		t.Fatalf("profileType = %q, want CPU (first from requested list)", profile.ProfileType)
	}
	if profile.Duration != "10s" {
		t.Fatalf("duration = %q, want 10s", profile.Duration)
	}
	if profile.Deployment == nil || profile.Deployment.ProjectID != "p1" {
		t.Fatal("deployment not preserved")
	}
	if profile.CreateTime == "" {
		t.Fatal("createTime is empty")
	}
}

func TestCreateProfileMissingType(t *testing.T) {
	api := newTestAPI()
	body := `{"deployment":{"projectId":"p1","target":"svc"}}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/p1/profiles", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "profileType") {
		t.Error("error should mention profileType")
	}
}

func TestCreateOfflineProfile(t *testing.T) {
	api := newTestAPI()
	body := `{"profileType":"HEAP","deployment":{"projectId":"p1","target":"svc"},"profileBytes":"cHJvZmlsZQ==","duration":"30s"}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/p1/profiles:createOffline", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	var profile Profile
	json.NewDecoder(rec.Body).Decode(&profile)
	if profile.ProfileType != "HEAP" {
		t.Fatalf("profileType = %q, want HEAP", profile.ProfileType)
	}
	if profile.ProfileBytes != "cHJvZmlsZQ==" {
		t.Fatalf("profileBytes not preserved")
	}
	if profile.Name == "" {
		t.Fatal("name is empty")
	}
}

func TestUpdateProfile(t *testing.T) {
	api := newTestAPI()

	// Create a profile first
	createBody := `{"profileType":["WALL"],"deployment":{"projectId":"p1","target":"svc"}}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/p1/profiles", strings.NewReader(createBody))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create status = %d", rec.Code)
	}
	var created Profile
	json.NewDecoder(rec.Body).Decode(&created)

	// Extract profile ID from name
	parts := strings.Split(created.Name, "/")
	profileID := parts[len(parts)-1]

	// PATCH to upload profile bytes
	patchBody := `{"profileBytes":"dGVzdC1wcm9maWxlLWRhdGE="}`
	req = httptest.NewRequest(http.MethodPatch, "/v2/projects/p1/profiles/"+profileID, strings.NewReader(patchBody))
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("patch status = %d; body = %s", rec.Code, rec.Body.String())
	}

	var updated Profile
	json.NewDecoder(rec.Body).Decode(&updated)
	if updated.ProfileBytes != "dGVzdC1wcm9maWxlLWRhdGE=" {
		t.Fatalf("profileBytes = %q, want uploaded data", updated.ProfileBytes)
	}
	if updated.ProfileType != "WALL" {
		t.Fatalf("profileType = %q, want WALL (unchanged)", updated.ProfileType)
	}
}

func TestUpdateProfileNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{"profileBytes":"dGVzdA=="}`
	req := httptest.NewRequest(http.MethodPatch, "/v2/projects/p1/profiles/nonexistent", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestUnsupportedMethod(t *testing.T) {
	api := newTestAPI()

	tests := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v2/projects/p1/profiles"},
		{http.MethodGet, "/v2/projects/p1/profiles/some-id"},
		{http.MethodDelete, "/v2/projects/p1/profiles/some-id"},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			api.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, want 405; body = %s", rec.Code, rec.Body.String())
			}
		})
	}
}
