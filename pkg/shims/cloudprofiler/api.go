// Package cloudprofiler implements the Cloud Profiler API v2 shim.
//
// Real API methods (ONLY these — no standard CRUD):
//   - POST /v2/projects/{project}/profiles — CreateProfile
//   - POST /v2/projects/{project}/profiles:createOffline — CreateOfflineProfile
//   - PATCH /v2/projects/{project}/profiles/{profileId} — UpdateProfile
//
// There is NO GET single profile, NO DELETE, NO LIST in the real API.
// CreateProfile is a long-poll in production; MiniSky returns immediately.
// Profiles are ephemeral — no persistence needed for a local emulator.
package cloudprofiler

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"minisky/pkg/registry"
)

func init() {
	registry.Register("cloudprofiler.googleapis.com", func(_ *registry.Context) http.Handler {
		return NewAPI()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resources
// ─────────────────────────────────────────────────────────────────────────────

// Deployment identifies the deployment associated with a profile.
type Deployment struct {
	ProjectID string            `json:"projectId,omitempty"`
	Target    string            `json:"target,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// Profile represents a Cloud Profiler profile resource.
type Profile struct {
	Name         string            `json:"name"`
	ProfileType  string            `json:"profileType,omitempty"`
	Deployment   *Deployment       `json:"deployment,omitempty"`
	Duration     string            `json:"duration,omitempty"`
	ProfileBytes string            `json:"profileBytes,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	CreateTime   string            `json:"createTime,omitempty"`
}

// createProfileRequest is the body for CreateProfile.
type createProfileRequest struct {
	Deployment  *Deployment `json:"deployment,omitempty"`
	ProfileType []string    `json:"profileType"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API
// ─────────────────────────────────────────────────────────────────────────────

// API is the stateless Cloud Profiler v2 shim.
type API struct {
	mu       sync.Mutex
	profiles map[string]*Profile // key: "projects/{p}/profiles/{id}"
	seq      int
}

// NewAPI creates a new Cloud Profiler API handler.
func NewAPI() *API {
	return &API{
		profiles: make(map[string]*Profile),
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: Cloud Profiler] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path
	// Parse: /v2/projects/{project}/profiles[/{profileId}][:createOffline]
	parts := strings.Split(strings.Trim(path, "/"), "/")

	project := segmentAfter(parts, "projects")
	profileID := segmentAfter(parts, "profiles")

	switch {
	// POST /v2/projects/{project}/profiles:createOffline
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/profiles:createOffline"):
		api.createOfflineProfile(w, r, project)

	// POST /v2/projects/{project}/profiles — CreateProfile
	case r.Method == http.MethodPost && profileID == "":
		api.createProfile(w, r, project)

	// PATCH /v2/projects/{project}/profiles/{profileId} — UpdateProfile
	case r.Method == http.MethodPatch && profileID != "":
		api.updateProfile(w, r, project, profileID)

	// GET or DELETE — not in the real API → 405
	case (r.Method == http.MethodGet || r.Method == http.MethodDelete):
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("method %s is not supported by Cloud Profiler API", r.Method))

	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found: "+path)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CreateProfile — POST /v2/projects/{project}/profiles
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createProfile(w http.ResponseWriter, r *http.Request, project string) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if project == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project is required")
		return
	}
	var req createProfileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	if len(req.ProfileType) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "profileType is required and must contain at least one type")
		return
	}
	if req.Deployment == nil || req.Deployment.ProjectID != project || req.Deployment.Target == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "deployment.projectId must match the request project and deployment.target is required")
		return
	}
	for _, pt := range req.ProfileType {
		if !validProfileType(pt) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", fmt.Sprintf("invalid profile type: %q", pt))
			return
		}
	}

	api.mu.Lock()
	api.seq++
	id := fmt.Sprintf("profile-%d", api.seq)
	name := fmt.Sprintf("projects/%s/profiles/%s", project, id)
	// Server assigns exactly ONE type from the requested list.
	profile := &Profile{
		Name:        name,
		ProfileType: req.ProfileType[0],
		Deployment:  req.Deployment,
		Duration:    "10s",
		Labels:      map[string]string{},
		CreateTime:  time.Now().UTC().Format(time.RFC3339),
	}
	api.profiles[name] = profile
	api.mu.Unlock()

	w.Header().Set("X-MiniSky-Simulated", "true")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(profile)
}

// ─────────────────────────────────────────────────────────────────────────────
// CreateOfflineProfile — POST /v2/projects/{project}/profiles:createOffline
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) createOfflineProfile(w http.ResponseWriter, r *http.Request, project string) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	if project == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "project is required")
		return
	}
	var body Profile
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	if body.ProfileType == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "profileType is required for offline profile")
		return
	}
	if !validProfileType(body.ProfileType) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", fmt.Sprintf("invalid profile type: %q", body.ProfileType))
		return
	}
	if body.Deployment == nil || body.Deployment.ProjectID != project || body.Deployment.Target == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "deployment.projectId must match the request project and deployment.target is required")
		return
	}
	profileBytes, err := base64.StdEncoding.DecodeString(body.ProfileBytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "profileBytes must be valid base64")
		return
	}
	if len(profileBytes) > 1<<20 {
		writeError(w, http.StatusRequestEntityTooLarge, "RESOURCE_EXHAUSTED", "decoded profileBytes exceeds 1 MiB")
		return
	}

	api.mu.Lock()
	api.seq++
	id := fmt.Sprintf("profile-%d", api.seq)
	name := fmt.Sprintf("projects/%s/profiles/%s", project, id)
	profile := &Profile{
		Name:         name,
		ProfileType:  body.ProfileType,
		Deployment:   body.Deployment,
		Duration:     body.Duration,
		ProfileBytes: body.ProfileBytes,
		Labels:       body.Labels,
		CreateTime:   time.Now().UTC().Format(time.RFC3339),
	}
	if profile.Labels == nil {
		profile.Labels = map[string]string{}
	}
	api.profiles[name] = profile
	api.mu.Unlock()

	w.Header().Set("X-MiniSky-Simulated", "true")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(profile)
}

// ─────────────────────────────────────────────────────────────────────────────
// UpdateProfile — PATCH /v2/projects/{project}/profiles/{profileId}
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) updateProfile(w http.ResponseWriter, r *http.Request, project, profileID string) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	name := fmt.Sprintf("projects/%s/profiles/%s", project, profileID)

	api.mu.Lock()
	defer api.mu.Unlock()

	profile, ok := api.profiles[name]
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("profile %q not found", name))
		return
	}

	var patch Profile
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	if patch.ProfileBytes != "" {
		profileBytes, err := base64.StdEncoding.DecodeString(patch.ProfileBytes)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "profileBytes must be valid base64")
			return
		}
		if len(profileBytes) > 1<<20 {
			writeError(w, http.StatusRequestEntityTooLarge, "RESOURCE_EXHAUSTED", "decoded profileBytes exceeds 1 MiB")
			return
		}
	}

	if patch.ProfileBytes != "" {
		profile.ProfileBytes = patch.ProfileBytes
	}
	if patch.Labels != nil {
		profile.Labels = patch.Labels
	}

	w.Header().Set("X-MiniSky-Simulated", "true")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(profile)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func segmentAfter(parts []string, key string) string {
	for i, p := range parts {
		if p == key && i+1 < len(parts) {
			// Don't return segments that contain ":" (custom method suffix)
			seg := parts[i+1]
			if idx := strings.Index(seg, ":"); idx >= 0 {
				return seg[:idx]
			}
			return seg
		}
	}
	return ""
}

func validProfileType(pt string) bool {
	switch pt {
	case "CPU", "HEAP", "THREADS", "WALL", "CONTENTION":
		return true
	}
	return false
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
			"status":  status,
			"details": []any{},
		},
	})
}
