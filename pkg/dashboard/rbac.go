package dashboard

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"

	localsecurity "minisky/pkg/security"
)

const dashboardOAuthScope = "https://www.googleapis.com/auth/cloud-platform"

var dashboardProjectPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
var dashboardProjectParentPattern = regexp.MustCompile(`^(folders|organizations)/[^/]+$`)

type dashboardAuthorizer interface {
	EnforcementEnabled() bool
	Authorize(resource, principal, permission string) bool
	VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error)
}

func withDashboardRBAC(next http.Handler, authorizer dashboardAuthorizer, audience string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authorizer == nil || !authorizer.EnforcementEnabled() {
			next.ServeHTTP(w, r)
			return
		}
		r.Header.Del("X-MiniSky-Principal")
		scheme, token, ok := strings.Cut(strings.TrimSpace(r.Header.Get("Authorization")), " ")
		if websocketToken := dashboardWebSocketToken(r); websocketToken != "" {
			scheme, token, ok = "Bearer", websocketToken, true
		}
		if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
			writeDashboardAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Bearer authentication is required")
			return
		}
		claims, err := authorizer.VerifyLocalToken(strings.TrimSpace(token), audience, dashboardOAuthScope)
		if err != nil {
			writeDashboardAuthError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Bearer credential is invalid or expired")
			return
		}
		r.Header.Set("X-MiniSky-Principal", claims.Subject)
		permission := "minisky.dashboard.manage"
		resource := ""
		if r.Method == http.MethodPost && r.URL.Path == "/api/projects" {
			var err error
			resource, err = dashboardProjectCreateParent(r)
			if err != nil {
				writeDashboardAuthError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			}
			permission = "resourcemanager.projects.create"
		} else {
			project, err := dashboardProject(r)
			if err != nil {
				writeDashboardAuthError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
				return
			}
			resource = "projects/" + project
		}
		if r.URL.Path == "/api/manage/compute/terminal" {
			permission = "minisky.dashboard.terminal"
		} else if r.Method == http.MethodGet || r.Method == http.MethodHead {
			permission = "minisky.dashboard.view"
		}
		if !authorizer.Authorize(resource, claims.Subject, permission) {
			writeDashboardAuthError(w, http.StatusForbidden, "PERMISSION_DENIED", "Caller lacks permission "+permission)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func dashboardProjectCreateParent(r *http.Request) (string, error) {
	if r.Body == nil {
		return "", &dashboardProjectError{"project creation requires a JSON body"}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil {
		return "", &dashboardProjectError{"unable to inspect dashboard project creation"}
	}
	if len(body) > 1<<20 {
		return "", &dashboardProjectError{"dashboard JSON request exceeds the 1 MiB authorization limit"}
	}
	var input struct {
		ProjectID string `json:"projectId"`
		Parent    string `json:"parent"`
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal(body, &input) != nil || json.Unmarshal(body, &payload) != nil ||
		!dashboardProjectPattern.MatchString(strings.TrimSpace(input.ProjectID)) {
		return "", &dashboardProjectError{"project creation requires a valid projectId"}
	}
	parent := strings.TrimSpace(input.Parent)
	if parent == "" {
		parent = "organizations/100000000000"
	}
	if !dashboardProjectParentPattern.MatchString(parent) {
		return "", &dashboardProjectError{"project creation requires a valid folders/{id} or organizations/{id} parent"}
	}
	payload["parent"], _ = json.Marshal(parent)
	normalized, err := json.Marshal(payload)
	if err != nil {
		return "", &dashboardProjectError{"unable to normalize dashboard project creation"}
	}
	r.Body = io.NopCloser(bytes.NewReader(normalized))
	r.ContentLength = int64(len(normalized))
	return parent, nil
}

func dashboardWebSocketToken(r *http.Request) string {
	if r.URL.Path != "/api/manage/compute/terminal" {
		return ""
	}
	protocols := strings.Split(r.Header.Get("Sec-WebSocket-Protocol"), ",")
	if len(protocols) < 2 || strings.TrimSpace(protocols[0]) != "minisky-auth" {
		return ""
	}
	return strings.TrimSpace(protocols[1])
}

func dashboardProject(r *http.Request) (string, error) {
	canonical := make(map[string]struct{})
	add := func(project string) error {
		project = strings.TrimSpace(project)
		if project == "" {
			return nil
		}
		if !dashboardProjectPattern.MatchString(project) {
			return &dashboardProjectError{"invalid dashboard project"}
		}
		canonical[project] = struct{}{}
		return nil
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	for index, part := range parts {
		if part == "projects" && index+1 < len(parts) {
			if err := add(strings.TrimSuffix(parts[index+1], ":")); err != nil {
				return "", err
			}
		}
	}
	if err := add(r.Header.Get("X-MiniSky-Project")); err != nil {
		return "", err
	}
	if err := add(dashboardWebSocketProject(r)); err != nil {
		return "", err
	}
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/json") && r.Body != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
		if err != nil {
			return "", &dashboardProjectError{"unable to inspect dashboard request project"}
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if len(body) > 1<<20 {
			return "", &dashboardProjectError{"dashboard JSON request exceeds the 1 MiB authorization limit"}
		}
		if len(bytes.TrimSpace(body)) > 0 {
			var value any
			if json.Unmarshal(body, &value) == nil {
				for _, project := range dashboardProjectsFromJSON(value) {
					if err := add(project); err != nil {
						return "", err
					}
				}
			}
		}
	}
	if len(canonical) > 1 {
		return "", &dashboardProjectError{"dashboard request contains conflicting project targets"}
	}
	queryProject := strings.TrimSpace(r.URL.Query().Get("project"))
	if queryProject == "" {
		queryProject = strings.TrimSpace(r.URL.Query().Get("projectId"))
	}
	var project string
	for candidate := range canonical {
		project = candidate
	}
	if queryProject != "" && (project == "" || queryProject != project) {
		return "", &dashboardProjectError{"dashboard project query does not match the canonical target"}
	}
	if project == "" {
		return "", &dashboardProjectError{"dashboard request requires an explicit canonical project target"}
	}
	return project, nil
}

func dashboardWebSocketProject(r *http.Request) string {
	if r.URL.Path != "/api/manage/compute/terminal" {
		return ""
	}
	for _, protocol := range strings.Split(r.Header.Get("Sec-WebSocket-Protocol"), ",") {
		if project, ok := strings.CutPrefix(strings.TrimSpace(protocol), "minisky-project."); ok {
			return project
		}
	}
	return ""
}

func dashboardProjectsFromJSON(value any) []string {
	var projects []string
	var visit func(any)
	visit = func(current any) {
		switch typed := current.(type) {
		case map[string]any:
			for key, nested := range typed {
				switch key {
				case "project", "projectId", "project_id":
					if project, ok := nested.(string); ok {
						projects = append(projects, project)
					}
				case "name", "parent", "logName":
					if resource, ok := nested.(string); ok {
						parts := strings.Split(strings.Trim(resource, "/"), "/")
						for index, part := range parts {
							if part == "projects" && index+1 < len(parts) {
								projects = append(projects, strings.TrimSuffix(parts[index+1], ":"))
							}
						}
					}
				}
				visit(nested)
			}
		case []any:
			for _, nested := range typed {
				visit(nested)
			}
		}
	}
	visit(value)
	return projects
}

type dashboardProjectError struct{ message string }

func (err *dashboardProjectError) Error() string { return err.message }

func writeDashboardAuthError(w http.ResponseWriter, code int, status, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "status": status, "message": message,
	}})
}
