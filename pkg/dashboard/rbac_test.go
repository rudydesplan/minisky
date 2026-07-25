package dashboard

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	localsecurity "minisky/pkg/security"
	"minisky/pkg/shims/resourcemanager"
	"minisky/pkg/state"
)

type dashboardTestAuthorizer struct {
	issuer     *localsecurity.Issuer
	permission string
	resource   string
	allow      bool
}

func (a *dashboardTestAuthorizer) EnforcementEnabled() bool { return true }
func (a *dashboardTestAuthorizer) VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error) {
	return a.issuer.Verify(token, localsecurity.VerifyOptions{Audience: audience, RequiredScope: scope})
}
func (a *dashboardTestAuthorizer) Authorize(resource, _ string, permission string) bool {
	a.resource = resource
	a.permission = permission
	return a.allow
}

func TestDashboardRBACUsesTokenPrincipalProjectAndRolePermission(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	authorizer := &dashboardTestAuthorizer{issuer: issuer, allow: true}
	handler := withDashboardRBAC(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-MiniSky-Principal") != "user:alice@example.com" {
			t.Fatalf("principal = %q", r.Header.Get("X-MiniSky-Principal"))
		}
		w.WriteHeader(http.StatusNoContent)
	}), authorizer, "dashboard")

	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "user:alice@example.com", Audience: "dashboard",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://localhost/api/settings?project=team-project", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-MiniSky-Project", "team-project")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if authorizer.resource != "projects/team-project" || authorizer.permission != "minisky.dashboard.manage" {
		t.Fatalf("resource=%q permission=%q", authorizer.resource, authorizer.permission)
	}
}

func TestDashboardRBACDefaultPermissiveAndStrictDenial(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	response := httptest.NewRecorder()
	withDashboardRBAC(next, nil, "").ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://localhost/api/services", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("permissive status = %d", response.Code)
	}

	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	response = httptest.NewRecorder()
	withDashboardRBAC(next, &dashboardTestAuthorizer{issuer: issuer}, "dashboard").ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "http://localhost/api/services", nil),
	)
	if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), "UNAUTHENTICATED") {
		t.Fatalf("strict status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestDashboardRBACUsesCanonicalPathAndRejectsQueryMismatch(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	authorizer := &dashboardTestAuthorizer{issuer: issuer, allow: true}
	handler := withDashboardRBAC(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), authorizer, "dashboard")
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "user:alice@example.com", Audience: "dashboard",
		Scopes: []string{dashboardOAuthScope}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(
		http.MethodPost,
		"http://localhost/api/manage/compute/projects/path-project/zones/us/instances?project=query-project",
		bytes.NewBufferString(`{"projectId":"body-project"}`),
	)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "project") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestDashboardTerminalRequiresDedicatedPermission(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	authorizer := &dashboardTestAuthorizer{issuer: issuer, allow: false}
	handler := withDashboardRBAC(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), authorizer, "dashboard")
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "user:viewer@example.com", Audience: "dashboard",
		Scopes: []string{dashboardOAuthScope}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodGet,
		"http://localhost/api/manage/compute/terminal?container=minisky-vm",
		nil,
	)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-MiniSky-Project", "team-project")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden || authorizer.permission != "minisky.dashboard.terminal" {
		t.Fatalf("status=%d permission=%q body=%s", response.Code, authorizer.permission, response.Body.String())
	}
}

func TestDashboardProjectCreateUsesExistingParentAndDedicatedPermission(t *testing.T) {
	for _, test := range []struct {
		name       string
		allow      bool
		wantStatus int
	}{
		{name: "allow", allow: true, wantStatus: http.StatusNoContent},
		{name: "deny", allow: false, wantStatus: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
			authorizer := &dashboardTestAuthorizer{issuer: issuer, allow: test.allow}
			handler := withDashboardRBAC(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}), authorizer, "dashboard")
			token, _, err := issuer.Issue(localsecurity.TokenRequest{
				Subject: "user:admin@example.com", Audience: "dashboard",
				Scopes: []string{dashboardOAuthScope}, Lifetime: time.Minute,
			})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "http://localhost/api/projects",
				bytes.NewBufferString(`{"projectId":"new-project","parent":"folders/200"}`))
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if authorizer.resource != "folders/200" ||
				authorizer.permission != "resourcemanager.projects.create" {
				t.Fatalf("resource=%q permission=%q", authorizer.resource, authorizer.permission)
			}
			if authorizer.resource == "projects/new-project" {
				t.Fatal("project creation was authorized against the not-yet-existing project")
			}
		})
	}
}

func TestDashboardProjectCreateForwardsAuthorizedDefaultParent(t *testing.T) {
	store, err := state.New(t.TempDir(), "dashboard-project-parent")
	if err != nil {
		t.Fatal(err)
	}
	projectAPI, err := resourcemanager.NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	authorizer := &dashboardTestAuthorizer{issuer: issuer, allow: true}
	api := &API{projectAPI: projectAPI}
	handler := withDashboardRBAC(http.HandlerFunc(api.handleProjects), authorizer, "dashboard")
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "user:admin@example.com", Audience: "dashboard",
		Scopes: []string{dashboardOAuthScope}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://localhost/api/projects",
		bytes.NewBufferString(`{"projectId":"new-project"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if authorizer.resource != "organizations/100000000000" {
		t.Fatalf("authorized resource=%q", authorizer.resource)
	}
	var operation struct {
		Response resourcemanager.Project `json:"response"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Fatal(err)
	}
	if operation.Response.Parent != authorizer.resource {
		t.Fatalf("persisted parent=%q, authorized parent=%q", operation.Response.Parent, authorizer.resource)
	}
	restarted, err := resourcemanager.NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	ancestors := restarted.Ancestors("projects/new-project")
	if len(ancestors) != 2 || ancestors[1] != authorizer.resource {
		t.Fatalf("ancestors after restart=%v", ancestors)
	}
}

func TestDashboardWebSocketOriginMustMatchListener(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8081/api/manage/compute/terminal", nil)
	request.Host = "127.0.0.1:8081"
	request.Header.Set("Origin", "http://evil.example")
	if dashboardSameOrigin(request) {
		t.Fatal("cross-origin terminal WebSocket was accepted")
	}
	request.Header.Set("Origin", "http://127.0.0.1:8081")
	if !dashboardSameOrigin(request) {
		t.Fatal("same-origin terminal WebSocket was rejected")
	}
}
