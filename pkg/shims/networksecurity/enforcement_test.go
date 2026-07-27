package networksecurity

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/orchestrator"
)

func TestAuthorizeHTTPAllowDenyNoPolicyAndProjectIsolation(t *testing.T) {
	api := newTestAPI()
	api.policies["projects/p/locations/global/authorizationPolicies/allow-public"] = &AuthorizationPolicy{
		Name:   "projects/p/locations/global/authorizationPolicies/allow-public",
		Action: "ALLOW",
		Rules: []Rule{{Destinations: []Destination{{
			Hosts: []string{"api.example.test"}, Methods: []string{"GET"}, Paths: []string{"/public"},
		}}}},
	}
	api.policies["projects/p/locations/global/authorizationPolicies/deny-admin"] = &AuthorizationPolicy{
		Name:   "projects/p/locations/global/authorizationPolicies/deny-admin",
		Action: "DENY",
		Rules: []Rule{{Destinations: []Destination{{
			Hosts: []string{"api.example.test"}, Methods: []string{"GET"}, Paths: []string{"/admin"},
		}}}},
	}

	for _, tc := range []struct {
		name            string
		project         string
		path            string
		wantMatched     bool
		wantAllowed     bool
		wantPolicyMatch bool
	}{
		{name: "allow", project: "p", path: "/public", wantMatched: true, wantAllowed: true, wantPolicyMatch: true},
		{name: "deny", project: "p", path: "/admin", wantMatched: true, wantAllowed: false, wantPolicyMatch: true},
		{name: "no policy", project: "p", path: "/other", wantAllowed: true},
		{name: "other project", project: "other", path: "/admin", wantAllowed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			matched, allowed, policy, err := api.AuthorizeHTTP(
				tc.project, "global", "192.0.2.1", "api.example.test", http.MethodGet, tc.path,
			)
			if err != nil {
				t.Fatal(err)
			}
			if matched != tc.wantMatched || allowed != tc.wantAllowed ||
				(policy != "") != tc.wantPolicyMatch {
				t.Fatalf("matched=%t allowed=%t policy=%q", matched, allowed, policy)
			}
		})
	}
}

func TestAuthorizeHTTPUsesPersistedPolicyAfterRestart(t *testing.T) {
	store := &mockStore{data: map[string][]byte{}}
	api := newAPI(orchestrator.NewOperationManager(), store)
	api.policies["projects/p/locations/global/authorizationPolicies/deny"] = &AuthorizationPolicy{
		Name: "projects/p/locations/global/authorizationPolicies/deny", Action: "DENY",
	}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	restarted := newAPI(orchestrator.NewOperationManager(), store)
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	matched, allowed, _, err := restarted.AuthorizeHTTP("p", "global", "", "host", http.MethodGet, "/")
	if err != nil || !matched || allowed {
		t.Fatalf("matched=%t allowed=%t err=%v", matched, allowed, err)
	}
}

type failNetworkSecurityStore struct {
	*mockStore
	fail bool
}

func (store *failNetworkSecurityStore) Save(name string, value any) error {
	if store.fail {
		return errors.New("injected save failure")
	}
	return store.mockStore.Save(name, value)
}

func TestAuthorizationPolicySaveFailureKeepsEnforcedDecision(t *testing.T) {
	store := &failNetworkSecurityStore{mockStore: &mockStore{data: map[string][]byte{}}}
	api := newAPI(orchestrator.NewOperationManager(), store)
	name := "projects/p/locations/global/authorizationPolicies/policy"
	api.policies[name] = &AuthorizationPolicy{Name: name, Action: "DENY"}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	store.fail = true
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPatch, "/v1/"+name+"?updateMask=action",
		strings.NewReader(`{"action":"ALLOW"}`)))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	matched, allowed, _, err := api.AuthorizeHTTP("p", "global", "", "host", http.MethodGet, "/")
	if err != nil || !matched || allowed {
		t.Fatalf("post-failure matched=%t allowed=%t err=%v", matched, allowed, err)
	}
}

func TestAuthorizeHTTPConcurrentPolicyChanges(t *testing.T) {
	api := newTestAPI()
	name := "projects/p/locations/global/authorizationPolicies/policy"
	api.policies[name] = &AuthorizationPolicy{Name: name, Action: "ALLOW"}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				matched, _, _, err := api.AuthorizeHTTP("p", "global", "", "host", http.MethodGet, "/")
				if err != nil || !matched {
					t.Errorf("matched=%t err=%v", matched, err)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for index := range 100 {
			api.mu.Lock()
			if index%2 == 0 {
				api.policies[name].Action = "ALLOW"
			} else {
				api.policies[name].Action = "DENY"
			}
			api.mu.Unlock()
		}
	}()
	wg.Wait()
}
