package iam

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/state"
)

const testResource = "/v1/projects/test-project"

func TestStrictModeTestIamPermissions(t *testing.T) {
	t.Setenv("MINISKY_IAM_MODE", "strict")
	api := newAPI(nil)
	setTestPolicy(t, api, []Binding{
		{
			Role:    "roles/storage.admin",
			Members: []string{"user:alice@example.com"},
		},
	})

	t.Run("Alice allowed", func(t *testing.T) {
		response := testPermissions(t, api, "user:alice@example.com", `{
			"permissions": ["storage.objects.get", "storage.objects.delete", "compute.instances.get"]
		}`)
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}

		var body struct {
			Permissions []string `json:"permissions"`
		}
		decodeResponse(t, response, &body)
		want := []string{"storage.objects.get", "storage.objects.delete"}
		if fmt.Sprint(body.Permissions) != fmt.Sprint(want) {
			t.Fatalf("permissions = %v, want %v", body.Permissions, want)
		}
	})

	t.Run("Bob denied", func(t *testing.T) {
		response := testPermissions(t, api, "user:bob@example.com", `{
			"permissions": ["storage.objects.get"]
		}`)
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}

		var body struct {
			Permissions []string `json:"permissions"`
		}
		decodeResponse(t, response, &body)
		if len(body.Permissions) != 0 {
			t.Fatalf("permissions = %v, want none", body.Permissions)
		}
	})

	t.Run("missing principal", func(t *testing.T) {
		response := testPermissions(t, api, "", `{"permissions":["storage.objects.get"]}`)
		assertIAMError(t, response, http.StatusForbidden, "PERMISSION_DENIED")
	})
}

func TestPermissiveModeReturnsAllRequestedPermissions(t *testing.T) {
	t.Setenv("MINISKY_IAM_MODE", "")
	api := newAPI(nil)

	response := testPermissions(t, api, "", `{
		"permissions": ["storage.objects.get", "compute.instances.delete"]
	}`)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}

	var body struct {
		Permissions []string `json:"permissions"`
	}
	decodeResponse(t, response, &body)
	if len(body.Permissions) != 2 {
		t.Fatalf("permissions = %v, want both requested permissions", body.Permissions)
	}
}

func TestTestIamPermissionsRejectsMalformedRequests(t *testing.T) {
	for _, mode := range []string{"", "strict"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MINISKY_IAM_MODE", mode)
			api := newAPI(nil)
			for _, body := range []string{`{"permissions":`, `{}`} {
				response := testPermissions(t, api, "user:alice@example.com", body)
				assertIAMError(t, response, http.StatusBadRequest, "INVALID_ARGUMENT")
			}
		})
	}
}

func TestStrictModeDirectPermissionRole(t *testing.T) {
	t.Setenv("MINISKY_IAM_MODE", "strict")
	api := newAPI(nil)
	setTestPolicy(t, api, []Binding{{
		Role:    "permission:pubsub.topics.publish",
		Members: []string{"serviceAccount:publisher@test-project.iam.gserviceaccount.com"},
	}})

	response := testPermissions(
		t,
		api,
		"serviceAccount:publisher@test-project.iam.gserviceaccount.com",
		`{"permissions":["pubsub.topics.publish","pubsub.topics.delete"]}`,
	)
	var body struct {
		Permissions []string `json:"permissions"`
	}
	decodeResponse(t, response, &body)
	if fmt.Sprint(body.Permissions) != "[pubsub.topics.publish]" {
		t.Fatalf("permissions = %v, want direct permission", body.Permissions)
	}
}

func TestServiceAccountImpersonationRolesAndResolution(t *testing.T) {
	api := newAPI(nil)
	api.strict = true
	account := &ServiceAccount{
		Name:      "projects/test-project/serviceAccounts/worker@test-project.iam.gserviceaccount.com",
		ProjectId: "test-project", UniqueId: "1234567890",
		Email: "worker@test-project.iam.gserviceaccount.com",
	}
	api.serviceAccounts["test-project:"+account.Email] = account
	policyResource := "projects/test-project/serviceAccounts/" + account.Email
	api.policies[policyResource] = &IamPolicy{Bindings: []Binding{
		{Role: "roles/iam.workloadIdentityUser", Members: []string{"principal://iam.googleapis.com/pool/subject/workload"}},
		{Role: "roles/iam.serviceAccountTokenCreator", Members: []string{"serviceAccount:delegate@example.com"}},
	}}
	requestResource := "projects/-/serviceAccounts/" + account.Email
	for principal := range map[string]struct{}{
		"principal://iam.googleapis.com/pool/subject/workload": {},
		"serviceAccount:delegate@example.com":                  {},
	} {
		if !api.Authorize(requestResource, principal, "iam.serviceAccounts.getAccessToken") {
			t.Fatalf("%s lacks getAccessToken", principal)
		}
	}
	for _, identifier := range []string{account.Email, account.UniqueId} {
		email, disabled, found := api.ResolveServiceAccount(identifier)
		if !found || disabled || email != account.Email {
			t.Fatalf("resolve %q = %q, %v, %v", identifier, email, disabled, found)
		}
	}
	account.Disabled = true
	if _, disabled, found := api.ResolveServiceAccount(account.Email); !found || !disabled {
		t.Fatalf("disabled account resolution = %v, %v", disabled, found)
	}
	if _, _, found := api.ResolveServiceAccount("missing@example.com"); found {
		t.Fatal("missing account resolved")
	}
}

func TestResolveServiceAccountSkipsNilAndMalformedEntries(t *testing.T) {
	api := newAPI(nil)
	validKey, valid := validPersistedServiceAccount("worker")
	valid.UniqueId = "100"
	_, malformed := validPersistedServiceAccount("broken")
	malformed.UniqueId = "200"
	malformed.Email = "Broken@test-project.iam.gserviceaccount.com"
	malformed.Name = "projects/test-project/serviceAccounts/" + malformed.Email
	_, keyMismatch := validPersistedServiceAccount("another")
	keyMismatch.UniqueId = "300"
	api.serviceAccounts = map[string]*ServiceAccount{
		"test-project:nil@test-project.iam.gserviceaccount.com": nil,
		"test-project:" + malformed.Email:                       malformed,
		"wrong-project:" + keyMismatch.Email:                    keyMismatch,
		validKey:                                                valid,
	}

	for _, identifier := range []string{
		malformed.Email,
		malformed.UniqueId,
		keyMismatch.Email,
		keyMismatch.UniqueId,
	} {
		if email, disabled, found := api.ResolveServiceAccount(identifier); found {
			t.Fatalf("malformed identifier %q resolved as email=%q disabled=%t", identifier, email, disabled)
		}
	}
	for _, identifier := range []string{valid.Email, valid.UniqueId} {
		if email, disabled, found := api.ResolveServiceAccount(identifier); !found || disabled || email != valid.Email {
			t.Fatalf("valid identifier %q resolved as email=%q disabled=%t found=%t",
				identifier, email, disabled, found)
		}
	}
	duplicateKey, duplicate := validPersistedServiceAccount("duplicate")
	duplicate.UniqueId = valid.UniqueId
	api.serviceAccounts[duplicateKey] = duplicate
	if email, disabled, found := api.ResolveServiceAccount(valid.UniqueId); found {
		t.Fatalf("duplicate unique ID resolved as email=%q disabled=%t", email, disabled)
	}
}

func TestCreateServiceAccountValidatesAccountIDBoundaries(t *testing.T) {
	thirtyCharacters := "a" + strings.Repeat("b", 29)
	tests := []struct {
		name      string
		accountID string
		want      int
	}{
		{name: "five characters", accountID: "abcde", want: http.StatusBadRequest},
		{name: "six characters", accountID: "abcdef", want: http.StatusOK},
		{name: "thirty characters", accountID: thirtyCharacters, want: http.StatusOK},
		{name: "thirty one characters", accountID: thirtyCharacters + "c", want: http.StatusBadRequest},
		{name: "uppercase", accountID: "Worker", want: http.StatusBadRequest},
		{name: "leading digit", accountID: "1worker", want: http.StatusBadRequest},
		{name: "trailing hyphen", accountID: "worker-", want: http.StatusBadRequest},
		{name: "colon", accountID: "worker:id", want: http.StatusBadRequest},
		{name: "control", accountID: "worker\nid", want: http.StatusBadRequest},
		{name: "encoded alias", accountID: "worker%2Fid", want: http.StatusBadRequest},
		{name: "separator", accountID: "worker/id", want: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newAPI(nil)
			body, err := json.Marshal(map[string]any{"accountId": test.accountID})
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(
				http.MethodPost,
				"/v1/projects/test-project/serviceAccounts",
				bytes.NewReader(body),
			))
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.want, response.Body.String())
			}
		})
	}
}

func TestServiceAccountViewerCanReadAccount(t *testing.T) {
	api := newAPI(nil)
	api.strict = true
	const (
		resource  = "projects/test-project/serviceAccounts/worker@test-project.iam.gserviceaccount.com"
		principal = "serviceAccount:worker@test-project.iam.gserviceaccount.com"
	)
	api.policies[resource] = &IamPolicy{Bindings: []Binding{{
		Role:    "roles/iam.serviceAccountViewer",
		Members: []string{principal},
	}}}
	if !api.Authorize(resource, principal, "iam.serviceAccounts.get") {
		t.Fatal("service account viewer lacks iam.serviceAccounts.get")
	}
}

func TestSpannerReadRolesCanListBackups(t *testing.T) {
	for _, role := range []string{"roles/spanner.viewer", "roles/spanner.admin"} {
		t.Run(role, func(t *testing.T) {
			const (
				projectResource  = "projects/test-project"
				instanceResource = projectResource + "/instances/cache"
				principal        = "user:alice@example.com"
			)
			for _, policyResource := range []string{instanceResource, projectResource} {
				api := newAPI(nil)
				api.strict = true
				api.policies[policyResource] = &IamPolicy{Bindings: []Binding{{
					Role: role, Members: []string{principal},
				}}}
				if !api.Authorize(instanceResource, principal, "spanner.backups.list") {
					t.Fatalf("%s binding at %s does not authorize the instance", role, policyResource)
				}
				if api.Authorize(instanceResource, "user:bob@example.com", "spanner.backups.list") {
					t.Fatalf("%s binding at %s authorized an unbound principal", role, policyResource)
				}
			}
		})
	}
}

func TestMiniSkyLocalRolesCoverDashboardAndGatewayPermissions(t *testing.T) {
	t.Setenv("MINISKY_IAM_MODE", "strict")
	api := newAPI(nil)
	api.policies["projects/team-project"] = &IamPolicy{Bindings: []Binding{
		{Role: "roles/minisky.viewer", Members: []string{"user:viewer@example.com"}},
		{Role: "roles/minisky.editor", Members: []string{"user:editor@example.com"}},
		{Role: "roles/minisky.admin", Members: []string{"user:admin@example.com"}},
	}}
	api.policies["organizations/100000000000"] = &IamPolicy{Bindings: []Binding{
		{Role: "roles/minisky.editor", Members: []string{"user:editor@example.com"}},
		{Role: "roles/minisky.admin", Members: []string{"user:admin@example.com"}},
	}}
	if !api.Authorize("projects/team-project", "user:viewer@example.com", "minisky.dashboard.view") {
		t.Fatal("viewer lacks dashboard view")
	}
	if api.Authorize("projects/team-project", "user:viewer@example.com", "minisky.dashboard.manage") {
		t.Fatal("viewer unexpectedly manages dashboard")
	}
	if api.Authorize("projects/team-project", "user:viewer@example.com", "minisky.dashboard.terminal") {
		t.Fatal("viewer unexpectedly has terminal access")
	}
	if !api.Authorize("projects/team-project", "user:editor@example.com", "compute.instances.create") {
		t.Fatal("editor lacks gateway mutation")
	}
	if !api.Authorize("projects/team-project", "user:admin@example.com", "storage.objects.delete") {
		t.Fatal("admin lacks destructive gateway permission")
	}
	if !api.Authorize("projects/team-project", "user:admin@example.com", "minisky.dashboard.terminal") {
		t.Fatal("admin lacks dedicated terminal permission")
	}
	if !api.Authorize("organizations/100000000000", "user:admin@example.com", "resourcemanager.projects.create") {
		t.Fatal("admin lacks dedicated project-create permission")
	}
	if api.Authorize("organizations/100000000000", "user:editor@example.com", "resourcemanager.projects.create") {
		t.Fatal("editor unexpectedly has project-create permission")
	}
}

func TestFederatedPrincipalViewerRoleUsesExactMemberMatching(t *testing.T) {
	t.Setenv("MINISKY_IAM_MODE", "strict")
	api := newAPI(nil)
	const principal = "principal://iam.googleapis.com/projects/local-dev-project/locations/global/workloadIdentityPools/ci-pool/subject/repository:minisky"
	api.policies["projects/local-dev-project"] = &IamPolicy{Bindings: []Binding{{
		Role:    "roles/minisky.viewer",
		Members: []string{principal},
	}}}

	for _, permission := range []string{
		"minisky.dashboard.view",
		"bigquery.datasets.get",
		"bigquery.datasets.list",
	} {
		if !api.Authorize("projects/local-dev-project", principal, permission) {
			t.Errorf("federated viewer lacks %q", permission)
		}
	}
	for _, permission := range []string{
		"minisky.dashboard.manage",
		"minisky.dashboard.terminal",
		"bigquery.datasets.update",
		"compute.instances.create",
		"storage.objects.delete",
	} {
		if api.Authorize("projects/local-dev-project", principal, permission) {
			t.Errorf("federated viewer unexpectedly has %q", permission)
		}
	}
	for _, nearMatch := range []string{
		strings.TrimSuffix(principal, "minisky"),
		principal + ":admin",
		"principalSet://iam.googleapis.com/projects/local-dev-project/locations/global/workloadIdentityPools/ci-pool/subject/repository:minisky",
	} {
		if api.Authorize("projects/local-dev-project", nearMatch, "minisky.dashboard.view") {
			t.Errorf("near-match principal %q was authorized", nearMatch)
		}
	}
}

func TestAuthorizeNestedChildResourceUsesProjectPolicy(t *testing.T) {
	api := newAPI(nil)
	api.strict = true
	api.policies["projects/team-project"] = &IamPolicy{Bindings: []Binding{{
		Role: "roles/compute.viewer", Members: []string{"user:alice@example.com"},
	}}}
	if !api.Authorize(
		"projects/team-project/zones/us-central1-a/instances/vm-1",
		"user:alice@example.com",
		"compute.instances.get",
	) {
		t.Fatal("project policy did not authorize a nested child resource")
	}
}

func TestPolicyCRUDConcurrentAccess(t *testing.T) {
	t.Setenv("MINISKY_IAM_MODE", "strict")
	api := newAPI(nil)

	const workers = 8
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < 50; iteration++ {
				policy := fmt.Sprintf(`{"policy":{"bindings":[{"role":"roles/compute.viewer","members":["user:alice@example.com"]}],"version":%d}}`, worker+1)
				request := httptest.NewRequest(http.MethodPost, testResource+":setIamPolicy", bytes.NewBufferString(policy))
				api.ServeHTTP(httptest.NewRecorder(), request)

				request = httptest.NewRequest(http.MethodGet, testResource+":getIamPolicy", nil)
				api.ServeHTTP(httptest.NewRecorder(), request)

				request = httptest.NewRequest(http.MethodPost, testResource+":testIamPermissions", bytes.NewBufferString(`{"permissions":["compute.instances.get"]}`))
				request.Header.Set(principalHeader, "user:alice@example.com")
				api.ServeHTTP(httptest.NewRecorder(), request)
			}
		}(worker)
	}
	wg.Wait()
}

type testHierarchy struct{}

func (testHierarchy) Ancestors(resource string) []string {
	return []string{resource, "folders/200", "organizations/100"}
}

func TestAuthorizeUsesInheritedPolicy(t *testing.T) {
	t.Setenv("MINISKY_IAM_MODE", "strict")
	api := newAPI(nil)
	api.hierarchy = testHierarchy{}
	api.policies["organizations/100"] = &IamPolicy{Bindings: []Binding{{
		Role: "roles/pubsub.admin", Members: []string{"user:alice@example.com"},
	}}}
	if !api.Authorize("projects/child-project", "user:alice@example.com", "pubsub.topics.publish") {
		t.Fatal("organization policy was not inherited")
	}
	if api.Authorize("projects/child-project", "user:bob@example.com", "pubsub.topics.publish") {
		t.Fatal("unbound principal inherited permission")
	}
}

func TestServiceAccountKeyDisableDeletePersists(t *testing.T) {
	store, err := state.New(t.TempDir(), "key-lifecycle")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	createAccount := httptest.NewRequest(http.MethodPost, "/v1/projects/test-project/serviceAccounts",
		bytes.NewBufferString(`{"accountId":"worker"}`))
	accountResponse := httptest.NewRecorder()
	api.ServeHTTP(accountResponse, createAccount)
	if accountResponse.Code != http.StatusOK {
		t.Fatalf("create account: %s", accountResponse.Body.String())
	}
	email := "worker@test-project.iam.gserviceaccount.com"
	keyResponse := httptest.NewRecorder()
	api.ServeHTTP(keyResponse, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test-project/serviceAccounts/"+email+"/keys", nil))
	var key ServiceAccountKey
	decodeResponse(t, keyResponse, &key)
	keyID := key.Name[strings.LastIndex(key.Name, "/")+1:]

	disableResponse := httptest.NewRecorder()
	api.ServeHTTP(disableResponse, httptest.NewRequest(http.MethodPost,
		"/v1/projects/test-project/serviceAccounts/"+email+"/keys/"+keyID+":disable", nil))
	if disableResponse.Code != http.StatusOK {
		t.Fatalf("disable key: %s", disableResponse.Body.String())
	}
	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	restarted.mu.RLock()
	disabled := restarted.findKeyLocked("test-project", email, keyID)
	restarted.mu.RUnlock()
	if disabled == nil || !disabled.Disabled {
		t.Fatalf("disabled key was not restored: %#v", disabled)
	}
	deleteResponse := httptest.NewRecorder()
	restarted.ServeHTTP(deleteResponse, httptest.NewRequest(http.MethodDelete,
		"/v1/projects/test-project/serviceAccounts/"+email+"/keys/"+keyID, nil))
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("delete key: %s", deleteResponse.Body.String())
	}
}

func TestServiceAccountKeyExpiry(t *testing.T) {
	api := newAPI(nil)
	now := time.Now().UTC()
	name := "projects/test/serviceAccounts/worker@example.com/keys/key-1"
	api.keys["test:worker@example.com"] = []*ServiceAccountKey{{
		Name: name, ValidAfterTime: now.Add(-time.Hour).Format(time.RFC3339),
		ValidBeforeTime: now.Add(time.Hour).Format(time.RFC3339),
	}}
	if !api.KeyUsable(name, now) {
		t.Fatal("key should be usable inside validity window")
	}
	if api.KeyUsable(name, now.Add(2*time.Hour)) {
		t.Fatal("expired key remained usable")
	}
}

func setTestPolicy(t *testing.T, api *API, bindings []Binding) {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{
		"policy": IamPolicy{Bindings: bindings},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, testResource+":setIamPolicy", bytes.NewReader(body))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("set policy status = %d, body = %s", response.Code, response.Body.String())
	}
}

func testPermissions(t *testing.T, api *API, principal, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, testResource+":testIamPermissions", bytes.NewBufferString(body))
	if principal != "" {
		request.Header.Set(principalHeader, principal)
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder, destination interface{}) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), destination); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
}

func assertIAMError(t *testing.T, response *httptest.ResponseRecorder, code int, status string) {
	t.Helper()
	if response.Code != code {
		t.Fatalf("status code = %d, want %d; body = %s", response.Code, code, response.Body.String())
	}
	var body struct {
		Error struct {
			Code   int    `json:"code"`
			Status string `json:"status"`
		} `json:"error"`
	}
	decodeResponse(t, response, &body)
	if body.Error.Code != code || body.Error.Status != status {
		t.Fatalf("error = %+v, want code %d status %s", body.Error, code, status)
	}
}
