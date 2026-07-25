package iam

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
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
