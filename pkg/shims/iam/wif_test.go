package iam

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

const (
	wifPoolName     = "projects/test-project/locations/global/workloadIdentityPools/github"
	wifProviderName = wifPoolName + "/providers/actions"
)

func TestWorkloadIdentityPoolProviderLifecycle(t *testing.T) {
	api := newAPI(nil)
	api.opMgr = orchestrator.NewOperationManager()

	createPool := wifRequest(t, api, http.MethodPost,
		"/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=github",
		`{"displayName":"GitHub","description":"CI identities","disabled":false}`)
	assertWIFOperation(t, api, createPool, wifPoolName)

	getPool := wifRequest(t, api, http.MethodGet, "/v1/"+wifPoolName, "")
	if getPool.Code != http.StatusOK {
		t.Fatalf("get pool: status=%d body=%s", getPool.Code, getPool.Body.String())
	}
	var pool WorkloadIdentityPool
	decodeResponse(t, getPool, &pool)
	if pool.Name != wifPoolName || pool.DisplayName != "GitHub" || pool.State != "ACTIVE" {
		t.Fatalf("pool = %#v", pool)
	}

	attestation := wifRequest(t, api, http.MethodGet, "/v1/"+wifPoolName+":listAttestationRules", "")
	var rules struct {
		AttestationRules []any `json:"attestationRules"`
	}
	decodeResponse(t, attestation, &rules)
	if rules.AttestationRules == nil || len(rules.AttestationRules) != 0 {
		t.Fatalf("attestation rules = %#v", rules.AttestationRules)
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		response := wifRequest(t, api, method, "/v1/"+wifPoolName+":listAttestationRules", "")
		assertIAMError(t, response, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
		if allow := response.Header().Get("Allow"); allow != http.MethodGet {
			t.Fatalf("%s Allow = %q, want GET", method, allow)
		}
	}

	createProvider := wifRequest(t, api, http.MethodPost,
		"/v1/"+wifPoolName+"/providers?workloadIdentityPoolProviderId=actions",
		`{"displayName":"Actions","attributeMapping":{"google.subject":"assertion.sub"},"attributeCondition":"assertion.repository_owner == 'minisky'","oidc":{"issuerUri":"https://token.actions.githubusercontent.com","allowedAudiences":["minisky"],"jwksJson":"{\"keys\":[]}"}}`)
	assertWIFOperation(t, api, createProvider, wifProviderName)

	patch := wifRequest(t, api, http.MethodPatch,
		"/v1/"+wifProviderName+"?updateMask=displayName%2Cdisabled%2Coidc.allowed_audiences%2Coidc.issuer_uri%2Coidc.jwks_json",
		`{"displayName":"GitHub Actions","disabled":true,"description":"must not change","oidc":{"issuerUri":"https://issuer.example","allowedAudiences":["updated"],"jwksJson":"{\"keys\":[{}]}"}}`)
	assertWIFOperation(t, api, patch, wifProviderName)

	getProvider := wifRequest(t, api, http.MethodGet, "/v1/"+wifProviderName, "")
	var provider WorkloadIdentityPoolProvider
	decodeResponse(t, getProvider, &provider)
	if provider.DisplayName != "GitHub Actions" || !provider.Disabled || provider.Description != "" ||
		provider.OIDC == nil || provider.OIDC.IssuerURI != "https://issuer.example" ||
		len(provider.OIDC.AllowedAudiences) != 1 || provider.OIDC.AllowedAudiences[0] != "updated" ||
		provider.OIDC.JWKSJSON != `{"keys":[{}]}` ||
		provider.AttributeMapping["google.subject"] != "assertion.sub" {
		t.Fatalf("provider = %#v", provider)
	}

	listProviders := wifRequest(t, api, http.MethodGet, "/v1/"+wifPoolName+"/providers", "")
	var providerList struct {
		Providers []WorkloadIdentityPoolProvider `json:"workloadIdentityPoolProviders"`
	}
	decodeResponse(t, listProviders, &providerList)
	if len(providerList.Providers) != 1 || providerList.Providers[0].Name != wifProviderName {
		t.Fatalf("providers = %#v", providerList.Providers)
	}

	deleteProvider := wifRequest(t, api, http.MethodDelete, "/v1/"+wifProviderName, "")
	assertWIFOperation(t, api, deleteProvider, wifProviderName)
	assertIAMError(t, wifRequest(t, api, http.MethodGet, "/v1/"+wifProviderName, ""), http.StatusNotFound, "NOT_FOUND")

	deletePool := wifRequest(t, api, http.MethodDelete, "/v1/"+wifPoolName, "")
	assertWIFOperation(t, api, deletePool, wifPoolName)
	assertIAMError(t, wifRequest(t, api, http.MethodGet, "/v1/"+wifPoolName, ""), http.StatusNotFound, "NOT_FOUND")
}

func TestWorkloadIdentityPoolsListDuplicateMalformedAndUnsupported(t *testing.T) {
	api := newAPI(nil)
	api.opMgr = orchestrator.NewOperationManager()
	path := "/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=github"
	assertWIFOperation(t, api, wifRequest(t, api, http.MethodPost, path, `{}`), wifPoolName)
	assertIAMError(t, wifRequest(t, api, http.MethodPost, path, `{}`), http.StatusConflict, "ALREADY_EXISTS")
	assertIAMError(t, wifRequest(t, api, http.MethodPost,
		"/v1/projects/test-project/locations/global/workloadIdentityPools", `{}`),
		http.StatusBadRequest, "INVALID_ARGUMENT")
	assertIAMError(t, wifRequest(t, api, http.MethodPost,
		"/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=bad/id", `{}`),
		http.StatusBadRequest, "INVALID_ARGUMENT")
	assertIAMError(t, wifRequest(t, api, http.MethodPost,
		"/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=broken", `{"displayName":`),
		http.StatusBadRequest, "INVALID_ARGUMENT")
	assertIAMError(t, wifRequest(t, api, http.MethodPatch, "/v1/"+wifPoolName, `{"displayName":"ignored"}`),
		http.StatusBadRequest, "INVALID_ARGUMENT")
	assertIAMError(t, wifRequest(t, api, http.MethodPost, "/v1/"+wifPoolName+":undelete", `{}`),
		http.StatusNotImplemented, "UNIMPLEMENTED")

	list := wifRequest(t, api, http.MethodGet,
		"/v1/projects/test-project/locations/global/workloadIdentityPools", "")
	var body struct {
		Pools []WorkloadIdentityPool `json:"workloadIdentityPools"`
	}
	decodeResponse(t, list, &body)
	if len(body.Pools) != 1 || body.Pools[0].Name != wifPoolName {
		t.Fatalf("pools = %#v", body.Pools)
	}
}

func TestWorkloadIdentityOperationUnknownAndMalformed(t *testing.T) {
	api := newAPI(nil)
	api.opMgr = orchestrator.NewOperationManager()
	assertIAMError(t, wifRequest(t, api, http.MethodGet,
		"/v1/projects/test-project/locations/global/workloadIdentityPools/operations/missing", ""),
		http.StatusNotFound, "NOT_FOUND")
	assertIAMError(t, wifRequest(t, api, http.MethodGet,
		"/v1/projects/test-project/locations/global/workloadIdentityPools/operations/", ""),
		http.StatusBadRequest, "INVALID_ARGUMENT")
}

func TestWorkloadIdentityOperationOnlyAllowsGet(t *testing.T) {
	api := newAPI(nil)
	api.opMgr = orchestrator.NewOperationManager()
	create := wifRequest(t, api, http.MethodPost,
		"/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=github", `{}`)
	var operation struct {
		Name string `json:"name"`
	}
	decodeResponse(t, create, &operation)

	for _, method := range []string{
		http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead, http.MethodOptions,
	} {
		t.Run(method, func(t *testing.T) {
			response := wifRequest(t, api, method, "/v1/"+operation.Name, "")
			assertIAMError(t, response, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
			if allow := response.Header().Get("Allow"); allow != http.MethodGet {
				t.Fatalf("Allow = %q, want GET", allow)
			}
		})
	}
}

func TestWorkloadIdentityOperationRejectsWrongKindAndParent(t *testing.T) {
	api := newAPI(nil)
	api.opMgr = orchestrator.NewOperationManager()
	target := wifPoolName

	wrongKind, err := api.opMgr.RegisterDurable("compute#operation", "CREATE", target, "", "global")
	if err != nil {
		t.Fatal(err)
	}
	assertIAMError(t, wifRequest(t, api, http.MethodGet,
		"/v1/"+workloadIdentityOperationParent(target)+"/operations/"+wrongKind.Name, ""),
		http.StatusNotFound, "NOT_FOUND")

	correct, err := api.opMgr.RegisterDurable("iam#workloadIdentityOperation", "CREATE", target, "", "global")
	if err != nil {
		t.Fatal(err)
	}
	assertIAMError(t, wifRequest(t, api, http.MethodGet,
		"/v1/projects/other-project/locations/global/workloadIdentityPools/other/operations/"+correct.Name, ""),
		http.StatusNotFound, "NOT_FOUND")
}

func TestWorkloadIdentityRejectsNumericAndMalformedParents(t *testing.T) {
	api := newAPI(nil)
	api.opMgr = orchestrator.NewOperationManager()

	for _, target := range []string{
		"/v1/projects/123456789012/locations/global/workloadIdentityPools?workloadIdentityPoolId=github",
		"/v1/projects/Bad_Project/locations/global/workloadIdentityPools?workloadIdentityPoolId=github",
		"/v1/projects/test-project/locations/us-central1/workloadIdentityPools?workloadIdentityPoolId=github",
		"/v1/projects/test-project/workloadIdentityPools?workloadIdentityPoolId=github",
	} {
		assertIAMError(t, wifRequest(t, api, http.MethodPost, target, `{}`),
			http.StatusBadRequest, "INVALID_ARGUMENT")
	}
	assertIAMError(t, wifRequest(t, api, http.MethodGet,
		"/v1/projects/123456789012/locations/global/workloadIdentityPools/github", ""),
		http.StatusBadRequest, "INVALID_ARGUMENT")

	local := wifRequest(t, api, http.MethodPost,
		"/v1/projects/local-dev-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=github", `{}`)
	if local.Code != http.StatusOK {
		t.Fatalf("local-dev-project status=%d body=%s", local.Code, local.Body.String())
	}
}

func TestWorkloadIdentityMetadataRestartAndExportBoundary(t *testing.T) {
	store, err := state.New(t.TempDir(), "wif")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api.opMgr = orchestrator.NewOperationManager()
	assertWIFOperation(t, api, wifRequest(t, api, http.MethodPost,
		"/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=github", `{}`), wifPoolName)
	assertWIFOperation(t, api, wifRequest(t, api, http.MethodPost,
		"/v1/"+wifPoolName+"/providers?workloadIdentityPoolProviderId=actions",
		`{"attributeMapping":{"google.subject":"assertion.sub"},"oidc":{"issuerUri":"https://issuer.example"}}`), wifProviderName)

	restarted, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if response := wifRequest(t, restarted, http.MethodGet, "/v1/"+wifPoolName, ""); response.Code != http.StatusOK {
		t.Fatalf("restart get: status=%d body=%s", response.Code, response.Body.String())
	}
	if response := wifRequest(t, restarted, http.MethodGet, "/v1/"+wifProviderName, ""); response.Code != http.StatusOK {
		t.Fatalf("restart provider get: status=%d body=%s", response.Code, response.Body.String())
	}

	var exported bytes.Buffer
	if err := store.Export(&exported); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(exported.Bytes(), []byte(`workloadIdentityPools`)) {
		t.Fatalf("export omitted WIF metadata: %s", exported.String())
	}
	if bytes.Contains(exported.Bytes(), []byte(`operation-`)) {
		t.Fatalf("IAM metadata exported transient operation: %s", exported.String())
	}
}

func TestWorkloadIdentitySaveFailureAndConcurrentAccess(t *testing.T) {
	store := &failingWIFStore{}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api.opMgr = orchestrator.NewOperationManager()
	failed := wifRequest(t, api, http.MethodPost,
		"/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=github", `{}`)
	assertIAMError(t, failed, http.StatusInternalServerError, "INTERNAL")

	concurrent := newAPI(nil)
	concurrent.opMgr = orchestrator.NewOperationManager()
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("pool-%d", i)
			wifRequest(t, concurrent, http.MethodPost,
				"/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId="+id, `{}`)
			wifRequest(t, concurrent, http.MethodGet,
				"/v1/projects/test-project/locations/global/workloadIdentityPools", "")
		}(i)
	}
	wg.Wait()
}

func TestWorkloadIdentityMutationsAreAtomicOnSaveFailure(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*API)
		method string
		target string
		body   string
		assert func(*testing.T, *API)
	}{
		{
			name:   "create pool",
			method: http.MethodPost,
			target: "/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=github",
			body:   `{"displayName":"GitHub"}`,
			assert: func(t *testing.T, api *API) {
				assertIAMError(t, wifRequest(t, api, http.MethodGet, "/v1/"+wifPoolName, ""), http.StatusNotFound, "NOT_FOUND")
			},
		},
		{
			name: "patch pool",
			setup: func(api *API) {
				api.workloadIdentityPools[wifPoolName] = &WorkloadIdentityPool{Name: wifPoolName, DisplayName: "before", State: "ACTIVE"}
			},
			method: http.MethodPatch,
			target: "/v1/" + wifPoolName + "?updateMask=displayName",
			body:   `{"displayName":"after"}`,
			assert: func(t *testing.T, api *API) {
				var pool WorkloadIdentityPool
				decodeResponse(t, wifRequest(t, api, http.MethodGet, "/v1/"+wifPoolName, ""), &pool)
				if pool.DisplayName != "before" {
					t.Fatalf("pool display name = %q, want before", pool.DisplayName)
				}
			},
		},
		{
			name: "delete pool",
			setup: func(api *API) {
				api.workloadIdentityPools[wifPoolName] = &WorkloadIdentityPool{Name: wifPoolName, State: "ACTIVE"}
				api.workloadIdentityProviders[wifProviderName] = &WorkloadIdentityPoolProvider{Name: wifProviderName, State: "ACTIVE"}
			},
			method: http.MethodDelete,
			target: "/v1/" + wifPoolName,
			assert: func(t *testing.T, api *API) {
				if response := wifRequest(t, api, http.MethodGet, "/v1/"+wifPoolName, ""); response.Code != http.StatusOK {
					t.Fatalf("pool disappeared after failed delete: status=%d body=%s", response.Code, response.Body.String())
				}
				if response := wifRequest(t, api, http.MethodGet, "/v1/"+wifProviderName, ""); response.Code != http.StatusOK {
					t.Fatalf("provider disappeared after failed pool delete: status=%d body=%s", response.Code, response.Body.String())
				}
			},
		},
		{
			name: "create provider",
			setup: func(api *API) {
				api.workloadIdentityPools[wifPoolName] = &WorkloadIdentityPool{Name: wifPoolName, State: "ACTIVE"}
			},
			method: http.MethodPost,
			target: "/v1/" + wifPoolName + "/providers?workloadIdentityPoolProviderId=actions",
			body:   `{"displayName":"Actions"}`,
			assert: func(t *testing.T, api *API) {
				assertIAMError(t, wifRequest(t, api, http.MethodGet, "/v1/"+wifProviderName, ""), http.StatusNotFound, "NOT_FOUND")
			},
		},
		{
			name: "patch provider",
			setup: func(api *API) {
				api.workloadIdentityPools[wifPoolName] = &WorkloadIdentityPool{Name: wifPoolName, State: "ACTIVE"}
				api.workloadIdentityProviders[wifProviderName] = &WorkloadIdentityPoolProvider{
					Name: wifProviderName, DisplayName: "before", State: "ACTIVE",
					OIDC: &WorkloadIdentityPoolOIDC{JWKSJSON: `{"keys":[]}`},
				}
			},
			method: http.MethodPatch,
			target: "/v1/" + wifProviderName + "?updateMask=displayName%2Coidc.jwksJson",
			body:   `{"displayName":"after","oidc":{"jwksJson":"{\"keys\":[{}]}"}}`,
			assert: func(t *testing.T, api *API) {
				var provider WorkloadIdentityPoolProvider
				decodeResponse(t, wifRequest(t, api, http.MethodGet, "/v1/"+wifProviderName, ""), &provider)
				if provider.DisplayName != "before" || provider.OIDC == nil || provider.OIDC.JWKSJSON != `{"keys":[]}` {
					t.Fatalf("provider changed after failed save: %#v", provider)
				}
			},
		},
		{
			name: "delete provider",
			setup: func(api *API) {
				api.workloadIdentityPools[wifPoolName] = &WorkloadIdentityPool{Name: wifPoolName, State: "ACTIVE"}
				api.workloadIdentityProviders[wifProviderName] = &WorkloadIdentityPoolProvider{Name: wifProviderName, State: "ACTIVE"}
			},
			method: http.MethodDelete,
			target: "/v1/" + wifProviderName,
			assert: func(t *testing.T, api *API) {
				if response := wifRequest(t, api, http.MethodGet, "/v1/"+wifProviderName, ""); response.Code != http.StatusOK {
					t.Fatalf("provider disappeared after failed delete: status=%d body=%s", response.Code, response.Body.String())
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api, err := NewAPIWithStore(&failingWIFStore{})
			if err != nil {
				t.Fatal(err)
			}
			if test.setup != nil {
				test.setup(api)
			}
			response := wifRequest(t, api, test.method, test.target, test.body)
			assertIAMError(t, response, http.StatusInternalServerError, "INTERNAL")
			test.assert(t, api)
		})
	}
}

func TestWorkloadIdentityMutationIsNotObservableBeforeSaveSucceeds(t *testing.T) {
	store := &blockingFailingWIFStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	api, err := NewAPIWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	api.workloadIdentityPools[wifPoolName] = &WorkloadIdentityPool{
		Name: wifPoolName, DisplayName: "before", State: "ACTIVE",
	}

	result := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		request := httptest.NewRequest(http.MethodPatch, "/v1/"+wifPoolName+"?updateMask=displayName", bytes.NewBufferString(`{"displayName":"after"}`))
		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)
		result <- response
	}()

	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("save did not start")
	}
	var during WorkloadIdentityPool
	decodeResponse(t, wifRequest(t, api, http.MethodGet, "/v1/"+wifPoolName, ""), &during)
	if during.DisplayName != "before" {
		t.Fatalf("mutation became observable before save completed: %#v", during)
	}
	close(store.release)
	response := <-result
	assertIAMError(t, response, http.StatusInternalServerError, "INTERNAL")

	var after WorkloadIdentityPool
	decodeResponse(t, wifRequest(t, api, http.MethodGet, "/v1/"+wifPoolName, ""), &after)
	if after.DisplayName != "before" {
		t.Fatalf("mutation remained observable after failed save: %#v", after)
	}
}

func TestWorkloadIdentityOperationRegistrationFailureDoesNotCommitResource(t *testing.T) {
	opMgr, err := orchestrator.NewOperationManagerWithStore(&failingWIFStore{})
	if err != nil {
		t.Fatal(err)
	}
	api := newAPI(nil)
	api.opMgr = opMgr

	response := wifRequest(t, api, http.MethodPost,
		"/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=github", `{}`)
	assertIAMError(t, response, http.StatusInternalServerError, "INTERNAL")
	assertIAMError(t, wifRequest(t, api, http.MethodGet, "/v1/"+wifPoolName, ""), http.StatusNotFound, "NOT_FOUND")
}

func TestWorkloadIdentityRequestAndJWKSBounds(t *testing.T) {
	api := newAPI(nil)
	api.workloadIdentityPools[wifPoolName] = &WorkloadIdentityPool{Name: wifPoolName, State: "ACTIVE"}
	api.workloadIdentityProviders[wifProviderName] = &WorkloadIdentityPoolProvider{Name: wifProviderName, State: "ACTIVE"}

	assertIAMError(t, wifRequest(t, api, http.MethodPost,
		"/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=trail", `{}garbage`),
		http.StatusBadRequest, "INVALID_ARGUMENT")
	assertIAMError(t, wifRequest(t, api, http.MethodGet,
		"/v1/projects/test-project/locations/global/workloadIdentityPools/trail", ""),
		http.StatusNotFound, "NOT_FOUND")

	oversizedJWKS := strings.Repeat("x", (64<<10)+1)
	assertIAMError(t, wifRequest(t, api, http.MethodPost,
		"/v1/"+wifPoolName+"/providers?workloadIdentityPoolProviderId=large",
		`{"oidc":{"jwksJson":"`+oversizedJWKS+`"}}`),
		http.StatusBadRequest, "INVALID_ARGUMENT")
	assertIAMError(t, wifRequest(t, api, http.MethodGet,
		"/v1/"+wifPoolName+"/providers/large", ""),
		http.StatusNotFound, "NOT_FOUND")
	assertIAMError(t, wifRequest(t, api, http.MethodPatch,
		"/v1/"+wifProviderName+"?updateMask=oidc.jwksJson",
		`{"oidc":{"jwksJson":"`+oversizedJWKS+`"}}`),
		http.StatusBadRequest, "INVALID_ARGUMENT")
	var provider WorkloadIdentityPoolProvider
	decodeResponse(t, wifRequest(t, api, http.MethodGet, "/v1/"+wifProviderName, ""), &provider)
	if provider.OIDC != nil {
		t.Fatalf("oversized patch stored JWKS: %#v", provider.OIDC)
	}

	oversizedBody := `{"displayName":"` + strings.Repeat("x", (1<<20)+1) + `"}`
	assertIAMError(t, wifRequest(t, api, http.MethodPost,
		"/v1/projects/test-project/locations/global/workloadIdentityPools?workloadIdentityPoolId=large", oversizedBody),
		http.StatusRequestEntityTooLarge, "RESOURCE_EXHAUSTED")
	assertIAMError(t, wifRequest(t, api, http.MethodPatch,
		"/v1/"+wifPoolName+"?updateMask=displayName", oversizedBody),
		http.StatusRequestEntityTooLarge, "RESOURCE_EXHAUSTED")
}

func TestLookupWorkloadIdentityProviderUsesExactAudienceAndReturnsDeepCopy(t *testing.T) {
	api := newAPI(nil)
	api.workloadIdentityPools[wifPoolName] = &WorkloadIdentityPool{Name: wifPoolName, State: "ACTIVE"}
	api.workloadIdentityProviders[wifProviderName] = &WorkloadIdentityPoolProvider{
		Name:             wifProviderName,
		State:            "ACTIVE",
		AttributeMapping: map[string]string{"google.subject": "assertion.sub"},
		OIDC: &WorkloadIdentityPoolOIDC{
			IssuerURI:        "https://issuer.invalid",
			AllowedAudiences: []string{"audience"},
			JWKSJSON:         `{"keys":[]}`,
		},
	}

	audience := "//iam.googleapis.com/" + wifProviderName
	config, ok := api.LookupWorkloadIdentityProvider(audience)
	if !ok || config.Pool.Name != wifPoolName || config.Provider.Name != wifProviderName {
		t.Fatalf("lookup = %#v, %v", config, ok)
	}
	config.Pool.Disabled = true
	config.Provider.AttributeMapping["google.subject"] = "changed"
	config.Provider.OIDC.AllowedAudiences[0] = "changed"

	again, ok := api.LookupWorkloadIdentityProvider(audience)
	if !ok || again.Pool.Disabled ||
		again.Provider.AttributeMapping["google.subject"] != "assertion.sub" ||
		again.Provider.OIDC.AllowedAudiences[0] != "audience" {
		t.Fatalf("lookup exposed mutable state: %#v", again)
	}
	if _, ok := api.LookupWorkloadIdentityProvider("https://iam.googleapis.com/" + wifProviderName); ok {
		t.Fatal("lookup accepted a non-canonical audience")
	}
	for _, audience := range []string{
		"//iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/github/providers/actions",
		"//iam.googleapis.com/projects/Bad_Project/locations/global/workloadIdentityPools/github/providers/actions",
		"//iam.googleapis.com/projects/test-project/locations/us-central1/workloadIdentityPools/github/providers/actions",
	} {
		if _, ok := api.LookupWorkloadIdentityProvider(audience); ok {
			t.Fatalf("lookup accepted invalid audience %q", audience)
		}
	}
}

type failingWIFStore struct{}

func (*failingWIFStore) Load(string, any) error { return state.ErrNotFound }
func (*failingWIFStore) Save(string, any) error { return errors.New("disk full") }

type blockingFailingWIFStore struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*blockingFailingWIFStore) Load(string, any) error { return state.ErrNotFound }

func (s *blockingFailingWIFStore) Save(string, any) error {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return errors.New("disk full")
}

func wifRequest(t *testing.T, api *API, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}

func assertWIFOperation(t *testing.T, api *API, response *httptest.ResponseRecorder, target string) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("operation status=%d body=%s", response.Code, response.Body.String())
	}
	var operation struct {
		Name string `json:"name"`
		Done bool   `json:"done"`
	}
	decodeResponse(t, response, &operation)
	if operation.Name == "" || operation.Done {
		t.Fatalf("initial operation = %#v", operation)
	}
	wantOperationPrefix := workloadIdentityOperationParent(target) + "/operations/"
	if !strings.HasPrefix(operation.Name, wantOperationPrefix) {
		t.Fatalf("operation name = %q, want prefix %q", operation.Name, wantOperationPrefix)
	}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		poll := wifRequest(t, api, http.MethodGet, "/v1/"+operation.Name, "")
		if poll.Code != http.StatusOK {
			t.Fatalf("poll status=%d body=%s", poll.Code, poll.Body.String())
		}
		var result map[string]any
		if err := json.Unmarshal(poll.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if done, _ := result["done"].(bool); done {
			metadata, _ := result["metadata"].(map[string]any)
			if metadata["target"] != target {
				t.Fatalf("operation metadata = %#v, target %q", metadata, target)
			}
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("operation did not complete")
}
