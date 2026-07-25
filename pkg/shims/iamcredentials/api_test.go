package iamcredentials

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	localsecurity "minisky/pkg/security"
)

type fakeIAM struct {
	strict   bool
	allow    bool
	audience string
	allowed  map[string]bool
	accounts map[string]fakeAccount
	issuer   *localsecurity.Issuer
	calls    []authorizationCall
}

type fakeAccount struct {
	email    string
	disabled bool
}

type authorizationCall struct {
	resource  string
	principal string
}

func (f *fakeIAM) EnforcementEnabled() bool { return f.strict }
func (f *fakeIAM) Authorize(resource, principal, _ string) bool {
	f.calls = append(f.calls, authorizationCall{resource: resource, principal: principal})
	if f.allowed != nil {
		return f.allowed[principal+"->"+resource]
	}
	return f.allow
}
func (f *fakeIAM) VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error) {
	return f.issuer.Verify(token, localsecurity.VerifyOptions{Audience: audience, RequiredScope: scope})
}
func (f *fakeIAM) ResolveServiceAccount(identifier string) (string, bool, bool) {
	account, ok := f.accounts[identifier]
	return account.email, account.disabled, ok
}
func (f *fakeIAM) IssueLocalToken(subject, audience string, scopes []string, lifetime time.Duration) (string, time.Time, error) {
	token, claims, err := f.issuer.Issue(localsecurity.TokenRequest{
		Subject: subject, Audience: audience, Scopes: scopes, Lifetime: lifetime,
	})
	return token, claims.ExpiresAt, err
}
func (f *fakeIAM) TokenAudience() string {
	if f.audience != "" {
		return f.audience
	}
	return "minisky-gateway"
}

func TestGenerateAccessTokenContractAndAuthorization(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	api := &API{iam: &fakeIAM{allow: true, issuer: issuer, accounts: accountSet("worker@example.iam.gserviceaccount.com")}}
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/-/serviceAccounts/worker@example.iam.gserviceaccount.com:generateAccessToken",
		bytes.NewBufferString(`{"scope":["https://www.googleapis.com/auth/cloud-platform"],"lifetime":"300s"}`),
	)
	request.Header.Set("X-MiniSky-Principal", "user:developer@example.com")
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		AccessToken string    `json:"accessToken"`
		ExpireTime  time.Time `json:"expireTime"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.AccessToken == "" || time.Until(body.ExpireTime) > 6*time.Minute {
		t.Fatalf("invalid response: %#v", body)
	}

	deniedIAM := &fakeIAM{
		strict: true, allow: false, issuer: issuer, accounts: accountSet("worker@example.iam.gserviceaccount.com"),
	}
	api.iam = deniedIAM
	request = httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/-/serviceAccounts/worker@example.iam.gserviceaccount.com:generateAccessToken",
		bytes.NewBufferString(`{"scope":["https://www.googleapis.com/auth/cloud-platform"],"lifetime":"300s"}`),
	)
	setBearer(t, request, issuer, "user:developer@example.com")
	response = httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || bytes.Contains(response.Body.Bytes(), []byte(body.AccessToken)) {
		t.Fatalf("denied response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestGenerateAccessTokenRejectsInvalidLifetimeAndScope(t *testing.T) {
	api := &API{iam: &fakeIAM{
		allow: true, issuer: localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now),
		accounts: accountSet("worker@example.iam.gserviceaccount.com"),
	}}
	for _, test := range []struct {
		body string
		code int
	}{
		{`{"scope":["scope"],"lifetime":"7200s"}`, http.StatusBadRequest},
		{`{"scope":[]}`, http.StatusBadRequest},
	} {
		request := httptest.NewRequest(http.MethodPost,
			"/v1/projects/-/serviceAccounts/worker@example.iam.gserviceaccount.com:generateAccessToken",
			bytes.NewBufferString(test.body))
		request.Header.Set("X-MiniSky-Principal", "user:developer@example.com")
		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)
		if response.Code != test.code {
			t.Fatalf("body=%s status=%d want=%d response=%s", test.body, response.Code, test.code, response.Body.String())
		}
	}
}

func TestGenerateAccessTokenAuthorizesOrderedDelegateChains(t *testing.T) {
	const caller = "principal://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/subject/repo"
	for _, count := range []int{0, 1, 4} {
		t.Run(string(rune('0'+count))+" delegates", func(t *testing.T) {
			issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
			delegates := make([]string, count)
			identifiers := []string{"target@example.com"}
			for index := range delegates {
				identifier := "delegate" + string(rune('1'+index)) + "@example.com"
				identifiers = append(identifiers, identifier)
				delegates[index] = "projects/-/serviceAccounts/" + identifier
			}
			fake := &fakeIAM{strict: true, issuer: issuer, accounts: accountSet(identifiers...),
				allowed: make(map[string]bool)}
			principal := caller
			for _, delegate := range delegates {
				resource := delegate
				fake.allowed[principal+"->"+resource] = true
				principal = "serviceAccount:" + strings.TrimPrefix(delegate, "projects/-/serviceAccounts/")
			}
			fake.allowed[principal+"->projects/-/serviceAccounts/target@example.com"] = true
			api := &API{iam: fake}
			body, err := json.Marshal(map[string]any{"scope": []string{"scope"}, "delegates": delegates})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost,
				"/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken", bytes.NewReader(body))
			setBearer(t, request, issuer, caller)
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s calls=%#v", response.Code, response.Body.String(), fake.calls)
			}
			if len(fake.calls) != count+1 {
				t.Fatalf("authorization calls=%#v", fake.calls)
			}
		})
	}
}

func TestGenerateAccessTokenRejectsMissingOrWrongOrderEdge(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	fake := &fakeIAM{
		strict: true, issuer: issuer, accounts: accountSet("first@example.com", "second@example.com", "target@example.com"),
		allowed: map[string]bool{
			"user:caller@example.com->projects/-/serviceAccounts/first@example.com": true,
			// This grants the reverse order only and must not authorize the requested chain.
			"serviceAccount:second@example.com->projects/-/serviceAccounts/first@example.com": true,
		},
	}
	api := &API{iam: fake}
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken",
		bytes.NewBufferString(`{"scope":["scope"],"delegates":["projects/-/serviceAccounts/first@example.com","projects/-/serviceAccounts/second@example.com"]}`))
	setBearer(t, request, issuer, "user:caller@example.com")
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestGenerateAccessTokenRejectsInvalidDelegateChains(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	api := &API{iam: &fakeIAM{issuer: issuer, allow: true, accounts: accountSet(
		"target@example.com", "one@example.com", "two@example.com", "three@example.com", "four@example.com", "five@example.com",
	)}}
	tests := []struct {
		name string
		body string
		code int
	}{
		{name: "five delegates", body: `{"scope":["scope"],"delegates":["projects/-/serviceAccounts/one@example.com","projects/-/serviceAccounts/two@example.com","projects/-/serviceAccounts/three@example.com","projects/-/serviceAccounts/four@example.com","projects/-/serviceAccounts/five@example.com"]}`, code: http.StatusNotImplemented},
		{name: "duplicate", body: `{"scope":["scope"],"delegates":["projects/-/serviceAccounts/one@example.com","projects/-/serviceAccounts/one@example.com"]}`, code: http.StatusBadRequest},
		{name: "target cycle", body: `{"scope":["scope"],"delegates":["projects/-/serviceAccounts/target@example.com"]}`, code: http.StatusBadRequest},
		{name: "malformed project", body: `{"scope":["scope"],"delegates":["projects/demo/serviceAccounts/one@example.com"]}`, code: http.StatusBadRequest},
		{name: "malformed identifier", body: `{"scope":["scope"],"delegates":["projects/-/serviceAccounts/not-an-account"]}`, code: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost,
				"/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken", bytes.NewBufferString(test.body))
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != test.code {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.code, response.Body.String())
			}
		})
	}
}

func TestGenerateAccessTokenChecksTargetAndDelegateState(t *testing.T) {
	for _, test := range []struct {
		name     string
		accounts map[string]fakeAccount
		body     string
		code     int
	}{
		{name: "missing target", accounts: accountSet("delegate@example.com"), body: `{"scope":["scope"]}`, code: http.StatusNotFound},
		{name: "disabled target", accounts: map[string]fakeAccount{"target@example.com": {email: "target@example.com", disabled: true}}, body: `{"scope":["scope"]}`, code: http.StatusPreconditionFailed},
		{name: "missing delegate", accounts: accountSet("target@example.com"), body: `{"scope":["scope"],"delegates":["projects/-/serviceAccounts/delegate@example.com"]}`, code: http.StatusNotFound},
		{name: "disabled delegate", accounts: map[string]fakeAccount{
			"target@example.com":   {email: "target@example.com"},
			"delegate@example.com": {email: "delegate@example.com", disabled: true},
		}, body: `{"scope":["scope"],"delegates":["projects/-/serviceAccounts/delegate@example.com"]}`, code: http.StatusPreconditionFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			api := &API{iam: &fakeIAM{
				issuer: localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now),
				allow:  true, accounts: test.accounts,
			}}
			request := httptest.NewRequest(http.MethodPost,
				"/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken", bytes.NewBufferString(test.body))
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != test.code {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.code, response.Body.String())
			}
		})
	}
}

func TestGenerateAccessTokenRejectsHeaderConflictAndRedactsTokens(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	fake := &fakeIAM{strict: true, issuer: issuer, allow: false, accounts: accountSet("target@example.com")}
	api := &API{iam: fake}
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken",
		bytes.NewBufferString(`{"scope":["scope"]}`))
	token := setBearer(t, request, issuer, "user:caller@example.com")
	request.Header.Set("X-MiniSky-Principal", "user:attacker@example.com")
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || bytes.Contains(response.Body.Bytes(), []byte(token)) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost,
		"/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken",
		bytes.NewBufferString(`{"scope":["scope"]}`))
	token = setBearer(t, request, issuer, "user:caller@example.com")
	response = httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || bytes.Contains(response.Body.Bytes(), []byte(token)) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodPost,
		"/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken",
		bytes.NewBufferString(`{"scope":["scope"]}`))
	request.Header.Set("X-MiniSky-Principal", "user:caller@example.com")
	response = httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("header-only status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestGenerateAccessTokenPermissiveDirectHandlerCompatibility(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	api := &API{iam: &fakeIAM{issuer: issuer, allow: true, accounts: accountSet("target@example.com")}}
	for _, bearer := range []string{"", "dummy-provider-bearer"} {
		t.Run(bearer, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost,
				"/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken",
				bytes.NewBufferString(`{"scope":["scope"]}`))
			request.Header.Set("X-MiniSky-Principal", "user:legacy@example.com")
			if bearer != "" {
				request.Header.Set("Authorization", "Bearer "+bearer)
			}
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestGenerateAccessTokenUsesConfiguredAudience(t *testing.T) {
	const audience = "https://gateway.minisky.test"
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	api := &API{iam: &fakeIAM{
		audience: audience, issuer: issuer, allow: true, accounts: accountSet("target@example.com"),
	}}
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/-/serviceAccounts/target@example.com:generateAccessToken",
		bytes.NewBufferString(`{"scope":["scope"]}`))
	request.Header.Set("Authorization", "Bearer provider-credential")
	response := httptest.NewRecorder()

	api.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, err := issuer.Verify(body.AccessToken, localsecurity.VerifyOptions{
		Audience: audience, RequiredScope: "scope",
	}); err != nil {
		t.Fatalf("issued token does not use configured audience: %v", err)
	}
}

func TestOtherIAMCredentialMethodsRemainUnimplemented(t *testing.T) {
	api := &API{}
	for _, method := range []string{"generateIdToken", "signJwt", "signBlob"} {
		request := httptest.NewRequest(http.MethodPost,
			"/v1/projects/-/serviceAccounts/target@example.com:"+method, bytes.NewBufferString(`{}`))
		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)
		if response.Code != http.StatusNotImplemented ||
			!bytes.Contains(response.Body.Bytes(), []byte(`"UNIMPLEMENTED"`)) {
			t.Fatalf("%s status=%d body=%s", method, response.Code, response.Body.String())
		}
	}
}

func accountSet(identifiers ...string) map[string]fakeAccount {
	accounts := make(map[string]fakeAccount, len(identifiers))
	for _, identifier := range identifiers {
		accounts[identifier] = fakeAccount{email: identifier}
	}
	return accounts
}

func setBearer(t *testing.T, request *http.Request, issuer *localsecurity.Issuer, subject string) string {
	t.Helper()
	token, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: subject, Audience: "minisky-gateway",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	return token
}
