package iamcredentials

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/router"
	localsecurity "minisky/pkg/security"
	_ "minisky/pkg/shims/accesscontextmanager"
	_ "minisky/pkg/shims/iam"
)

type fakeIAM struct {
	strict   bool
	allow    bool
	audience string
	allowed  map[string]bool
	accounts map[string]fakeAccount
	issuer   *localsecurity.Issuer
	calls    []authorizationCall
	events   *[]string
	issues   int
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
	if f.events != nil {
		*f.events = append(*f.events, "authorize:"+principal+"->"+resource)
	}
	f.calls = append(f.calls, authorizationCall{resource: resource, principal: principal})
	if f.allowed != nil {
		return f.allowed[principal+"->"+resource]
	}
	return f.allow
}
func (f *fakeIAM) VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error) {
	if f.events != nil {
		*f.events = append(*f.events, "authenticate")
	}
	return f.issuer.Verify(token, localsecurity.VerifyOptions{Audience: audience, RequiredScope: scope})
}
func (f *fakeIAM) ResolveServiceAccount(identifier string) (string, bool, bool) {
	if f.events != nil {
		*f.events = append(*f.events, "resolve:"+identifier)
	}
	account, ok := f.accounts[identifier]
	return account.email, account.disabled, ok
}
func (f *fakeIAM) IssueLocalToken(subject, audience string, scopes []string, lifetime time.Duration) (string, time.Time, error) {
	f.issues++
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

type recordingCredentialPerimeter struct {
	decisions map[string][2]bool
	fallback  [2]bool
	projects  []string
	events    *[]string
}

func (e *recordingCredentialPerimeter) EvaluateServicePerimeter(project, service, _, _ string) (bool, bool) {
	e.projects = append(e.projects, project)
	if e.events != nil {
		*e.events = append(*e.events, "perimeter:"+project+":"+service)
	}
	if decision, ok := e.decisions[project]; ok {
		return decision[0], decision[1]
	}
	return e.fallback[0], e.fallback[1]
}

type fixedCredentialPerimeter struct{}

func (fixedCredentialPerimeter) EvaluateServicePerimeter(_, _, _, _ string) (bool, bool) {
	return true, false
}

func TestConfigureServicePerimetersIsSafeDuringServing(t *testing.T) {
	const target = "worker@valid-project.iam.gserviceaccount.com"
	api := &API{iam: &fakeIAM{
		issuer:   localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now),
		allow:    true,
		accounts: accountSet(target),
	}}
	api.ConfigureServicePerimeters(fixedCredentialPerimeter{})

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			api.ConfigureServicePerimeters(fixedCredentialPerimeter{})
		}()
		go func() {
			defer wg.Done()
			request := httptest.NewRequest(
				http.MethodPost,
				"/v1/projects/-/serviceAccounts/"+target+":generateAccessToken",
				bytes.NewBufferString(`{"scope":["scope"]}`),
			)
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != http.StatusForbidden {
				t.Errorf("status=%d body=%s", response.Code, response.Body.String())
			}
		}()
	}
	wg.Wait()
}

func TestPostBootWiresIAMAndAccessContextManager(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "iamcredentials-postboot")
	t.Setenv(registry.ExperimentalServicesEnv, "1")
	handlers, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	api, ok := handlers["iamcredentials.googleapis.com"].(*API)
	if !ok {
		t.Fatalf("IAM Credentials handler=%T", handlers["iamcredentials.googleapis.com"])
	}
	if api.iam == nil {
		t.Fatal("IAM dependency was not wired")
	}
	wantPerimeters, ok := handlers["accesscontextmanager.googleapis.com"].(servicePerimeterEvaluator)
	if !ok {
		t.Fatalf("Access Context Manager handler=%T", handlers["accesscontextmanager.googleapis.com"])
	}
	if api.perimeters != wantPerimeters {
		t.Fatalf("perimeter dependency=%T want=%T", api.perimeters, wantPerimeters)
	}
}

func TestGenerateAccessTokenEvaluatesPerimetersAfterFullStrictAuthorization(t *testing.T) {
	const (
		caller        = "user:caller@example.com"
		targetID      = "100000000000000000001"
		delegateID    = "200000000000000000002"
		targetEmail   = "minisky-target@target-project.iam.gserviceaccount.com"
		delegateEmail = "minisky-delegate@delegate-project.iam.gserviceaccount.com"
	)
	events := []string{}
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	fake := &fakeIAM{
		strict: true,
		issuer: issuer,
		accounts: map[string]fakeAccount{
			targetID:   {email: targetEmail},
			delegateID: {email: delegateEmail},
		},
		allowed: map[string]bool{
			caller + "->projects/-/serviceAccounts/" + delegateEmail:                          true,
			"serviceAccount:" + delegateEmail + "->projects/-/serviceAccounts/" + targetEmail: true,
		},
		events: &events,
	}
	evaluator := &recordingCredentialPerimeter{
		decisions: map[string][2]bool{"projects/target-project": {true, false}},
		events:    &events,
	}
	api := &API{iam: fake, perimeters: evaluator}
	gateway := router.NewProxyRouterWithManager(nil)
	gateway.RegisterShim("iamcredentials.googleapis.com", api)
	gateway.ConfigureSecurity(fake, nil, false, "minisky-gateway")
	request := httptest.NewRequest(
		http.MethodPost,
		"http://127.0.0.1/_minisky/iamcredentials/v1/projects/-/serviceAccounts/"+targetID+":generateAccessToken",
		bytes.NewBufferString(`{"scope":["scope"],"delegates":["projects/-/serviceAccounts/`+delegateID+`"]}`),
	)
	setBearer(t, request, issuer, caller)
	response := httptest.NewRecorder()

	gateway.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden ||
		!strings.Contains(response.Body.String(), "Request is prohibited by VPC Service Controls") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	want := []string{
		"authenticate",
		"authenticate",
		"resolve:" + targetID,
		"resolve:" + delegateID,
		"authorize:" + caller + "->projects/-/serviceAccounts/" + delegateEmail,
		"authorize:serviceAccount:" + delegateEmail + "->projects/-/serviceAccounts/" + targetEmail,
		"perimeter:projects/target-project:iamcredentials.googleapis.com",
		"perimeter:projects/delegate-project:iamcredentials.googleapis.com",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%v want=%v", events, want)
	}
}

func TestGenerateAccessTokenPerimeterDecisionMatrix(t *testing.T) {
	const (
		target      = "minisky-target@project-one.iam.gserviceaccount.com"
		delegateOne = "minisky-delegate-one@project-one.iam.gserviceaccount.com"
		delegateTwo = "minisky-delegate-two@project-two.iam.gserviceaccount.com"
	)
	tests := []struct {
		name       string
		decisions  map[string][2]bool
		wantStatus int
	}{
		{
			name: "unconfigured remains allowed",
			decisions: map[string][2]bool{
				"projects/project-one": {false, false},
				"projects/project-two": {false, false},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "configured project denial",
			decisions: map[string][2]bool{
				"projects/project-one": {true, true},
				"projects/project-two": {true, false},
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "persistence failure denial",
			decisions: map[string][2]bool{
				"projects/project-one": {true, false},
				"projects/project-two": {true, false},
			},
			wantStatus: http.StatusForbidden,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
			iam := &fakeIAM{
				issuer: issuer,
				allow:  true,
				accounts: accountSet(
					target,
					delegateOne,
					delegateTwo,
				),
			}
			evaluator := &recordingCredentialPerimeter{decisions: test.decisions}
			api := &API{iam: iam, perimeters: evaluator}
			request := httptest.NewRequest(
				http.MethodPost,
				"/v1/projects/-/serviceAccounts/"+target+":generateAccessToken",
				bytes.NewBufferString(`{"scope":["scope"],"delegates":[`+
					`"projects/-/serviceAccounts/`+delegateOne+`",`+
					`"projects/-/serviceAccounts/`+delegateTwo+`"]}`),
			)
			response := httptest.NewRecorder()

			api.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.wantStatus, response.Body.String())
			}
			if want := []string{"projects/project-one", "projects/project-two"}; !reflect.DeepEqual(evaluator.projects, want) {
				t.Fatalf("perimeter projects=%v want=%v", evaluator.projects, want)
			}
			if test.wantStatus == http.StatusForbidden {
				if !strings.Contains(response.Body.String(), `"status":"PERMISSION_DENIED"`) ||
					!strings.Contains(response.Body.String(), "Request is prohibited by VPC Service Controls") {
					t.Fatalf("unexpected perimeter denial body=%s", response.Body.String())
				}
			}
		})
	}
}

func TestProjectFromCanonicalServiceAccountEmailEnforcesProjectBoundaries(t *testing.T) {
	thirtyCharacters := "a" + strings.Repeat("b", 29)
	tests := []struct {
		name    string
		project string
		wantOK  bool
	}{
		{name: "five characters", project: "abcde"},
		{name: "six characters", project: "abcdef", wantOK: true},
		{name: "thirty characters", project: thirtyCharacters, wantOK: true},
		{name: "thirty one characters", project: thirtyCharacters + "c"},
		{name: "uppercase", project: "abcdeF"},
		{name: "leading digit", project: "1abcde"},
		{name: "trailing hyphen", project: "abcde-"},
		{name: "numeric positive", project: "1", wantOK: true},
		{name: "numeric zero", project: "0"},
		{name: "numeric leading zero", project: "01"},
		{name: "numeric uint64 maximum", project: "18446744073709551615", wantOK: true},
		{name: "numeric uint64 overflow", project: "18446744073709551616"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			email := "worker@" + test.project + ".iam.gserviceaccount.com"
			project, ok := projectFromCanonicalServiceAccountEmail(email)
			if ok != test.wantOK {
				t.Fatalf("project=%q ok=%t wantOK=%t", project, ok, test.wantOK)
			}
			if ok && project != test.project {
				t.Fatalf("project=%q want=%q", project, test.project)
			}
		})
	}
}

func TestMalformedResolvedProjectDomainsFailClosedBeforeEvaluationAndIssuance(t *testing.T) {
	const (
		targetID      = "100000000000000000001"
		delegateID    = "200000000000000000002"
		validTarget   = "target@valid-project.iam.gserviceaccount.com"
		validDelegate = "delegate@other-project.iam.gserviceaccount.com"
	)
	tests := []struct {
		name         string
		targetEmail  string
		delegateMail string
	}{
		{name: "short target project", targetEmail: "target@short.iam.gserviceaccount.com", delegateMail: validDelegate},
		{name: "long target project", targetEmail: "target@a" + strings.Repeat("b", 30) + ".iam.gserviceaccount.com", delegateMail: validDelegate},
		{name: "uppercase target project", targetEmail: "target@Invalid-project.iam.gserviceaccount.com", delegateMail: validDelegate},
		{name: "leading-zero numeric target project", targetEmail: "target@012345.iam.gserviceaccount.com", delegateMail: validDelegate},
		{name: "malformed delegate project", targetEmail: validTarget, delegateMail: "delegate@bad_.iam.gserviceaccount.com"},
	}
	for _, test := range tests {
		for _, configureEvaluator := range []bool{false, true} {
			name := "default off"
			if configureEvaluator {
				name = "configured evaluator"
			}
			t.Run(test.name+"/"+name, func(t *testing.T) {
				issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
				iam := &fakeIAM{
					issuer: issuer,
					allow:  true,
					accounts: map[string]fakeAccount{
						targetID:   {email: test.targetEmail},
						delegateID: {email: test.delegateMail},
					},
				}
				evaluator := &recordingCredentialPerimeter{fallback: [2]bool{true, true}}
				api := &API{iam: iam}
				if configureEvaluator {
					api.ConfigureServicePerimeters(evaluator)
				}
				request := httptest.NewRequest(
					http.MethodPost,
					"/v1/projects/-/serviceAccounts/"+targetID+":generateAccessToken",
					bytes.NewBufferString(`{"scope":["scope"],"delegates":["projects/-/serviceAccounts/`+delegateID+`"]}`),
				)
				response := httptest.NewRecorder()

				api.ServeHTTP(response, request)

				if response.Code != http.StatusForbidden ||
					!strings.Contains(response.Body.String(), "Request is prohibited by VPC Service Controls") {
					t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
				}
				if len(evaluator.projects) != 0 {
					t.Fatalf("malformed resolved email reached evaluator: %v", evaluator.projects)
				}
				if iam.issues != 0 {
					t.Fatalf("malformed resolved email issued %d tokens", iam.issues)
				}
			})
		}
	}
}

func TestGenerateAccessTokenRejectsInvalidDelegateAliasesBeforeSecurity(t *testing.T) {
	const target = "minisky-target@target-project.iam.gserviceaccount.com"
	tests := []struct {
		name     string
		delegate string
	}{
		{name: "encoded alias", delegate: "projects/-/serviceAccounts/minisky%40delegate-project.iam.gserviceaccount.com"},
		{name: "double encoded alias", delegate: "projects/-/serviceAccounts/minisky%2540delegate-project.iam.gserviceaccount.com"},
		{name: "encoded separator", delegate: "projects/-/serviceAccounts/minisky%2Fdelegate@delegate-project.iam.gserviceaccount.com"},
		{name: "control alias", delegate: "projects/-/serviceAccounts/minisky\n@delegate-project.iam.gserviceaccount.com"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := []string{}
			issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
			iam := &fakeIAM{
				strict:   true,
				issuer:   issuer,
				accounts: accountSet(target),
				events:   &events,
			}
			evaluator := &recordingCredentialPerimeter{events: &events}
			api := &API{iam: iam, perimeters: evaluator}
			body, err := json.Marshal(map[string]any{
				"scope":     []string{"scope"},
				"delegates": []string{test.delegate},
			})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(
				http.MethodPost,
				"/v1/projects/-/serviceAccounts/"+target+":generateAccessToken",
				bytes.NewReader(body),
			)
			response := httptest.NewRecorder()

			api.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if len(events) != 0 || len(iam.calls) != 0 || len(evaluator.projects) != 0 {
				t.Fatalf("invalid delegate triggered security decisions: events=%v auth=%v perimeter=%v",
					events, iam.calls, evaluator.projects)
			}
		})
	}
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

func TestGenerateAccessTokenRejectsOversizedBodyBeforeSecurity(t *testing.T) {
	const target = "minisky-target@target-project.iam.gserviceaccount.com"
	events := []string{}
	iam := &fakeIAM{
		issuer:   localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now),
		allow:    true,
		accounts: accountSet(target),
		events:   &events,
	}
	evaluator := &recordingCredentialPerimeter{events: &events}
	api := &API{iam: iam, perimeters: evaluator}
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/-/serviceAccounts/"+target+":generateAccessToken",
		strings.NewReader(`{"scope":["scope"],"padding":"`+strings.Repeat("x", (1<<20)+1)+`"}`),
	)
	response := httptest.NewRecorder()

	api.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if len(events) != 0 || len(evaluator.projects) != 0 {
		t.Fatalf("oversized body triggered security decisions: events=%v perimeter=%v", events, evaluator.projects)
	}
}

func TestGenerateAccessTokenAuthorizesOrderedDelegateChains(t *testing.T) {
	const caller = "principal://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/subject/repo"
	for _, count := range []int{0, 1, 4} {
		t.Run(string(rune('0'+count))+" delegates", func(t *testing.T) {
			issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
			delegates := make([]string, count)
			target := "target@example-project.iam.gserviceaccount.com"
			identifiers := []string{target}
			for index := range delegates {
				identifier := "delegate" + string(rune('1'+index)) +
					"@example-project.iam.gserviceaccount.com"
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
			fake.allowed[principal+"->projects/-/serviceAccounts/"+target] = true
			api := &API{iam: fake}
			body, err := json.Marshal(map[string]any{"scope": []string{"scope"}, "delegates": delegates})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost,
				"/v1/projects/-/serviceAccounts/"+target+":generateAccessToken", bytes.NewReader(body))
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
	evaluator := &recordingCredentialPerimeter{
		decisions: map[string][2]bool{"projects/example": {true, false}},
	}
	api := &API{iam: fake, perimeters: evaluator}
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
	if len(evaluator.projects) != 0 {
		t.Fatalf("unauthorized requests reached perimeter evaluator: %v", evaluator.projects)
	}
}

func TestGenerateAccessTokenPermissiveDirectHandlerCompatibility(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	const target = "target@example-project.iam.gserviceaccount.com"
	api := &API{iam: &fakeIAM{issuer: issuer, allow: true, accounts: accountSet(target)}}
	for _, bearer := range []string{"", "dummy-provider-bearer"} {
		t.Run(bearer, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost,
				"/v1/projects/-/serviceAccounts/"+target+":generateAccessToken",
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

func TestGenerateAccessTokenPermissiveAndStrictIAMThroughGateway(t *testing.T) {
	const target = "minisky-target@local-dev-project.iam.gserviceaccount.com"
	t.Run("permissive request is dispatched", func(t *testing.T) {
		issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
		iam := &fakeIAM{issuer: issuer, allow: true, accounts: accountSet(target)}
		evaluator := &recordingCredentialPerimeter{}
		gateway := router.NewProxyRouterWithManager(nil)
		gateway.RegisterShim("iamcredentials.googleapis.com", &API{iam: iam, perimeters: evaluator})

		request := httptest.NewRequest(
			http.MethodPost,
			"http://127.0.0.1/_minisky/iamcredentials/v1/projects/-/serviceAccounts/"+
				target+":generateAccessToken",
			bytes.NewBufferString(`{"scope":["scope"]}`),
		)
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
		if want := []string{"projects/local-dev-project"}; !reflect.DeepEqual(evaluator.projects, want) {
			t.Fatalf("perimeter projects=%v want=%v", evaluator.projects, want)
		}
	})

	t.Run("strict request reaches service-account authorization", func(t *testing.T) {
		issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
		iam := &fakeIAM{strict: true, issuer: issuer, allow: false, accounts: accountSet(target)}
		evaluator := &recordingCredentialPerimeter{
			decisions: map[string][2]bool{"projects/local-dev-project": {true, false}},
		}
		gateway := router.NewProxyRouterWithManager(nil)
		gateway.RegisterShim("iamcredentials.googleapis.com", &API{iam: iam, perimeters: evaluator})
		gateway.ConfigureSecurity(iam, nil, false, "minisky-gateway")

		request := httptest.NewRequest(
			http.MethodPost,
			"http://127.0.0.1/_minisky/iamcredentials/v1/projects/-/serviceAccounts/"+
				target+":generateAccessToken",
			bytes.NewBufferString(`{"scope":["scope"]}`),
		)
		setBearer(t, request, issuer, "user:caller@example.com")
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
		if len(iam.calls) != 1 || iam.calls[0].resource != "projects/-/serviceAccounts/"+target {
			t.Fatalf("authorization calls=%+v", iam.calls)
		}
		if len(evaluator.projects) != 0 ||
			strings.Contains(response.Body.String(), "VPC Service Controls") {
			t.Fatalf("unauthorized request revealed perimeter decision: calls=%v body=%s",
				evaluator.projects, response.Body.String())
		}
	})
}

func TestGenerateAccessTokenGatewayRejectsInvalidTargetAliases(t *testing.T) {
	const target = "minisky-target@local-dev-project.iam.gserviceaccount.com"
	tests := []struct {
		name   string
		target string
	}{
		{name: "invalid project segment", target: "projects/local-dev-project/serviceAccounts/" + target},
		{name: "malformed alias", target: "projects/-/serviceAccounts/not-an-account"},
		{
			name:   "encoded alias",
			target: "projects/-/serviceAccounts/minisky-target%40local-dev-project.iam.gserviceaccount.com",
		},
		{
			name:   "double encoded alias",
			target: "projects/-/serviceAccounts/minisky-target%2540local-dev-project.iam.gserviceaccount.com",
		},
		{
			name:   "encoded separator",
			target: "projects/-/serviceAccounts/minisky-target%2Flocal-dev-project.iam.gserviceaccount.com",
		},
		{
			name:   "encoded control",
			target: "projects/-/serviceAccounts/minisky-target%0Alocal-dev-project.iam.gserviceaccount.com",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := []string{}
			issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
			iam := &fakeIAM{issuer: issuer, allow: true, accounts: accountSet(target), events: &events}
			evaluator := &recordingCredentialPerimeter{events: &events}
			gateway := router.NewProxyRouterWithManager(nil)
			gateway.RegisterShim("iamcredentials.googleapis.com", &API{iam: iam, perimeters: evaluator})
			request := httptest.NewRequest(
				http.MethodPost,
				"http://127.0.0.1/_minisky/iamcredentials/v1/"+test.target+":generateAccessToken",
				bytes.NewBufferString(`{"scope":["scope"]}`),
			)
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if len(events) != 0 || len(iam.calls) != 0 || len(evaluator.projects) != 0 {
				t.Fatalf("invalid target triggered security decisions: events=%v auth=%v perimeter=%v",
					events, iam.calls, evaluator.projects)
			}
		})
	}
}

func TestGenerateAccessTokenUsesConfiguredAudience(t *testing.T) {
	const audience = "https://gateway.minisky.test"
	const target = "target@example-project.iam.gserviceaccount.com"
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	api := &API{iam: &fakeIAM{
		audience: audience, issuer: issuer, allow: true, accounts: accountSet(target),
	}}
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/-/serviceAccounts/"+target+":generateAccessToken",
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
