package iamcredentials

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	localsecurity "minisky/pkg/security"
)

type fakeIAM struct {
	allow  bool
	issuer *localsecurity.Issuer
}

func (f *fakeIAM) EnforcementEnabled() bool { return true }
func (f *fakeIAM) Authorize(string, string, string) bool {
	return f.allow
}
func (f *fakeIAM) IssueLocalToken(subject, audience string, scopes []string, lifetime time.Duration) (string, time.Time, error) {
	token, claims, err := f.issuer.Issue(localsecurity.TokenRequest{
		Subject: subject, Audience: audience, Scopes: scopes, Lifetime: lifetime,
	})
	return token, claims.ExpiresAt, err
}

func TestGenerateAccessTokenContractAndAuthorization(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	api := &API{iam: &fakeIAM{allow: true, issuer: issuer}}
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

	api.iam = &fakeIAM{allow: false, issuer: issuer}
	request = httptest.NewRequest(
		http.MethodPost,
		"/v1/projects/-/serviceAccounts/worker@example.iam.gserviceaccount.com:generateAccessToken",
		bytes.NewBufferString(`{"scope":["https://www.googleapis.com/auth/cloud-platform"],"lifetime":"300s"}`),
	)
	request.Header.Set("X-MiniSky-Principal", "user:developer@example.com")
	response = httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || bytes.Contains(response.Body.Bytes(), []byte(body.AccessToken)) {
		t.Fatalf("denied response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestGenerateAccessTokenRejectsUnsupportedDelegationAndInvalidLifetime(t *testing.T) {
	api := &API{iam: &fakeIAM{
		allow: true, issuer: localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now),
	}}
	for _, test := range []struct {
		body string
		code int
	}{
		{`{"scope":["scope"],"delegates":["projects/-/serviceAccounts/delegate@example.com"]}`, http.StatusNotImplemented},
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
