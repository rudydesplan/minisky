package sts

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	localsecurity "minisky/pkg/security"
)

type fakeIAM struct {
	issuer   *localsecurity.Issuer
	audience string
}

func (f fakeIAM) VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error) {
	return f.issuer.Verify(token, localsecurity.VerifyOptions{Audience: audience, RequiredScope: scope})
}
func (f fakeIAM) IssueLocalToken(subject, audience string, scopes []string, lifetime time.Duration) (string, time.Time, error) {
	token, claims, err := f.issuer.Issue(localsecurity.TokenRequest{Subject: subject, Audience: audience, Scopes: scopes, Lifetime: lifetime})
	return token, claims.ExpiresAt, err
}
func (f fakeIAM) TokenAudience() string {
	if f.audience != "" {
		return f.audience
	}
	return "minisky-gateway"
}

func TestLocalTokenExchangeAndUnsupportedProvider(t *testing.T) {
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	subject, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "serviceAccount:external@example.com", Audience: "minisky-sts",
		Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	api := &API{iam: fakeIAM{issuer: issuer}}
	form := url.Values{
		"grant_type":           {tokenExchangeGrant},
		"requested_token_type": {accessTokenType},
		"subject_token_type":   {accessTokenType},
		"subject_token":        {subject},
		"audience":             {"minisky-sts"},
		"scope":                {"https://www.googleapis.com/auth/cloud-platform"},
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/token", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"access_token"`) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}

	form.Set("subject_token_type", "urn:ietf:params:oauth:token-type:jwt")
	request = httptest.NewRequest(http.MethodPost, "/v1/token", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response = httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest ||
		!strings.Contains(response.Body.String(), `"error":"invalid_target"`) {
		t.Fatalf("unsupported status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestLocalTokenExchangeUsesConfiguredTargetAudience(t *testing.T) {
	const targetAudience = "https://gateway.minisky.test"
	issuer := localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now)
	subject, _, err := issuer.Issue(localsecurity.TokenRequest{
		Subject: "serviceAccount:external@example.com", Audience: "minisky-sts",
		Scopes: []string{"scope-one"}, Lifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	api := &API{iam: fakeIAM{issuer: issuer, audience: targetAudience}}
	form := url.Values{
		"grant_type":           {tokenExchangeGrant},
		"requested_token_type": {accessTokenType},
		"subject_token_type":   {accessTokenType},
		"subject_token":        {subject},
		"audience":             {"minisky-sts"},
		"scope":                {"scope-one"},
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/token", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()

	api.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		AccessToken string `json:"access_token"`
	}
	decodeSTSJSON(t, response, &body)
	if _, err := issuer.Verify(body.AccessToken, localsecurity.VerifyOptions{
		Audience: targetAudience, RequiredScope: "scope-one",
	}); err != nil {
		t.Fatalf("issued token does not use configured audience: %v", err)
	}
}
