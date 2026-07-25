package sts

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	localsecurity "minisky/pkg/security"
	iamshim "minisky/pkg/shims/iam"
)

const (
	testPoolName     = "projects/test-project/locations/global/workloadIdentityPools/github"
	testProviderName = testPoolName + "/providers/actions"
	testAudience     = "//iam.googleapis.com/" + testProviderName
)

type federatedIAM struct {
	issuer            *localsecurity.Issuer
	config            *iamshim.WorkloadIdentityProviderConfig
	audience          string
	acceptAnyAudience bool
}

func (f *federatedIAM) VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error) {
	return f.issuer.Verify(token, localsecurity.VerifyOptions{Audience: audience, RequiredScope: scope})
}

func (f *federatedIAM) IssueLocalToken(subject, audience string, scopes []string, lifetime time.Duration) (string, time.Time, error) {
	token, claims, err := f.issuer.Issue(localsecurity.TokenRequest{
		Subject: subject, Audience: audience, Scopes: scopes, Lifetime: lifetime,
	})
	return token, claims.ExpiresAt, err
}

func (f *federatedIAM) TokenAudience() string {
	if f.audience != "" {
		return f.audience
	}
	return "minisky-gateway"
}

func (f *federatedIAM) LookupWorkloadIdentityProvider(audience string) (*iamshim.WorkloadIdentityProviderConfig, bool) {
	if !f.acceptAnyAudience && audience != testAudience || f.config == nil {
		return nil, false
	}
	return f.config, true
}

func TestOIDCJWTExchange(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	key := mustRSAKey(t)
	fake := newFederatedIAM(t, key)
	fake.audience = "https://gateway.minisky.test"
	api := &API{iam: fake, wif: fake, now: func() time.Time { return now }}
	token := signJWT(t, key, "key-1", "RS256", map[string]any{
		"iss": "https://issuer.invalid",
		"aud": []string{"other", testAudience},
		"sub": "repo:minisky/project:ref:refs/heads/main",
		"exp": now.Add(30 * time.Minute).Unix(),
		"nbf": now.Add(-time.Minute).Unix(),
		"iat": now.Add(-time.Minute).Unix(),
	})

	response := exchangeJWT(t, api, testAudience, token)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	decodeSTSJSON(t, response, &body)
	if body.AccessToken == "" || body.ExpiresIn <= 0 || body.ExpiresIn > 3600 {
		t.Fatalf("response = %#v", body)
	}
	claims, err := fake.issuer.Verify(body.AccessToken, localsecurity.VerifyOptions{
		Audience:      fake.audience,
		RequiredScope: "scope-one",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(claims.Scopes) != 2 || claims.Scopes[1] != "scope-two" {
		t.Fatalf("scopes = %#v", claims.Scopes)
	}
	wantSubject := "principal://iam.googleapis.com/" + testPoolName + "/subject/repo:minisky%2Fproject:ref:refs%2Fheads%2Fmain"
	if claims.Subject != wantSubject {
		t.Fatalf("subject=%q want=%q", claims.Subject, wantSubject)
	}

	fake.config.Provider.OIDC.AllowedAudiences = []string{"configured-audience"}
	configuredToken := signJWT(t, key, "key-1", "RS256", map[string]any{
		"iss": "https://issuer.invalid", "aud": "configured-audience", "sub": "subject", "exp": now.Add(time.Hour).Unix(),
	})
	if configured := exchangeJWT(t, api, testAudience, configuredToken); configured.Code != http.StatusOK {
		t.Fatalf("configured audience: status=%d body=%s", configured.Code, configured.Body.String())
	}
}

func TestOIDCJWTExchangeRequiresScope(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	key := mustRSAKey(t)
	fake := newFederatedIAM(t, key)
	api := &API{iam: fake, wif: fake, now: func() time.Time { return now }}
	token := signJWT(t, key, "key-1", "RS256", map[string]any{
		"iss": "https://issuer.invalid", "aud": testAudience, "sub": "subject", "exp": now.Add(time.Hour).Unix(),
	})
	form := url.Values{
		"grant_type":           {tokenExchangeGrant},
		"requested_token_type": {accessTokenType},
		"subject_token_type":   {jwtTokenType},
		"subject_token":        {token},
		"audience":             {testAudience},
		"scope":                {" \t "},
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/token", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()

	api.ServeHTTP(response, request)

	assertSTSError(t, response, http.StatusBadRequest)
	var body struct {
		Error string `json:"error"`
	}
	decodeSTSJSON(t, response, &body)
	if body.Error != "invalid_request" {
		t.Fatalf("error=%q body=%s", body.Error, response.Body.String())
	}
}

func TestOIDCJWTExchangeRejectsNumericProjectAudienceBeforeLookup(t *testing.T) {
	key := mustRSAKey(t)
	fake := newFederatedIAM(t, key)
	numericAudience := "//iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/github/providers/actions"
	fake.acceptAnyAudience = true
	api := &API{iam: fake, wif: fake}

	response := exchangeJWT(t, api, numericAudience, "not-a-token")
	assertSTSError(t, response, http.StatusBadRequest)
	var body struct {
		Error string `json:"error"`
	}
	decodeSTSJSON(t, response, &body)
	if body.Error != "invalid_target" {
		t.Fatalf("error=%q body=%s", body.Error, response.Body.String())
	}
}

func TestOIDCJWTExchangeRejectsInvalidTokens(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	key := mustRSAKey(t)
	otherKey := mustRSAKey(t)
	baseClaims := map[string]any{
		"iss": "https://issuer.invalid",
		"aud": testAudience,
		"sub": "subject",
		"exp": now.Add(time.Hour).Unix(),
	}
	tests := []struct {
		name  string
		token func() string
	}{
		{name: "wrong signature", token: func() string { return signJWT(t, otherKey, "key-1", "RS256", baseClaims) }},
		{name: "wrong kid", token: func() string { return signJWT(t, key, "other", "RS256", baseClaims) }},
		{name: "wrong alg", token: func() string { return signJWT(t, key, "key-1", "RS512", baseClaims) }},
		{name: "wrong issuer", token: func() string {
			return signJWT(t, key, "key-1", "RS256", withClaim(baseClaims, "iss", "https://other.invalid"))
		}},
		{name: "wrong audience", token: func() string {
			return signJWT(t, key, "key-1", "RS256", withClaim(baseClaims, "aud", "other"))
		}},
		{name: "expired", token: func() string {
			return signJWT(t, key, "key-1", "RS256", withClaim(baseClaims, "exp", now.Add(-time.Minute).Unix()))
		}},
		{name: "not yet valid", token: func() string {
			return signJWT(t, key, "key-1", "RS256", withClaim(baseClaims, "nbf", now.Add(time.Minute).Unix()))
		}},
		{name: "future issued at", token: func() string {
			return signJWT(t, key, "key-1", "RS256", withClaim(baseClaims, "iat", now.Add(time.Minute).Unix()))
		}},
		{name: "fractional expiry", token: func() string {
			return signJWT(t, key, "key-1", "RS256", withClaim(baseClaims, "exp", 1800000000.5))
		}},
		{name: "missing expiry", token: func() string {
			return signJWT(t, key, "key-1", "RS256", withoutClaim(baseClaims, "exp"))
		}},
		{name: "empty subject", token: func() string {
			return signJWT(t, key, "key-1", "RS256", withClaim(baseClaims, "sub", ""))
		}},
		{name: "oversized subject", token: func() string {
			return signJWT(t, key, "key-1", "RS256", withClaim(baseClaims, "sub", strings.Repeat("s", maxSubjectBytes+1)))
		}},
		{name: "malformed", token: func() string { return "not.a.jwt.with-four-segments" }},
		{name: "oversized", token: func() string { return strings.Repeat("x", maxSubjectTokenBytes+1) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := newFederatedIAM(t, key)
			api := &API{iam: fake, wif: fake, now: func() time.Time { return now }}
			assertSTSError(t, exchangeJWT(t, api, testAudience, test.token()), http.StatusBadRequest)
		})
	}
}

func TestOIDCJWTExchangeRejectsInvalidJWKSBounds(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	key := mustRSAKey(t)
	token := signJWT(t, key, "key-1", "RS256", map[string]any{
		"iss": "https://issuer.invalid", "aud": testAudience, "sub": "subject", "exp": now.Add(time.Hour).Unix(),
	})
	manyKeys := make([]any, maxJWKCount+1)
	for index := range manyKeys {
		manyKeys[index] = map[string]any{}
	}
	encodedManyKeys, err := json.Marshal(map[string]any{"keys": manyKeys})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		jwks string
	}{
		{name: "malformed", jwks: `{"keys":`},
		{name: "oversized", jwks: strings.Repeat("x", maxJWKSBytes+1)},
		{name: "too many keys", jwks: string(encodedManyKeys)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := newFederatedIAM(t, key)
			fake.config.Provider.OIDC.JWKSJSON = test.jwks
			api := &API{iam: fake, wif: fake, now: func() time.Time { return now }}
			assertSTSError(t, exchangeJWT(t, api, testAudience, token), http.StatusBadRequest)
		})
	}
}

func TestOIDCJWTExchangeRejectsUnavailableOrUnsupportedProviders(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	key := mustRSAKey(t)
	validToken := signJWT(t, key, "key-1", "RS256", map[string]any{
		"iss": "https://issuer.invalid", "aud": testAudience, "sub": "subject", "exp": now.Add(time.Hour).Unix(),
	})
	tests := []struct {
		name   string
		mutate func(*federatedIAM)
	}{
		{name: "deleted provider", mutate: func(f *federatedIAM) { f.config = nil }},
		{name: "disabled pool", mutate: func(f *federatedIAM) { f.config.Pool.Disabled = true }},
		{name: "deleted pool", mutate: func(f *federatedIAM) { f.config.Pool.State = "DELETED" }},
		{name: "disabled provider", mutate: func(f *federatedIAM) { f.config.Provider.Disabled = true }},
		{name: "deleted provider state", mutate: func(f *federatedIAM) { f.config.Provider.State = "DELETED" }},
		{name: "condition", mutate: func(f *federatedIAM) { f.config.Provider.AttributeCondition = "assertion.sub == 'subject'" }},
		{name: "mapping", mutate: func(f *federatedIAM) { f.config.Provider.AttributeMapping["attribute.extra"] = "assertion.extra" }},
		{name: "mapping expression", mutate: func(f *federatedIAM) { f.config.Provider.AttributeMapping["google.subject"] = "assertion.email" }},
		{name: "aws kind", mutate: func(f *federatedIAM) { f.config.Provider.AWS = map[string]any{"accountId": "123"} }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := newFederatedIAM(t, key)
			test.mutate(fake)
			api := &API{iam: fake, wif: fake, now: func() time.Time { return now }}
			response := exchangeJWT(t, api, testAudience, validToken)
			assertSTSError(t, response, http.StatusBadRequest)
			if strings.Contains(response.Body.String(), validToken) {
				t.Fatal("error echoed subject token")
			}
		})
	}
}

func newFederatedIAM(t *testing.T, key *rsa.PrivateKey) *federatedIAM {
	t.Helper()
	modulus := base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes())
	exponent := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes())
	jwks, err := json.Marshal(map[string]any{"keys": []any{map[string]any{
		"kty": "RSA", "kid": "key-1", "alg": "RS256", "use": "sig", "n": modulus, "e": exponent,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return &federatedIAM{
		issuer: localsecurity.NewIssuer([]byte("01234567890123456789012345678901"), time.Now),
		config: &iamshim.WorkloadIdentityProviderConfig{
			Pool: &iamshim.WorkloadIdentityPool{Name: testPoolName, State: "ACTIVE"},
			Provider: &iamshim.WorkloadIdentityPoolProvider{
				Name: testProviderName, State: "ACTIVE",
				AttributeMapping: map[string]string{"google.subject": "assertion.sub"},
				OIDC: &iamshim.WorkloadIdentityPoolOIDC{
					IssuerURI: "https://issuer.invalid", JWKSJSON: string(jwks),
				},
			},
		},
	}
}

func exchangeJWT(t *testing.T, api *API, audience, token string) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{
		"grant_type":           {tokenExchangeGrant},
		"requested_token_type": {accessTokenType},
		"subject_token_type":   {jwtTokenType},
		"subject_token":        {token},
		"audience":             {audience},
		"scope":                {"scope-one scope-two"},
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/token", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}

func assertSTSError(t *testing.T, response *httptest.ResponseRecorder, status int) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	decodeSTSJSON(t, response, &body)
	if body.Error == "" || body.ErrorDescription == "" {
		t.Fatalf("not an OAuth STS error: %s", response.Body.String())
	}
}

func decodeSTSJSON(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode %q: %v", response.Body.String(), err)
	}
}

func mustRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func signJWT(t *testing.T, key *rsa.PrivateKey, kid, alg string, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": alg, "kid": kid, "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := key.Sign(rand.Reader, digest[:], crypto.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func withClaim(source map[string]any, key string, value any) map[string]any {
	result := make(map[string]any, len(source)+1)
	for name, claim := range source {
		result[name] = claim
	}
	result[key] = value
	return result
}

func withoutClaim(source map[string]any, omitted string) map[string]any {
	result := make(map[string]any, len(source)-1)
	for name, claim := range source {
		if name != omitted {
			result[name] = claim
		}
	}
	return result
}
