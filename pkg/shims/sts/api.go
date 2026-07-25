package sts

import (
	"bytes"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"minisky/pkg/registry"
	localsecurity "minisky/pkg/security"
	iamshim "minisky/pkg/shims/iam"
)

const (
	tokenExchangeGrant   = "urn:ietf:params:oauth:grant-type:token-exchange"
	accessTokenType      = "urn:ietf:params:oauth:token-type:access_token"
	jwtTokenType         = "urn:ietf:params:oauth:token-type:jwt"
	maxSubjectTokenBytes = 32 << 10
	maxJWKSBytes         = 64 << 10
	maxJWKCount          = 32
	maxSubjectBytes      = 256

	// OIDC providers commonly tolerate a small amount of clock drift. MiniSky
	// applies this only to optional nbf/iat checks; exp must still be in future.
	oidcClockSkew = 30 * time.Second
)

type iamService interface {
	VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error)
	IssueLocalToken(subject, audience string, scopes []string, lifetime time.Duration) (string, time.Time, error)
	TokenAudience() string
}

type wifProviderLookup interface {
	LookupWorkloadIdentityProvider(audience string) (*iamshim.WorkloadIdentityProviderConfig, bool)
}

type API struct {
	iam iamService
	wif wifProviderLookup
	now func() time.Time
}

func init() {
	registry.Register("sts.googleapis.com", func(*registry.Context) http.Handler { return &API{} })
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	iam := ctx.GetShim("iam.googleapis.com")
	if service, ok := iam.(iamService); ok {
		api.iam = service
	}
	if lookup, ok := iam.(wifProviderLookup); ok {
		api.wif = lookup
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost || r.URL.Path != "/v1/token" {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Only token exchange is supported")
		return
	}
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		writeSTSError(w, http.StatusUnsupportedMediaType, "invalid_request", "form-encoded request required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := r.ParseForm(); err != nil {
		writeSTSError(w, http.StatusBadRequest, "invalid_request", "invalid token exchange request")
		return
	}
	if r.Form.Get("grant_type") != tokenExchangeGrant ||
		r.Form.Get("requested_token_type") != accessTokenType {
		writeSTSError(w, http.StatusBadRequest, "invalid_request", "requested STS grant or token type is unsupported")
		return
	}
	if api.iam == nil {
		writeSTSError(w, http.StatusServiceUnavailable, "temporarily_unavailable", "local credential issuer unavailable")
		return
	}
	switch r.Form.Get("subject_token_type") {
	case accessTokenType:
		api.exchangeLocalToken(w, r)
	case jwtTokenType:
		api.exchangeOIDCToken(w, r)
	default:
		writeSTSError(w, http.StatusBadRequest, "invalid_request", "subject token type is unsupported")
	}
}

func (api *API) exchangeLocalToken(w http.ResponseWriter, r *http.Request) {
	audience := r.Form.Get("audience")
	scope := r.Form.Get("scope")
	claims, err := api.iam.VerifyLocalToken(r.Form.Get("subject_token"), audience, scope)
	if err != nil {
		writeSTSError(w, http.StatusUnauthorized, "invalid_grant", "subject token is invalid")
		return
	}
	api.issueToken(w, claims.Subject, strings.Fields(scope))
}

func (api *API) exchangeOIDCToken(w http.ResponseWriter, r *http.Request) {
	scope := strings.TrimSpace(r.Form.Get("scope"))
	if scope == "" {
		writeSTSError(w, http.StatusBadRequest, "invalid_request", "scope is required for workload identity token exchange")
		return
	}
	if api.wif == nil {
		writeSTSError(w, http.StatusBadRequest, "invalid_target", "workload identity provider is unavailable")
		return
	}
	audience := r.Form.Get("audience")
	if !iamshim.ValidWorkloadIdentityProviderAudience(audience) {
		writeSTSError(w, http.StatusBadRequest, "invalid_target", "workload identity provider audience is invalid")
		return
	}
	config, ok := api.wif.LookupWorkloadIdentityProvider(audience)
	if !ok {
		writeSTSError(w, http.StatusBadRequest, "invalid_target", "workload identity provider was not found")
		return
	}
	if err := validateExecutableProvider(config); err != nil {
		writeSTSError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	subject, err := validateOIDCJWT(r.Form.Get("subject_token"), audience, config.Provider, api.currentTime())
	if err != nil {
		writeSTSError(w, http.StatusBadRequest, "invalid_grant", "subject token is invalid")
		return
	}
	// PathEscape preserves a readable principal while preventing a subject from
	// creating additional principal path segments.
	principal := "principal://iam.googleapis.com/" + config.Pool.Name + "/subject/" + url.PathEscape(subject)
	api.issueToken(w, principal, strings.Fields(scope))
}

func (api *API) issueToken(w http.ResponseWriter, subject string, scopes []string) {
	token, expires, err := api.iam.IssueLocalToken(subject, api.iam.TokenAudience(), scopes, time.Hour)
	if err != nil {
		writeSTSError(w, http.StatusBadRequest, "invalid_grant", "token exchange failed")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token":      token,
		"issued_token_type": accessTokenType,
		"token_type":        "Bearer",
		"expires_in":        int(time.Until(expires).Seconds()),
		"scope":             strings.Join(scopes, " "),
	})
}

func (api *API) currentTime() time.Time {
	if api.now != nil {
		return api.now()
	}
	return time.Now()
}

func validateExecutableProvider(config *iamshim.WorkloadIdentityProviderConfig) error {
	if config == nil || config.Pool == nil || config.Provider == nil {
		return errors.New("workload identity provider is unavailable")
	}
	if config.Pool.State != "ACTIVE" || config.Pool.Disabled ||
		config.Provider.State != "ACTIVE" || config.Provider.Disabled {
		return errors.New("workload identity provider is not active")
	}
	provider := config.Provider
	if provider.OIDC == nil || provider.AWS != nil || provider.SAML != nil || provider.X509 != nil {
		return errors.New("workload identity provider kind is unsupported")
	}
	if provider.AttributeCondition != "" {
		return errors.New("attribute conditions are unsupported")
	}
	if len(provider.AttributeMapping) != 1 ||
		provider.AttributeMapping["google.subject"] != "assertion.sub" {
		return errors.New("attribute mapping is unsupported")
	}
	return nil
}

type jwtHeader struct {
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
	Type      string `json:"typ,omitempty"`
}

type jwkSet struct {
	Keys []jwk `json:"keys"`
}

type jwk struct {
	KeyType   string `json:"kty"`
	KeyID     string `json:"kid"`
	Algorithm string `json:"alg,omitempty"`
	Use       string `json:"use,omitempty"`
	Modulus   string `json:"n"`
	Exponent  string `json:"e"`
}

func validateOIDCJWT(token, exchangeAudience string, provider *iamshim.WorkloadIdentityPoolProvider, now time.Time) (string, error) {
	if len(token) == 0 || len(token) > maxSubjectTokenBytes {
		return "", errors.New("subject token size is invalid")
	}
	segments := strings.Split(token, ".")
	if len(segments) != 3 {
		return "", errors.New("subject token must contain three segments")
	}
	headerJSON, err := decodeJWTPart(segments[0])
	if err != nil {
		return "", err
	}
	var header jwtHeader
	if err := decodeStrictJSON(headerJSON, &header); err != nil ||
		header.Algorithm != "RS256" || header.KeyID == "" {
		return "", errors.New("subject token header is invalid")
	}
	payloadJSON, err := decodeJWTPart(segments[1])
	if err != nil {
		return "", err
	}
	claims, err := decodeClaims(payloadJSON)
	if err != nil {
		return "", err
	}
	if claims.Issuer != provider.OIDC.IssuerURI ||
		!audienceAllowed(claims.Audiences, exchangeAudience, provider.OIDC.AllowedAudiences) {
		return "", errors.New("subject token claims are invalid")
	}
	if claims.Subject == "" || len(claims.Subject) > maxSubjectBytes {
		return "", errors.New("subject is invalid")
	}
	nowUnix := now.Unix()
	if claims.ExpiresAt <= nowUnix ||
		(claims.NotBefore != nil && *claims.NotBefore > now.Add(oidcClockSkew).Unix()) ||
		(claims.IssuedAt != nil && *claims.IssuedAt > now.Add(oidcClockSkew).Unix()) {
		return "", errors.New("subject token time claims are invalid")
	}
	publicKey, err := publicKeyForJWT(provider.OIDC.JWKSJSON, header)
	if err != nil {
		return "", err
	}
	signature, err := base64.RawURLEncoding.DecodeString(segments[2])
	if err != nil {
		return "", errors.New("subject token signature is invalid")
	}
	digest := sha256.Sum256([]byte(segments[0] + "." + segments[1]))
	if err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, digest[:], signature); err != nil {
		return "", errors.New("subject token signature is invalid")
	}
	return claims.Subject, nil
}

type oidcClaims struct {
	Issuer    string
	Audiences []string
	Subject   string
	ExpiresAt int64
	NotBefore *int64
	IssuedAt  *int64
}

func decodeClaims(data []byte) (oidcClaims, error) {
	if !utf8.Valid(data) {
		return oidcClaims{}, errors.New("subject token payload is invalid")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return oidcClaims{}, errors.New("subject token payload is invalid")
	}
	var claims oidcClaims
	if err := json.Unmarshal(raw["iss"], &claims.Issuer); err != nil ||
		json.Unmarshal(raw["sub"], &claims.Subject) != nil ||
		json.Unmarshal(raw["exp"], &claims.ExpiresAt) != nil {
		return oidcClaims{}, errors.New("subject token claims are invalid")
	}
	audiences, err := decodeAudience(raw["aud"])
	if err != nil {
		return oidcClaims{}, err
	}
	claims.Audiences = audiences
	for name, destination := range map[string]**int64{"nbf": &claims.NotBefore, "iat": &claims.IssuedAt} {
		value, exists := raw[name]
		if !exists {
			continue
		}
		var parsed int64
		if err := json.Unmarshal(value, &parsed); err != nil {
			return oidcClaims{}, errors.New("subject token time claim is invalid")
		}
		*destination = &parsed
	}
	return claims, nil
}

func decodeAudience(raw json.RawMessage) ([]string, error) {
	var one string
	if err := json.Unmarshal(raw, &one); err == nil {
		if one == "" {
			return nil, errors.New("subject token audience is invalid")
		}
		return []string{one}, nil
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err != nil || len(many) == 0 {
		return nil, errors.New("subject token audience is invalid")
	}
	for _, audience := range many {
		if audience == "" {
			return nil, errors.New("subject token audience is invalid")
		}
	}
	return many, nil
}

func audienceAllowed(tokenAudiences []string, exchangeAudience string, configured []string) bool {
	allowed := configured
	if len(allowed) == 0 {
		allowed = []string{exchangeAudience}
	}
	for _, tokenAudience := range tokenAudiences {
		for _, allowedAudience := range allowed {
			if tokenAudience == allowedAudience {
				return true
			}
		}
	}
	return false
}

func publicKeyForJWT(rawJWKS string, header jwtHeader) (*rsa.PublicKey, error) {
	if len(rawJWKS) == 0 || len(rawJWKS) > maxJWKSBytes || !utf8.ValidString(rawJWKS) {
		return nil, errors.New("provider JWKS is invalid")
	}
	var set jwkSet
	if err := decodeStrictJSON([]byte(rawJWKS), &set); err != nil ||
		len(set.Keys) == 0 || len(set.Keys) > maxJWKCount {
		return nil, errors.New("provider JWKS is invalid")
	}
	var match *jwk
	for index := range set.Keys {
		if set.Keys[index].KeyID == header.KeyID {
			if match != nil {
				return nil, errors.New("provider JWKS key id is ambiguous")
			}
			match = &set.Keys[index]
		}
	}
	if match == nil || match.KeyType != "RSA" ||
		(match.Algorithm != "" && match.Algorithm != "RS256") ||
		(match.Use != "" && match.Use != "sig") {
		return nil, errors.New("provider JWKS key is invalid")
	}
	modulus, err := base64.RawURLEncoding.DecodeString(match.Modulus)
	if err != nil {
		return nil, errors.New("provider JWKS key is invalid")
	}
	exponentBytes, err := base64.RawURLEncoding.DecodeString(match.Exponent)
	if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 4 {
		return nil, errors.New("provider JWKS key is invalid")
	}
	exponent := new(big.Int).SetBytes(exponentBytes)
	if !exponent.IsInt64() || exponent.Int64() < 3 || exponent.Int64()%2 == 0 {
		return nil, errors.New("provider JWKS key is invalid")
	}
	key := &rsa.PublicKey{N: new(big.Int).SetBytes(modulus), E: int(exponent.Int64())}
	if key.N.BitLen() < 2048 || key.N.BitLen() > 8192 {
		return nil, errors.New("provider JWKS RSA key size is invalid")
	}
	return key, nil
}

func decodeJWTPart(encoded string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || !utf8.Valid(decoded) {
		return nil, errors.New("subject token encoding is invalid")
	}
	return decoded, nil
}

func decodeStrictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
}

func writeSTSError(w http.ResponseWriter, code int, oauthError, description string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":             oauthError,
		"error_description": description,
	})
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "status": status, "message": message,
	}})
}
