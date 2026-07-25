package sts

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"minisky/pkg/registry"
	localsecurity "minisky/pkg/security"
)

const (
	tokenExchangeGrant = "urn:ietf:params:oauth:grant-type:token-exchange"
	accessTokenType    = "urn:ietf:params:oauth:token-type:access_token"
)

type iamService interface {
	VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error)
	IssueLocalToken(subject, audience string, scopes []string, lifetime time.Duration) (string, time.Time, error)
}

type API struct{ iam iamService }

func init() {
	registry.Register("sts.googleapis.com", func(*registry.Context) http.Handler { return &API{} })
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	if iam, ok := ctx.GetShim("iam.googleapis.com").(iamService); ok {
		api.iam = iam
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost || r.URL.Path != "/v1/token" {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Only local token exchange is supported")
		return
	}
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		writeError(w, http.StatusUnsupportedMediaType, "INVALID_ARGUMENT", "form-encoded request required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := r.ParseForm(); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid token exchange request")
		return
	}
	if r.Form.Get("grant_type") != tokenExchangeGrant ||
		r.Form.Get("requested_token_type") != accessTokenType {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Requested STS grant or token type is unsupported")
		return
	}
	if r.Form.Get("subject_token_type") != accessTokenType {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Only MiniSky local access tokens can be exchanged")
		return
	}
	if api.iam == nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "Local credential issuer unavailable")
		return
	}
	audience := r.Form.Get("audience")
	scope := r.Form.Get("scope")
	claims, err := api.iam.VerifyLocalToken(r.Form.Get("subject_token"), audience, scope)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Subject token is invalid")
		return
	}
	scopes := strings.Fields(scope)
	token, expires, err := api.iam.IssueLocalToken(claims.Subject, "minisky-gateway", scopes, time.Hour)
	if err != nil {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "Token exchange failed")
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

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "status": status, "message": message,
	}})
}
