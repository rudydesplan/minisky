package iamcredentials

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"minisky/pkg/registry"
)

type iamService interface {
	EnforcementEnabled() bool
	Authorize(resource, principal, permission string) bool
	IssueLocalToken(subject, audience string, scopes []string, lifetime time.Duration) (string, time.Time, error)
}

type API struct {
	iam iamService
}

func init() {
	registry.Register("iamcredentials.googleapis.com", func(*registry.Context) http.Handler {
		return &API{}
	})
}

func (api *API) OnPostBoot(ctx *registry.Context) {
	if iam, ok := ctx.GetShim("iam.googleapis.com").(iamService); ok {
		api.iam = iam
	}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, ":generateAccessToken") {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Only generateAccessToken is supported")
		return
	}
	const prefix = "/v1/projects/-/serviceAccounts/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Service account name must use projects/-/serviceAccounts/{email}")
		return
	}
	email := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, prefix), ":generateAccessToken")
	if email == "" || !strings.Contains(email, "@") {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "A service account email is required")
		return
	}
	var input struct {
		Delegates []string `json:"delegates"`
		Scopes    []string `json:"scope"`
		Lifetime  string   `json:"lifetime"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid generateAccessToken request")
		return
	}
	if len(input.Delegates) != 0 {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Delegated impersonation chains are not supported locally")
		return
	}
	if len(input.Scopes) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "At least one scope is required")
		return
	}
	lifetime := time.Hour
	if input.Lifetime != "" {
		parsed, err := time.ParseDuration(input.Lifetime)
		if err != nil || parsed <= 0 || parsed > time.Hour {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "lifetime must be at most 3600s")
			return
		}
		lifetime = parsed
	}
	if api.iam == nil {
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE", "IAM credential issuer is unavailable")
		return
	}
	resource := "projects/-/serviceAccounts/" + email
	principal := strings.TrimSpace(r.Header.Get("X-MiniSky-Principal"))
	if api.iam.EnforcementEnabled() {
		if principal == "" {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Authentication is required")
			return
		}
		if !api.iam.Authorize(resource, principal, "iam.serviceAccounts.getAccessToken") {
			writeError(w, http.StatusForbidden, "PERMISSION_DENIED", "Caller cannot impersonate this service account")
			return
		}
	}
	token, expires, err := api.iam.IssueLocalToken(
		"serviceAccount:"+email, "minisky-gateway", input.Scopes, lifetime,
	)
	if err != nil {
		writeError(w, http.StatusBadRequest, "FAILED_PRECONDITION", "Service account cannot issue credentials")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"accessToken": token,
		"expireTime":  expires.UTC().Format(time.RFC3339Nano),
	})
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "status": status, "message": message,
	}})
}
