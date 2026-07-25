package iamcredentials

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"minisky/pkg/registry"
	localsecurity "minisky/pkg/security"
)

type iamService interface {
	EnforcementEnabled() bool
	Authorize(resource, principal, permission string) bool
	VerifyLocalToken(token, audience, scope string) (localsecurity.Claims, error)
	ResolveServiceAccount(identifier string) (email string, disabled, found bool)
	IssueLocalToken(subject, audience string, scopes []string, lifetime time.Duration) (string, time.Time, error)
	TokenAudience() string
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
	targetName := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/"), ":generateAccessToken")
	targetIdentifier, ok := parseServiceAccountName(targetName)
	if !ok {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Service account name must use projects/-/serviceAccounts/{email or ID}")
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
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Invalid generateAccessToken request")
		return
	}
	if len(input.Delegates) > 4 {
		writeError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "Delegation chains longer than four delegates are not supported locally")
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

	delegateIdentifiers := make([]string, len(input.Delegates))
	seenIdentifiers := map[string]struct{}{targetIdentifier: {}}
	for index, delegate := range input.Delegates {
		identifier, valid := parseServiceAccountName(delegate)
		if !valid {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Delegates must use projects/-/serviceAccounts/{email or ID}")
			return
		}
		if _, duplicate := seenIdentifiers[identifier]; duplicate {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Delegation chain contains a duplicate or cycle")
			return
		}
		seenIdentifiers[identifier] = struct{}{}
		delegateIdentifiers[index] = identifier
	}

	principal, authenticated := api.requestPrincipal(w, r)
	if !authenticated {
		return
	}

	targetEmail, targetDisabled, targetFound := api.iam.ResolveServiceAccount(targetIdentifier)
	if !targetFound {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Service account was not found")
		return
	}
	if targetDisabled {
		writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "Service account is disabled")
		return
	}
	targetResource := canonicalServiceAccountResource(targetEmail)
	delegateEmails := make([]string, len(delegateIdentifiers))
	delegateResources := make([]string, len(delegateIdentifiers))
	seen := map[string]struct{}{targetEmail: {}}
	for index, identifier := range delegateIdentifiers {
		email, disabled, found := api.iam.ResolveServiceAccount(identifier)
		if !found {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "A delegate service account was not found")
			return
		}
		if disabled {
			writeError(w, http.StatusPreconditionFailed, "FAILED_PRECONDITION", "A delegate service account is disabled")
			return
		}
		if _, duplicate := seen[email]; duplicate {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Delegation chain contains a duplicate or cycle")
			return
		}
		seen[email] = struct{}{}
		delegateEmails[index] = email
		delegateResources[index] = canonicalServiceAccountResource(email)
	}

	if api.iam.EnforcementEnabled() {
		edgePrincipal := principal
		for index, resource := range delegateResources {
			if !api.iam.Authorize(resource, edgePrincipal, "iam.serviceAccounts.getAccessToken") {
				writeError(w, http.StatusForbidden, "PERMISSION_DENIED", "Delegation chain is not authorized")
				return
			}
			edgePrincipal = "serviceAccount:" + delegateEmails[index]
		}
		if !api.iam.Authorize(targetResource, edgePrincipal, "iam.serviceAccounts.getAccessToken") {
			writeError(w, http.StatusForbidden, "PERMISSION_DENIED", "Delegation chain is not authorized")
			return
		}
	}
	token, expires, err := api.iam.IssueLocalToken(
		"serviceAccount:"+targetEmail, api.iam.TokenAudience(), input.Scopes, lifetime,
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

func (api *API) requestPrincipal(w http.ResponseWriter, r *http.Request) (string, bool) {
	headerPrincipal := strings.TrimSpace(r.Header.Get("X-MiniSky-Principal"))
	if !api.iam.EnforcementEnabled() {
		return headerPrincipal, true
	}
	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	if authorization == "" {
		writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Bearer authentication is required")
		return "", false
	}
	scheme, token, ok := strings.Cut(authorization, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Bearer authentication is malformed")
		return "", false
	}
	claims, err := api.iam.VerifyLocalToken(
		strings.TrimSpace(token),
		api.iam.TokenAudience(),
		"https://www.googleapis.com/auth/cloud-platform",
	)
	principal := strings.TrimSpace(claims.Subject)
	if err != nil || principal == "" {
		writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Bearer credential is invalid or expired")
		return "", false
	}
	if headerPrincipal != "" && headerPrincipal != principal {
		writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "Bearer identity conflicts with the supplied principal")
		return "", false
	}
	r.Header.Set("X-MiniSky-Principal", principal)
	return principal, true
}

func parseServiceAccountName(name string) (string, bool) {
	const prefix = "projects/-/serviceAccounts/"
	if !strings.HasPrefix(name, prefix) {
		return "", false
	}
	identifier := strings.TrimPrefix(name, prefix)
	if identifier == "" || strings.ContainsAny(identifier, "/\\ \t\r\n") {
		return "", false
	}
	if allDigits(identifier) {
		return identifier, true
	}
	if strings.Count(identifier, "@") != 1 {
		return "", false
	}
	local, domain, _ := strings.Cut(identifier, "@")
	if local == "" || domain == "" || !strings.Contains(domain, ".") {
		return "", false
	}
	return identifier, true
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func canonicalServiceAccountResource(email string) string {
	return "projects/-/serviceAccounts/" + email
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "status": status, "message": message,
	}})
}
