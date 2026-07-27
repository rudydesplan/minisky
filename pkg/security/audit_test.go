package security

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestAuditLogHashChainRedactionTamperAndBoundedExport(t *testing.T) {
	profileDir := t.TempDir()
	audit, err := OpenAuditLog(profileDir, "team-a", false)
	if err != nil {
		t.Fatal(err)
	}
	defer audit.Close()

	handler := audit.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}), func(r *http.Request) AuditEvent {
		return AuditEvent{
			Principal: r.Header.Get("X-MiniSky-Principal"),
			Method:    r.Method,
			Service:   "secretmanager.googleapis.com",
			Route:     "/v1/projects/{id}/secrets",
			Project:   "demo-project",
		}
	})
	request := httptest.NewRequest(http.MethodPost, "http://localhost/v1/projects/demo-project/secrets?token=query-secret", strings.NewReader(`{"secret":"body-secret"}`))
	request.Header.Set("Authorization", "Bearer header-secret")
	request.Header.Set("X-MiniSky-Principal", "user:alice@example.com")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d", response.Code)
	}
	if err := audit.Verify(); err != nil {
		t.Fatal(err)
	}
	var exported bytes.Buffer
	if err := audit.Export(&exported, 1); err != nil {
		t.Fatal(err)
	}
	text := exported.String()
	for _, secret := range []string{"query-secret", "body-secret", "header-secret", "Authorization"} {
		if strings.Contains(text, secret) {
			t.Fatalf("audit export contains secret %q: %s", secret, text)
		}
	}
	if !strings.Contains(text, `"status": 201`) || !strings.Contains(text, `"profile": "team-a"`) {
		t.Fatalf("audit export missing bounded metadata: %s", text)
	}
	if err := audit.Export(&bytes.Buffer{}, maxAuditExportRecords+1); err == nil {
		t.Fatal("expected export bound error")
	}

	if err := audit.Close(); err != nil {
		t.Fatal(err)
	}
	file := audit.Path()
	payload, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	payload[len(payload)/2] ^= 1
	if err := os.WriteFile(file, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenAuditLog(profileDir, "team-a", true); err == nil {
		t.Fatal("expected tamper detection")
	}
}

func TestAuditWrapCapturesAuthenticatedFederatedPrincipalWithoutSecrets(t *testing.T) {
	profileDir := t.TempDir()
	audit, err := OpenAuditLog(profileDir, "federated", true)
	if err != nil {
		t.Fatal(err)
	}
	defer audit.Close()

	const principal = "principal://iam.googleapis.com/projects/local-dev-project/locations/global/workloadIdentityPools/ci-pool/subject/repository:minisky"
	handler := audit.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-MiniSky-Principal"); got != "user:attacker@example.com" {
			t.Fatalf("request principal = %q, want caller-supplied value before authentication middleware", got)
		}
		r.Header.Set("X-MiniSky-Principal", principal)
		w.WriteHeader(http.StatusAccepted)
	}), func(r *http.Request) AuditEvent {
		return AuditEvent{
			Principal: r.Header.Get("X-MiniSky-Principal"),
			Method:    r.Method,
			Service:   "bigquery.googleapis.com",
			Route:     "/bigquery/v2/projects/{project}/datasets/{dataset}",
			Project:   "local-dev-project",
		}
	})

	secrets := []string{
		"bearer-secret",
		"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ3b3JrbG9hZCJ9.signature",
		"-----BEGIN PRIVATE KEY-----",
		"request-body-secret",
		"raw-query-secret",
		"cookie-secret",
		"arbitrary-header-secret",
	}
	request := httptest.NewRequest(
		http.MethodPatch,
		"http://localhost/bigquery/v2/projects/local-dev-project/datasets/analytics?token="+secrets[4],
		strings.NewReader(`{"credential":"`+secrets[3]+`"}`),
	)
	request.Header.Set("Authorization", "Bearer "+secrets[0]+"."+secrets[1])
	request.Header.Set("Cookie", "session="+secrets[5])
	request.Header.Set("X-Private-Key", secrets[2])
	request.Header.Set("X-Arbitrary", secrets[6])
	request.Header.Set("X-MiniSky-Principal", "user:attacker@example.com")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusAccepted)
	}

	var exported bytes.Buffer
	if err := audit.Export(&exported, 10); err != nil {
		t.Fatal(err)
	}
	var records []AuditRecord
	if err := json.Unmarshal(exported.Bytes(), &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("records = %d, want strict attempt and complete", len(records))
	}
	if records[0].Phase != "attempt" || records[0].Principal != "" {
		t.Fatalf("attempt = %#v; pre-authentication strict attempt may omit principal", records[0])
	}
	complete := records[1]
	if complete.Phase != "complete" ||
		complete.Principal != principal ||
		complete.Status != http.StatusAccepted ||
		complete.Route != "/bigquery/v2/projects/{project}/datasets/{dataset}" ||
		complete.Project != "local-dev-project" {
		t.Fatalf("complete record = %#v", complete)
	}

	raw, err := os.ReadFile(audit.Path())
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{string(raw), exported.String()} {
		for _, secret := range secrets {
			if strings.Contains(output, secret) {
				t.Errorf("audit output contains secret %q", secret)
			}
		}
	}
}

func TestAuditRecordFieldsAreSanitizedAndBounded(t *testing.T) {
	audit, err := OpenAuditLog(t.TempDir(), "bounds", false)
	if err != nil {
		t.Fatal(err)
	}
	defer audit.Close()

	handler := audit.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), func(*http.Request) AuditEvent {
		return AuditEvent{
			Principal: "principal://iam.googleapis.com/" + strings.Repeat("p", 400) + "\r\ninjected",
			Service:   strings.Repeat("s", 300),
			Route:     "/" + strings.Repeat("r", 700),
			Project:   strings.Repeat("p", 200),
		}
	})
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/resource", nil))

	var exported bytes.Buffer
	if err := audit.Export(&exported, 1); err != nil {
		t.Fatal(err)
	}
	var records []AuditRecord
	if err := json.Unmarshal(exported.Bytes(), &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	record := records[0]
	if len(record.Principal) > 256 || len(record.Service) > 253 ||
		len(record.Route) > 512 || len(record.Project) > 128 {
		t.Fatalf("unbounded record fields: principal=%d service=%d route=%d project=%d",
			len(record.Principal), len(record.Service), len(record.Route), len(record.Project))
	}
	if strings.ContainsAny(record.Principal, "\r\n") {
		t.Fatalf("principal contains control characters: %q", record.Principal)
	}
}

type failingAuditWriter struct{}

func (failingAuditWriter) Write([]byte) (int, error) { return 0, context.Canceled }
func (failingAuditWriter) Close() error              { return nil }

func TestAuditStrictModeRejectsMutationBeforeDispatchOnWriteFailure(t *testing.T) {
	audit := &AuditLog{strict: true, profile: "strict", writer: failingAuditWriter{}}
	dispatched := false
	handler := audit.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		dispatched = true
		w.WriteHeader(http.StatusNoContent)
	}), func(r *http.Request) AuditEvent { return AuditEvent{Method: r.Method} })
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "http://localhost/resource", nil))
	if response.Code != http.StatusInternalServerError || dispatched {
		t.Fatalf("status=%d dispatched=%t", response.Code, dispatched)
	}
	if !errors.Is(audit.PersistenceError(), context.Canceled) {
		t.Fatalf("persistence error = %v, want write failure", audit.PersistenceError())
	}
}

func TestAuditNonStrictWriteFailureDegradesPersistenceHealth(t *testing.T) {
	audit := &AuditLog{profile: "non-strict", writer: failingAuditWriter{}}
	handler := audit.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "http://localhost/resource", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
	if !errors.Is(audit.PersistenceError(), context.Canceled) {
		t.Fatalf("persistence error = %v, want write failure", audit.PersistenceError())
	}
}

func TestAuditCheckpointDetectsValidPrefixTruncation(t *testing.T) {
	profileDir := t.TempDir()
	audit, err := OpenAuditLog(profileDir, "truncate", false)
	if err != nil {
		t.Fatal(err)
	}
	handler := audit.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), func(r *http.Request) AuditEvent { return AuditEvent{Method: r.Method, Route: "/resource"} })
	for i := 0; i < 2; i++ {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "http://localhost/resource", nil))
	}
	if err := audit.Close(); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(audit.Path())
	if err != nil {
		t.Fatal(err)
	}
	lastLine := bytes.LastIndex(payload[:len(payload)-1], []byte{'\n'})
	if lastLine < 0 {
		t.Fatal("audit log did not contain two records")
	}
	if err := os.WriteFile(audit.Path(), payload[:lastLine+1], 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenAuditLog(profileDir, "truncate", false); err == nil {
		t.Fatal("expected valid-prefix truncation detection")
	}
}

func TestAuditRejectsSymlinkedProfilePath(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	profile := root + "/profile"
	if err := os.Symlink(outside, profile); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenAuditLog(profile, "symlink", false); err == nil {
		t.Fatal("expected symlinked profile rejection")
	}
}
