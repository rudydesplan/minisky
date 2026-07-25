package security

import (
	"bytes"
	"context"
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
