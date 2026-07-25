package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	localsecurity "minisky/pkg/security"
	"minisky/pkg/state"
)

func TestAuditCLIContracts(t *testing.T) {
	root := t.TempDir()
	const profile = "enterprise-wif"
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", profile)

	auditPath := seedCLIAuditRecords(t, root, profile, 3)

	var verifyOutput bytes.Buffer
	verify := newAuditCommand()
	verify.SetArgs([]string{"verify", "--profile", profile})
	verify.SetOut(&verifyOutput)
	verify.SetErr(&bytes.Buffer{})
	if err := verify.Execute(); err != nil {
		t.Fatalf("audit verify failed: %v", err)
	}
	if !strings.Contains(verifyOutput.String(), "Audit hash chain verified") {
		t.Fatalf("verify output = %q", verifyOutput.String())
	}

	exportPath := filepath.Join(t.TempDir(), "audit-export.json")
	if err := os.WriteFile(exportPath, []byte("preserve-on-failure"), 0o600); err != nil {
		t.Fatal(err)
	}
	invalidExport := newAuditCommand()
	invalidExport.SetArgs([]string{"export", exportPath, "--profile", profile, "--limit", "10001"})
	invalidExport.SetOut(&bytes.Buffer{})
	invalidExport.SetErr(&bytes.Buffer{})
	if err := invalidExport.Execute(); err == nil {
		t.Fatal("audit export accepted an unbounded limit")
	}
	preserved, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(preserved) != "preserve-on-failure" {
		t.Fatalf("failed atomic export replaced destination with %q", preserved)
	}

	exportCommand := newAuditCommand()
	exportCommand.SetArgs([]string{"export", exportPath, "--profile", profile, "--limit", "2"})
	exportCommand.SetOut(&bytes.Buffer{})
	exportCommand.SetErr(&bytes.Buffer{})
	if err := exportCommand.Execute(); err != nil {
		t.Fatalf("audit export failed: %v", err)
	}
	payload, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	var records []localsecurity.AuditRecord
	if err := json.Unmarshal(payload, &records); err != nil {
		t.Fatalf("decode audit export: %v", err)
	}
	if len(records) != 2 || records[0].Sequence != 2 || records[1].Sequence != 3 {
		t.Fatalf("bounded records = %#v", records)
	}
	info, err := os.Stat(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("export permissions = %04o, want 0600", info.Mode().Perm())
	}
	for _, secret := range []string{
		"cli-bearer-secret",
		"cli-body-secret",
		"cli-query-secret",
		"cli-cookie-secret",
		"-----BEGIN PRIVATE KEY-----",
	} {
		if strings.Contains(string(payload), secret) {
			t.Errorf("audit export contains secret %q", secret)
		}
	}

	tampered, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	tampered[len(tampered)/2] ^= 1
	if err := os.WriteFile(auditPath, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	verifyTampered := newAuditCommand()
	verifyTampered.SetArgs([]string{"verify", "--profile", profile})
	verifyTampered.SetOut(&bytes.Buffer{})
	verifyTampered.SetErr(&bytes.Buffer{})
	if err := verifyTampered.Execute(); err == nil {
		t.Fatal("audit verify accepted a tampered log")
	}
}

func seedCLIAuditRecords(t *testing.T, root, profile string, count int) string {
	t.Helper()
	store, err := state.New(root, profile)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := localsecurity.OpenAuditLog(store.ProfileDir(), profile, false)
	if err != nil {
		t.Fatal(err)
	}
	handler := audit.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-MiniSky-Principal",
			"principal://iam.googleapis.com/projects/local-dev-project/locations/global/workloadIdentityPools/ci-pool/subject/repository:minisky")
		w.WriteHeader(http.StatusNoContent)
	}), func(r *http.Request) localsecurity.AuditEvent {
		return localsecurity.AuditEvent{
			Method: r.Method, Service: "bigquery.googleapis.com",
			Route: "/bigquery/v2/projects/{project}/datasets", Project: "local-dev-project",
		}
	})
	for i := 0; i < count; i++ {
		request := httptest.NewRequest(
			http.MethodPost,
			"http://localhost/bigquery/v2/projects/local-dev-project/datasets?token=cli-query-secret",
			strings.NewReader(`{"secret":"cli-body-secret"}`),
		)
		request.Header.Set("Authorization", "Bearer cli-bearer-secret")
		request.Header.Set("Cookie", "session=cli-cookie-secret")
		request.Header.Set("X-Private-Key", "-----BEGIN PRIVATE KEY-----")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("seed status = %d", response.Code)
		}
	}
	path := audit.Path()
	if err := audit.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
