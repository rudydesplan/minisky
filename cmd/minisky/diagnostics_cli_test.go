package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
)

func TestDiagnosticsCLIUsesTLSBearerAndProject(t *testing.T) {
	var authorization, project, queryProject string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		project = r.Header.Get("X-MiniSky-Project")
		queryProject = r.URL.Query().Get("project")
		_, _ = w.Write([]byte(`{"requests":[]}`))
	}))
	t.Cleanup(server.Close)

	certificate, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(t.TempDir(), "diagnostics-ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	diagnosticsEndpoint = server.URL
	diagnosticsCAFile = caFile
	diagnosticsBearerToken = "local-token"
	diagnosticsProject = "local-dev-project"
	t.Cleanup(resetDiagnosticsOptions)

	command := &cobra.Command{}
	command.SetContext(context.Background())
	command.SetOut(&discardWriter{})
	if err := printDiagnostics(command, http.MethodGet, "/api/diagnostics/requests", nil); err != nil {
		t.Fatal(err)
	}
	if authorization != "Bearer local-token" {
		t.Fatalf("Authorization = %q", authorization)
	}
	if project != "local-dev-project" {
		t.Fatalf("X-MiniSky-Project = %q", project)
	}
	if queryProject != "local-dev-project" {
		t.Fatalf("project query = %q", queryProject)
	}
}

func TestDiagnosticsCLIRejectsUntrustedTLS(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	diagnosticsEndpoint = server.URL
	t.Cleanup(resetDiagnosticsOptions)

	command := &cobra.Command{}
	command.SetContext(context.Background())
	command.SetOut(&discardWriter{})
	if err := printDiagnostics(command, http.MethodGet, "/api/diagnostics/requests", nil); err == nil {
		t.Fatal("untrusted diagnostics certificate was accepted")
	}
}

func TestDiagnosticsBearerRequiresTLSOrExactLoopbackHost(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		wantErr  bool
	}{
		{name: "IPv4 loopback", endpoint: "http://127.0.0.1:8081"},
		{name: "IPv6 loopback", endpoint: "http://[::1]:8081"},
		{name: "localhost", endpoint: "http://localhost:8081"},
		{name: "localhost trailing dot", endpoint: "http://localhost.:8081"},
		{name: "TLS remote", endpoint: "https://diagnostics.example.test"},
		{name: "remote HTTP", endpoint: "http://192.0.2.10:8081", wantErr: true},
		{name: "deceptive localhost suffix", endpoint: "http://localhost.example.test:8081", wantErr: true},
		{name: "deceptive IPv4 suffix", endpoint: "http://127.0.0.1.example.test:8081", wantErr: true},
		{name: "integer encoded IPv4", endpoint: "http://2130706433:8081", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateDiagnosticsBearerURL(test.endpoint, "secret")
			if (err != nil) != test.wantErr {
				t.Fatalf("validateDiagnosticsBearerURL(%q) error = %v, wantErr %t", test.endpoint, err, test.wantErr)
			}
		})
	}
}

type discardWriter struct{}

func (*discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func resetDiagnosticsOptions() {
	diagnosticsEndpoint = ""
	diagnosticsCAFile = ""
	diagnosticsBearerToken = ""
	diagnosticsProject = ""
}
