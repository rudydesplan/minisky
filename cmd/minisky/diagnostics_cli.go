package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const defaultDiagnosticsEndpoint = "http://127.0.0.1:8081"

var (
	diagnosticsEndpoint    string
	diagnosticsCAFile      string
	diagnosticsBearerToken string
	diagnosticsProject     string
)

var diagnosticsCmd = &cobra.Command{
	Use:   "diagnostics",
	Short: "Query local gateway diagnostics",
}

var diagnosticsRequestsCmd = &cobra.Command{
	Use:   "requests",
	Short: "List bounded gateway access records",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return printDiagnostics(cmd, http.MethodGet, "/api/diagnostics/requests", nil)
	},
}

var diagnosticsTracesCmd = &cobra.Command{
	Use:   "traces",
	Short: "List trace-correlated gateway records",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return printDiagnostics(cmd, http.MethodGet, "/api/diagnostics/traces", nil)
	},
}

var diagnosticsMetricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Print Prometheus-compatible gateway metrics",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return printDiagnostics(cmd, http.MethodGet, "/api/diagnostics/metrics", nil)
	},
}

var diagnosticsReplayCmd = &cobra.Command{
	Use:   "replay REQUEST_ID",
	Short: "Replay an eligible request through the same gateway",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return printDiagnostics(cmd, http.MethodPost, "/api/diagnostics/requests/"+url.PathEscape(args[0])+"/replay", http.NoBody)
	},
}

func diagnosticsBaseURL() (string, error) {
	base := strings.TrimSpace(diagnosticsEndpoint)
	if base == "" {
		base = strings.TrimSpace(os.Getenv("MINISKY_DIAGNOSTICS_ENDPOINT"))
	}
	if base == "" {
		base = defaultDiagnosticsEndpoint
	}
	parsed, err := url.Parse(base)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("invalid diagnostics endpoint %q: use an absolute HTTP or HTTPS URL", base)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func diagnosticsHTTPClient(base string) (*http.Client, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("parse diagnostics endpoint: %w", err)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if parsed.Scheme == "https" {
		caFile := strings.TrimSpace(diagnosticsCAFile)
		if caFile == "" {
			caFile = strings.TrimSpace(os.Getenv("MINISKY_DIAGNOSTICS_CA_CERT"))
		}
		if caFile != "" {
			pemBytes, err := os.ReadFile(caFile)
			if err != nil {
				return nil, fmt.Errorf("read diagnostics CA certificate: %w", err)
			}
			roots, err := x509.SystemCertPool()
			if err != nil || roots == nil {
				roots = x509.NewCertPool()
			}
			if !roots.AppendCertsFromPEM(pemBytes) {
				return nil, errors.New("diagnostics CA file contains no certificates")
			}
			transport.TLSClientConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    roots,
			}
		}
	}
	return &http.Client{Transport: transport, Timeout: 30 * time.Second}, nil
}

func validateDiagnosticsBearerURL(endpoint, token string) error {
	if strings.TrimSpace(token) == "" {
		return nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("invalid diagnostics endpoint %q", endpoint)
	}
	if parsed.Scheme == "https" {
		return nil
	}
	host := strings.TrimSuffix(parsed.Hostname(), ".")
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	if zone := strings.LastIndexByte(host, '%'); zone >= 0 {
		host = host[:zone]
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("refusing to send diagnostics bearer token over cleartext HTTP to non-loopback host %q", parsed.Hostname())
}

func printDiagnostics(cmd *cobra.Command, method, path string, body io.Reader) error {
	base, err := diagnosticsBaseURL()
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(cmd.Context(), method, base+path, body)
	if err != nil {
		return fmt.Errorf("build diagnostics request: %w", err)
	}
	token := strings.TrimSpace(diagnosticsBearerToken)
	if token == "" {
		token = strings.TrimSpace(os.Getenv("MINISKY_DIAGNOSTICS_BEARER_TOKEN"))
	}
	if token != "" {
		if err := validateDiagnosticsBearerURL(base, token); err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+token)
	}
	project := strings.TrimSpace(diagnosticsProject)
	if project == "" {
		project = strings.TrimSpace(os.Getenv("MINISKY_DIAGNOSTICS_PROJECT"))
	}
	if project == "" {
		project = "local-dev-project"
	}
	if project != "" {
		request.Header.Set("X-MiniSky-Project", project)
		if path != "/api/diagnostics/metrics" {
			query := request.URL.Query()
			query.Set("project", project)
			request.URL.RawQuery = query.Encode()
		}
	}
	client, err := diagnosticsHTTPClient(base)
	if err != nil {
		return err
	}
	client.CheckRedirect = func(redirect *http.Request, _ []*http.Request) error {
		return validateDiagnosticsBearerURL(redirect.URL.String(), token)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("diagnostics request failed: %w", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("read diagnostics response: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("diagnostics request failed with status %s: %s", response.Status, strings.TrimSpace(string(payload)))
	}
	if strings.Contains(response.Header.Get("Content-Type"), "application/json") {
		var formatted bytes.Buffer
		if json.Indent(&formatted, payload, "", "  ") == nil {
			payload = formatted.Bytes()
		}
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), string(payload))
	return err
}

func init() {
	diagnosticsCmd.PersistentFlags().StringVar(
		&diagnosticsEndpoint,
		"diagnostics-endpoint",
		"",
		"Local dashboard diagnostics URL (env: MINISKY_DIAGNOSTICS_ENDPOINT)",
	)
	diagnosticsCmd.PersistentFlags().StringVar(
		&diagnosticsCAFile,
		"diagnostics-ca-cert",
		"",
		"CA certificate PEM for HTTPS diagnostics (env: MINISKY_DIAGNOSTICS_CA_CERT)",
	)
	diagnosticsCmd.PersistentFlags().StringVar(
		&diagnosticsBearerToken,
		"diagnostics-bearer-token",
		"",
		"Local dashboard bearer token (env: MINISKY_DIAGNOSTICS_BEARER_TOKEN)",
	)
	diagnosticsCmd.PersistentFlags().StringVar(
		&diagnosticsProject,
		"diagnostics-project",
		"",
		"Dashboard project sent in X-MiniSky-Project (env: MINISKY_DIAGNOSTICS_PROJECT)",
	)
	diagnosticsCmd.AddCommand(
		diagnosticsRequestsCmd,
		diagnosticsTracesCmd,
		diagnosticsMetricsCmd,
		diagnosticsReplayCmd,
	)
	rootCmd.AddCommand(diagnosticsCmd)
}
