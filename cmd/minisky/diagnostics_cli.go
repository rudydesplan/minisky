package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

const defaultDiagnosticsEndpoint = "http://127.0.0.1:8081"

var diagnosticsEndpoint string

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
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" {
		return "", fmt.Errorf("invalid diagnostics endpoint %q: use an absolute local HTTP URL", base)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
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
	response, err := http.DefaultClient.Do(request)
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
	diagnosticsCmd.AddCommand(
		diagnosticsRequestsCmd,
		diagnosticsTracesCmd,
		diagnosticsMetricsCmd,
		diagnosticsReplayCmd,
	)
	rootCmd.AddCommand(diagnosticsCmd)
}
