package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/spf13/cobra"
)

const defaultMiniSkyEndpoint = "http://localhost:8080"

var miniskyEndpoint string

var rootCmd = &cobra.Command{
	Use:   "minisky",
	Short: "MiniSky: A lightweight, Go-based High-Fidelity local GCP emulator",
	Long: `MiniSky is a lightweight, high-performance emulator for Google Cloud Platform services written entirely in Go.
It uses dynamic lazy-loading to ensure a sub-100ms startup, spinning up resources only when requested via API or the Dashboard.`,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&miniskyEndpoint, "endpoint", "", "MiniSky API gateway base URL (env: MINISKY_ENDPOINT)")
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func miniskyAPIURL(service, servicePath string) (string, error) {
	base := strings.TrimSpace(miniskyEndpoint)
	if base == "" {
		base = strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT"))
	}
	if base == "" {
		base = defaultMiniSkyEndpoint
	}

	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("invalid MiniSky endpoint %q: use an absolute URL such as %s", base, defaultMiniSkyEndpoint)
	}
	relative, err := url.Parse(servicePath)
	if err != nil {
		return "", fmt.Errorf("invalid service path %q: %w", servicePath, err)
	}
	u.Path = path.Join(u.Path, "_minisky", service, relative.Path)
	u.RawPath = ""
	u.RawQuery = relative.RawQuery
	return u.String(), nil
}

func getJSON(endpoint string, target any) error {
	return requestJSON(http.MethodGet, endpoint, nil, target)
}

func postJSON(endpoint string, body any, target any) error {
	return requestJSON(http.MethodPost, endpoint, body, target)
}

func requestJSON(method, endpoint string, body any, target any) error {
	var requestBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		requestBody = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, endpoint, requestBody)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		detail := strings.TrimSpace(string(message))
		if detail != "" {
			return fmt.Errorf("request failed with status %s: %s", resp.Status, detail)
		}
		return fmt.Errorf("request failed with status %s", resp.Status)
	}
	if target == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func unsupportedHeadlessCommand(cmd *cobra.Command, service string) {
	fmt.Fprintf(cmd.ErrOrStderr(), "Unsupported: %s does not expose a public MiniSky shim route; this command cannot run through the headless API gateway.\n", service)
}
