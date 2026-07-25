package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "minisky",
	Short: "MiniSky: A lightweight, Go-based High-Fidelity local GCP emulator",
	Long: `MiniSky is a lightweight, high-performance emulator for Google Cloud Platform services written entirely in Go.
It uses dynamic lazy-loading to ensure a sub-100ms startup, spinning up resources only when requested via API or the Dashboard.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func getJSON(endpoint string, target any) error {
	resp, err := http.Get(endpoint)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("request failed with status %s", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
