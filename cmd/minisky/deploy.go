package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	deployName       string
	deployRuntime    string
	deployEntryPoint string
	deploySource     string
	deployType       string
)

var deployCmd = &cobra.Command{
	Use:     "deploy",
	Short:   "Deploy a serverless resource to MiniSky",
	Example: `./minisky deploy --name my-func --runtime python312 --entry-point handler --source main.py`,
	Run: func(cmd *cobra.Command, args []string) {
		// 1. Read source code
		code, err := os.ReadFile(deploySource)
		if err != nil {
			fmt.Printf("❌ Error reading source file: %v\n", err)
			return
		}

		// 2. Prepare payload
		payload := map[string]interface{}{
			"type":       deployType,
			"name":       deployName,
			"runtime":    deployRuntime,
			"entryPoint": deployEntryPoint,
			"code":       string(code),
		}
		// 3. Send to MiniSky Gateway
		endpoint, err := miniskyAPIURL("cloudfunctions", "/v2/deploy")
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "❌ Invalid API endpoint: %v\n", err)
			return
		}

		fmt.Fprintf(cmd.OutOrStdout(), "🚀 Deploying %s '%s' to MiniSky...\n", deployType, deployName)
		if err := postJSON(endpoint, payload, nil); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "❌ Deployment failed: %v\n", err)
			return
		}

		fmt.Fprintf(cmd.OutOrStdout(), "✅ Successfully deployed '%s'!\n", deployName)
		fmt.Fprintln(cmd.OutOrStdout(), "Run 'minisky list' to inspect the deployed resource.")
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List resources in MiniSky",
	Run: func(cmd *cobra.Command, args []string) {
		functionsEndpoint, err := miniskyAPIURL("cloudfunctions", "/v2/functions")
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}
		servicesEndpoint, err := miniskyAPIURL("run", "/v2/services")
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}

		// Fetch Functions
		fmt.Fprintln(cmd.OutOrStdout(), "--- Cloud Functions v2 ---")
		printResources(cmd, functionsEndpoint)

		// Fetch Services
		fmt.Fprintln(cmd.OutOrStdout(), "\n--- Cloud Run Services ---")
		printResources(cmd, servicesEndpoint)
	},
}

func printResources(cmd *cobra.Command, endpoint string) {
	var data struct {
		Functions []interface{} `json:"functions"`
		Services  []interface{} `json:"services"`
	}
	if err := getJSON(endpoint, &data); err != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
		return
	}

	list := data.Functions
	if list == nil {
		list = data.Services
	}

	if len(list) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "  (None)")
		return
	}

	for _, item := range list {
		m := item.(map[string]interface{})
		name := m["name"].(string)
		state := m["state"].(string)
		fmt.Fprintf(cmd.OutOrStdout(), "  - %s [%s]\n", last(strings.Split(name, "/")), state)
	}
}

// Simple helper because strings.Split returns a slice
func last(parts []string) string {
	return parts[len(parts)-1]
}

func init() {
	deployCmd.Flags().StringVar(&deployName, "name", "", "Name of the resource")
	deployCmd.Flags().StringVar(&deployRuntime, "runtime", "python312", "Runtime (python312, nodejs22, etc.)")
	deployCmd.Flags().StringVar(&deployEntryPoint, "entry-point", "handler", "Function entry point")
	deployCmd.Flags().StringVar(&deploySource, "source", "", "Path to source code file")
	deployCmd.Flags().StringVar(&deployType, "type", "function", "Resource type (function or service)")

	deployCmd.MarkFlagRequired("name")
	deployCmd.MarkFlagRequired("source")

	rootCmd.AddCommand(deployCmd)
	rootCmd.AddCommand(listCmd)
}
