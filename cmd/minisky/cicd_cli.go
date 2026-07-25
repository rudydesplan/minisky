package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	artifactProject   string
	artifactLocation  string
	cloudBuildProject string
)

var arCmd = &cobra.Command{
	Use:   "artifact-registry",
	Short: "Manage Artifact Registry repositories",
}

var cbCmd = &cobra.Command{
	Use:   "cloud-build",
	Short: "Manage Cloud Build workflows",
}

var vertexCmd = &cobra.Command{
	Use:   "vertex-ai",
	Short: "Manage Vertex AI models and providers",
}

func init() {
	// Artifact Registry
	arCmd.AddCommand(&cobra.Command{
		Use: "repositories list",
		Run: func(cmd *cobra.Command, args []string) {
			var data struct {
				Repositories []struct {
					Name string `json:"name"`
				} `json:"repositories"`
			}
			endpoint, err := miniskyAPIURL("artifactregistry", fmt.Sprintf("/v1/projects/%s/locations/%s/repositories", artifactProject, artifactLocation))
			if err == nil {
				err = getJSON(endpoint, &data)
			}
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Fprintln(cmd.OutOrStdout(), "ARTIFACT REGISTRY REPOSITORIES:")
			for _, r := range data.Repositories {
				fmt.Fprintf(cmd.OutOrStdout(), "  - %s\n", r.Name)
			}
		},
	})

	// Cloud Build
	cbCmd.AddCommand(&cobra.Command{
		Use: "builds list",
		Run: func(cmd *cobra.Command, args []string) {
			var data struct {
				Builds []struct {
					ID     string `json:"id"`
					Status string `json:"status"`
				} `json:"builds"`
			}
			endpoint, err := miniskyAPIURL("cloudbuild", fmt.Sprintf("/v1/projects/%s/builds", cloudBuildProject))
			if err == nil {
				err = getJSON(endpoint, &data)
			}
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Fprintln(cmd.OutOrStdout(), "CLOUD BUILD HISTORY:")
			for _, b := range data.Builds {
				fmt.Fprintf(cmd.OutOrStdout(), "  - [%s] %s\n", b.Status, b.ID)
			}
		},
	})

	// Vertex AI
	vertexCmd.AddCommand(&cobra.Command{
		Use: "models list",
		Run: func(cmd *cobra.Command, args []string) {
			var data struct {
				Models []string `json:"models"`
			}
			endpoint, err := miniskyAPIURL("aiplatform", "/v1/internal/models")
			if err == nil {
				err = getJSON(endpoint, &data)
			}
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Fprintln(cmd.OutOrStdout(), "AVAILABLE AI MODELS:")
			for _, m := range data.Models {
				fmt.Fprintf(cmd.OutOrStdout(), "  - %s\n", m)
			}
		},
	})

	arCmd.PersistentFlags().StringVar(&artifactProject, "project", "local-dev-project", "GCP project ID")
	arCmd.PersistentFlags().StringVar(&artifactLocation, "location", "us-central1", "Artifact Registry location")
	cbCmd.PersistentFlags().StringVar(&cloudBuildProject, "project", "local-dev-project", "GCP project ID")
	rootCmd.AddCommand(arCmd)
	rootCmd.AddCommand(cbCmd)
	rootCmd.AddCommand(vertexCmd)
}
