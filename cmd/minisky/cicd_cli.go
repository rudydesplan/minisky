package main

import (
	"fmt"
	"net/http"

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
	arCmd.AddCommand(&cobra.Command{
		Use:  "repository-create REPOSITORY_ID",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := miniskyAPIURL("artifactregistry", fmt.Sprintf(
				"/v1/projects/%s/locations/%s/repositories?repositoryId=%s", artifactProject, artifactLocation, args[0]))
			if err != nil {
				return err
			}
			return postJSON(endpoint, map[string]any{"format": "DOCKER"}, nil)
		},
	})
	arCmd.AddCommand(&cobra.Command{
		Use:  "repository-delete REPOSITORY_ID",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := miniskyAPIURL("artifactregistry", fmt.Sprintf(
				"/v1/projects/%s/locations/%s/repositories/%s", artifactProject, artifactLocation, args[0]))
			if err != nil {
				return err
			}
			return requestJSON(http.MethodDelete, endpoint, nil, nil)
		},
	})
	arCmd.AddCommand(&cobra.Command{
		Use:  "packages REPOSITORY_ID",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := miniskyAPIURL("artifactregistry", fmt.Sprintf(
				"/v1/projects/%s/locations/%s/repositories/%s/packages", artifactProject, artifactLocation, args[0]))
			if err != nil {
				return err
			}
			var response struct {
				Packages []struct {
					Name string `json:"name"`
				} `json:"packages"`
			}
			if err := getJSON(endpoint, &response); err != nil {
				return err
			}
			for _, pkg := range response.Packages {
				fmt.Fprintln(cmd.OutOrStdout(), pkg.Name)
			}
			return nil
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
