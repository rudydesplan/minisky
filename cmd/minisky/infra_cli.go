package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	gkeProject  string
	gkeLocation string
	sqlProject  string
)

var gkeCmd = &cobra.Command{
	Use:   "gke",
	Short: "Manage Kubernetes clusters",
}

var sqlCmd = &cobra.Command{
	Use:   "sql",
	Short: "Manage Cloud SQL instances",
}

func init() {
	// GKE
	gkeCmd.AddCommand(&cobra.Command{
		Use: "clusters list",
		Run: func(cmd *cobra.Command, args []string) {
			var data struct {
				Clusters []struct {
					Name   string `json:"name"`
					Status string `json:"status"`
				} `json:"clusters"`
			}
			endpoint, err := miniskyAPIURL("container", fmt.Sprintf("/v1/projects/%s/locations/%s/clusters", gkeProject, gkeLocation))
			if err == nil {
				err = getJSON(endpoint, &data)
			}
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Fprintln(cmd.OutOrStdout(), "GKE CLUSTERS:")
			for _, c := range data.Clusters {
				fmt.Fprintf(cmd.OutOrStdout(), "  - %s [%s]\n", c.Name, c.Status)
			}
		},
	})

	// SQL
	sqlCmd.AddCommand(&cobra.Command{
		Use: "instances list",
		Run: func(cmd *cobra.Command, args []string) {
			var data struct {
				Items []struct {
					Name  string `json:"name"`
					State string `json:"state"`
				} `json:"items"`
			}
			endpoint, err := miniskyAPIURL("sqladmin", fmt.Sprintf("/v1/projects/%s/instances", sqlProject))
			if err == nil {
				err = getJSON(endpoint, &data)
			}
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Fprintln(cmd.OutOrStdout(), "CLOUDSQL INSTANCES:")
			for _, i := range data.Items {
				fmt.Fprintf(cmd.OutOrStdout(), "  - %s [%s]\n", i.Name, i.State)
			}
		},
	})

	gkeCmd.PersistentFlags().StringVar(&gkeProject, "project", "local-dev-project", "GCP project ID")
	gkeCmd.PersistentFlags().StringVar(&gkeLocation, "location", "us-central1-a", "GKE location")
	sqlCmd.PersistentFlags().StringVar(&sqlProject, "project", "local-dev-project", "GCP project ID")
	rootCmd.AddCommand(gkeCmd)
	rootCmd.AddCommand(sqlCmd)
}
