package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	computeProject string
	computeZone    string
)

var computeCmd = &cobra.Command{
	Use:   "compute",
	Short: "Manage Compute Engine resources",
}

var instancesCmd = &cobra.Command{
	Use:   "instances",
	Short: "Manage GCE instances",
}

var listInstancesCmd = &cobra.Command{
	Use:   "list",
	Short: "List GCE instances",
	Run: func(cmd *cobra.Command, args []string) {
		var data struct {
			Items []struct {
				Name   string `json:"name"`
				Status string `json:"status"`
			} `json:"items"`
		}
		endpoint, err := miniskyAPIURL("compute", fmt.Sprintf("/compute/v1/projects/%s/zones/%s/instances", computeProject, computeZone))
		if err == nil {
			err = getJSON(endpoint, &data)
		}
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}

		fmt.Fprintln(cmd.OutOrStdout(), "COMPUTE INSTANCES:")
		if len(data.Items) == 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "  (None)")
			return
		}
		for _, i := range data.Items {
			fmt.Fprintf(cmd.OutOrStdout(), "  - %s [%s]\n", i.Name, i.Status)
		}
	},
}

func init() {
	computeCmd.PersistentFlags().StringVar(&computeProject, "project", "local-dev-project", "GCP project ID")
	computeCmd.PersistentFlags().StringVar(&computeZone, "zone", "us-central1-a", "Compute zone")
	instancesCmd.AddCommand(listInstancesCmd)
	computeCmd.AddCommand(instancesCmd)
	rootCmd.AddCommand(computeCmd)
}
