package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	dataprocProject string
	dataprocRegion  string
	bigtableProject string
)

var dataprocCmd = &cobra.Command{
	Use:   "dataproc",
	Short: "Manage Dataproc clusters",
}

var bigtableCmd = &cobra.Command{
	Use:   "bigtable",
	Short: "Manage Bigtable instances",
}

func init() {
	// Dataproc
	dataprocCmd.AddCommand(&cobra.Command{
		Use: "clusters list",
		Run: func(cmd *cobra.Command, args []string) {
			var data struct {
				Clusters []struct {
					Name   string                 `json:"clusterName"`
					Status struct{ State string } `json:"status"`
				} `json:"clusters"`
			}
			endpoint, err := miniskyAPIURL("dataproc", fmt.Sprintf("/v1/projects/%s/regions/%s/clusters", dataprocProject, dataprocRegion))
			if err == nil {
				err = getJSON(endpoint, &data)
			}
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Fprintln(cmd.OutOrStdout(), "DATAPROC CLUSTERS:")
			for _, c := range data.Clusters {
				fmt.Fprintf(cmd.OutOrStdout(), "  - %s [%s]\n", c.Name, c.Status.State)
			}
		},
	})

	// Bigtable
	bigtableCmd.AddCommand(&cobra.Command{
		Use: "instances list",
		Run: func(cmd *cobra.Command, args []string) {
			var data struct {
				Instances []struct {
					Name  string `json:"name"`
					State string `json:"state"`
				} `json:"instances"`
			}
			endpoint, err := miniskyAPIURL("bigtableadmin", fmt.Sprintf("/v2/projects/%s/instances", bigtableProject))
			if err == nil {
				err = getJSON(endpoint, &data)
			}
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Fprintln(cmd.OutOrStdout(), "BIGTABLE INSTANCES:")
			for _, i := range data.Instances {
				fmt.Fprintf(cmd.OutOrStdout(), "  - %s [%s]\n", i.Name, i.State)
			}
		},
	})

	dataprocCmd.PersistentFlags().StringVar(&dataprocProject, "project", "local-dev-project", "GCP project ID")
	dataprocCmd.PersistentFlags().StringVar(&dataprocRegion, "region", "us-central1", "Dataproc region")
	bigtableCmd.PersistentFlags().StringVar(&bigtableProject, "project", "local-dev-project", "GCP project ID")
	rootCmd.AddCommand(dataprocCmd)
	rootCmd.AddCommand(bigtableCmd)
}
