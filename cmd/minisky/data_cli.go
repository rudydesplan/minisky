package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
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
			port := os.Getenv("MINISKY_UI_PORT")
			if port == "" {
				port = "8081"
			}
			var data struct {
				Clusters []struct {
					Name   string                 `json:"clusterName"`
					Status struct{ State string } `json:"status"`
				} `json:"clusters"`
			}
			if err := getJSON(fmt.Sprintf("http://localhost:%s/api/manage/dataproc/clusters", port), &data); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Println("DATAPROC CLUSTERS:")
			for _, c := range data.Clusters {
				fmt.Printf("  - %s [%s]\n", c.Name, c.Status.State)
			}
		},
	})

	// Bigtable
	bigtableCmd.AddCommand(&cobra.Command{
		Use: "instances list",
		Run: func(cmd *cobra.Command, args []string) {
			port := os.Getenv("MINISKY_UI_PORT")
			if port == "" {
				port = "8081"
			}
			var data struct {
				Instances []struct {
					Name  string `json:"name"`
					State string `json:"state"`
				} `json:"instances"`
			}
			if err := getJSON(fmt.Sprintf("http://localhost:%s/api/manage/bigtable/instances", port), &data); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Println("BIGTABLE INSTANCES:")
			for _, i := range data.Instances {
				fmt.Printf("  - %s [%s]\n", i.Name, i.State)
			}
		},
	})

	rootCmd.AddCommand(dataprocCmd)
	rootCmd.AddCommand(bigtableCmd)
}
