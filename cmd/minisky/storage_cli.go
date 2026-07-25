package main

import (
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
)

var storageProject string

var storageCmd = &cobra.Command{
	Use:   "storage",
	Short: "Manage Cloud Storage resources",
}

var storageBucketsCmd = &cobra.Command{
	Use:   "buckets",
	Short: "Manage storage buckets",
}

var listBucketsCmd = &cobra.Command{
	Use:   "list",
	Short: "List storage buckets",
	Run: func(cmd *cobra.Command, args []string) {
		var data struct {
			Items []struct {
				Name string `json:"name"`
			} `json:"items"`
		}
		endpoint, err := miniskyAPIURL("storage", "/storage/v1/b?project="+url.QueryEscape(storageProject))
		if err == nil {
			err = getJSON(endpoint, &data)
		}
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}

		fmt.Fprintln(cmd.OutOrStdout(), "STORAGE BUCKETS:")
		if len(data.Items) == 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "  (None)")
			return
		}
		for _, b := range data.Items {
			fmt.Fprintf(cmd.OutOrStdout(), "  - gs://%s\n", b.Name)
		}
	},
}

func init() {
	storageCmd.PersistentFlags().StringVar(&storageProject, "project", "local-dev-project", "GCP project ID")
	storageBucketsCmd.AddCommand(listBucketsCmd)
	storageCmd.AddCommand(storageBucketsCmd)
	rootCmd.AddCommand(storageCmd)
}
