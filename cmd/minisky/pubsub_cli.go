package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var pubsubProject string

var pubsubCmd = &cobra.Command{
	Use:   "pubsub",
	Short: "Manage Pub/Sub resources",
}

var topicsCmd = &cobra.Command{
	Use:   "topics",
	Short: "Manage Pub/Sub topics",
}

var listTopicsCmd = &cobra.Command{
	Use:   "list",
	Short: "List Pub/Sub topics",
	Run: func(cmd *cobra.Command, args []string) {
		var data struct {
			Topics []struct {
				Name string `json:"name"`
			} `json:"topics"`
		}
		endpoint, err := miniskyAPIURL("pubsub", fmt.Sprintf("/v1/projects/%s/topics", pubsubProject))
		if err == nil {
			err = getJSON(endpoint, &data)
		}
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}

		fmt.Fprintln(cmd.OutOrStdout(), "PUB/SUB TOPICS:")
		if len(data.Topics) == 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "  (None)")
			return
		}
		for _, t := range data.Topics {
			fmt.Fprintf(cmd.OutOrStdout(), "  - %s\n", t.Name)
		}
	},
}

func init() {
	pubsubCmd.PersistentFlags().StringVar(&pubsubProject, "project", "local-dev-project", "GCP project ID")
	topicsCmd.AddCommand(listTopicsCmd)
	pubsubCmd.AddCommand(topicsCmd)
	rootCmd.AddCommand(pubsubCmd)
}
