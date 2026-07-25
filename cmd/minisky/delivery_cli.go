package main

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

var (
	schedulerProject  string
	schedulerLocation string
)

var schedulerCmd = &cobra.Command{
	Use:   "scheduler",
	Short: "Manage Cloud Scheduler jobs through the API gateway",
}

func init() {
	jobs := &cobra.Command{Use: "jobs", Short: "Manage Scheduler jobs"}
	jobs.AddCommand(&cobra.Command{
		Use: "list",
		RunE: func(cmd *cobra.Command, _ []string) error {
			endpoint, err := miniskyAPIURL("cloudscheduler", fmt.Sprintf(
				"/v1/projects/%s/locations/%s/jobs", schedulerProject, schedulerLocation))
			if err != nil {
				return err
			}
			var response struct {
				Jobs []struct {
					Name  string `json:"name"`
					State string `json:"state"`
				} `json:"jobs"`
			}
			if err := getJSON(endpoint, &response); err != nil {
				return err
			}
			for _, job := range response.Jobs {
				fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\n", job.State, job.Name)
			}
			return nil
		},
	})
	jobs.AddCommand(&cobra.Command{
		Use:  "run JOB_ID",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			jobID := strings.TrimSpace(args[0])
			endpoint, err := miniskyAPIURL("cloudscheduler", fmt.Sprintf(
				"/v1/projects/%s/locations/%s/jobs/%s:run", schedulerProject, schedulerLocation, jobID))
			if err != nil {
				return err
			}
			return requestJSON(http.MethodPost, endpoint, map[string]any{}, nil)
		},
	})
	schedulerCmd.PersistentFlags().StringVar(&schedulerProject, "project", "local-dev-project", "GCP project ID")
	schedulerCmd.PersistentFlags().StringVar(&schedulerLocation, "location", "us-central1", "Scheduler location")
	schedulerCmd.AddCommand(jobs)
	rootCmd.AddCommand(schedulerCmd)
}
