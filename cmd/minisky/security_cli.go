package main

import (
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

var (
	kmsProject     string
	kmsLocation    string
	secretsProject string
	tasksProject   string
	tasksLocation  string
)

var kmsCmd = &cobra.Command{
	Use:   "kms",
	Short: "Manage Cloud KMS keys",
}

var secretCmd = &cobra.Command{
	Use:   "secrets",
	Short: "Manage Secret Manager secrets",
}

var tasksCmd = &cobra.Command{
	Use:   "tasks",
	Short: "Manage Cloud Tasks queues",
}

func init() {
	// KMS
	kmsCmd.AddCommand(&cobra.Command{
		Use: "keyrings list",
		Run: func(cmd *cobra.Command, args []string) {
			var data struct {
				KeyRings []struct {
					Name string `json:"name"`
				} `json:"keyRings"`
			}
			endpoint, err := miniskyAPIURL("cloudkms", fmt.Sprintf("/v1/projects/%s/locations/%s/keyRings", kmsProject, kmsLocation))
			if err == nil {
				err = getJSON(endpoint, &data)
			}
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Fprintln(cmd.OutOrStdout(), "KMS KEY RINGS:")
			for _, k := range data.KeyRings {
				fmt.Fprintf(cmd.OutOrStdout(), "  - %s\n", k.Name)
			}
		},
	})

	// Secret Manager
	secretCmd.AddCommand(&cobra.Command{
		Use: "list",
		Run: func(cmd *cobra.Command, args []string) {
			var data struct {
				Secrets []struct {
					Name string `json:"name"`
				} `json:"secrets"`
			}
			endpoint, err := miniskyAPIURL("secretmanager", fmt.Sprintf("/v1/projects/%s/secrets", secretsProject))
			if err == nil {
				err = getJSON(endpoint, &data)
			}
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Fprintln(cmd.OutOrStdout(), "SECRET MANAGER SECRETS:")
			for _, s := range data.Secrets {
				fmt.Fprintf(cmd.OutOrStdout(), "  - %s\n", s.Name)
			}
		},
	})

	// Cloud Tasks
	tasksCmd.AddCommand(&cobra.Command{
		Use: "queues list",
		Run: func(cmd *cobra.Command, args []string) {
			var data struct {
				Queues []struct {
					Name  string `json:"name"`
					State string `json:"state"`
				} `json:"queues"`
			}
			endpoint, err := miniskyAPIURL("cloudtasks", fmt.Sprintf("/v2/projects/%s/locations/%s/queues", tasksProject, tasksLocation))
			if err == nil {
				err = getJSON(endpoint, &data)
			}
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Fprintln(cmd.OutOrStdout(), "CLOUD TASKS QUEUES:")
			for _, q := range data.Queues {
				fmt.Fprintf(cmd.OutOrStdout(), "  - %s [%s]\n", q.Name, q.State)
			}
		},
	})
	tasksCmd.AddCommand(&cobra.Command{
		Use:  "queue-create QUEUE_ID",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := miniskyAPIURL("cloudtasks", fmt.Sprintf("/v2/projects/%s/locations/%s/queues", tasksProject, tasksLocation))
			if err != nil {
				return err
			}
			name := fmt.Sprintf("projects/%s/locations/%s/queues/%s", tasksProject, tasksLocation, args[0])
			return postJSON(endpoint, map[string]any{"name": name}, nil)
		},
	})
	tasksCmd.AddCommand(&cobra.Command{
		Use:  "enqueue QUEUE_ID URL [BODY]",
		Args: cobra.RangeArgs(2, 3),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := miniskyAPIURL("cloudtasks", fmt.Sprintf(
				"/v2/projects/%s/locations/%s/queues/%s/tasks", tasksProject, tasksLocation, args[0]))
			if err != nil {
				return err
			}
			body := ""
			if len(args) == 3 {
				body = base64.StdEncoding.EncodeToString([]byte(args[2]))
			}
			return postJSON(endpoint, map[string]any{"task": map[string]any{"httpRequest": map[string]any{
				"url": args[1], "httpMethod": http.MethodPost, "body": body,
			}}}, nil)
		},
	})

	kmsCmd.PersistentFlags().StringVar(&kmsProject, "project", "local-dev-project", "GCP project ID")
	kmsCmd.PersistentFlags().StringVar(&kmsLocation, "location", "global", "Cloud KMS location")
	secretCmd.PersistentFlags().StringVar(&secretsProject, "project", "local-dev-project", "GCP project ID")
	tasksCmd.PersistentFlags().StringVar(&tasksProject, "project", "local-dev-project", "GCP project ID")
	tasksCmd.PersistentFlags().StringVar(&tasksLocation, "location", "us-central1", "Cloud Tasks location")
	rootCmd.AddCommand(kmsCmd)
	rootCmd.AddCommand(secretCmd)
	rootCmd.AddCommand(tasksCmd)
}
