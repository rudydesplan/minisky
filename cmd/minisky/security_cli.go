package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
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
			port := os.Getenv("MINISKY_UI_PORT")
			if port == "" {
				port = "8081"
			}
			var data struct {
				KeyRings []struct {
					Name string `json:"name"`
				} `json:"keyRings"`
			}
			if err := getJSON(fmt.Sprintf("http://localhost:%s/api/manage/cloudkms/keyRings", port), &data); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Println("KMS KEY RINGS:")
			for _, k := range data.KeyRings {
				fmt.Printf("  - %s\n", k.Name)
			}
		},
	})

	// Secret Manager
	secretCmd.AddCommand(&cobra.Command{
		Use: "list",
		Run: func(cmd *cobra.Command, args []string) {
			port := os.Getenv("MINISKY_UI_PORT")
			if port == "" {
				port = "8081"
			}
			var data struct {
				Secrets []struct {
					Name string `json:"name"`
				} `json:"secrets"`
			}
			if err := getJSON(fmt.Sprintf("http://localhost:%s/api/manage/secretmanager/secrets", port), &data); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Println("SECRET MANAGER SECRETS:")
			for _, s := range data.Secrets {
				fmt.Printf("  - %s\n", s.Name)
			}
		},
	})

	// Cloud Tasks
	tasksCmd.AddCommand(&cobra.Command{
		Use: "queues list",
		Run: func(cmd *cobra.Command, args []string) {
			port := os.Getenv("MINISKY_UI_PORT")
			if port == "" {
				port = "8081"
			}
			var data struct {
				Queues []struct {
					Name  string `json:"name"`
					State string `json:"state"`
				} `json:"queues"`
			}
			if err := getJSON(fmt.Sprintf("http://localhost:%s/api/manage/cloudtasks/queues", port), &data); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
				return
			}
			fmt.Println("CLOUD TASKS QUEUES:")
			for _, q := range data.Queues {
				fmt.Printf("  - %s [%s]\n", q.Name, q.State)
			}
		},
	})

	rootCmd.AddCommand(kmsCmd)
	rootCmd.AddCommand(secretCmd)
	rootCmd.AddCommand(tasksCmd)
}
