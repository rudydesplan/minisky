package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var logsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Manage Cloud Logging",
}

var tailLogsCmd = &cobra.Command{
	Use:   "tail",
	Short: "Tail logs in real-time from MiniSky",
	Run: func(cmd *cobra.Command, args []string) {
		endpoint, err := miniskyAPIURL("logging", "/v2/entries:list")
		if err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			return
		}

		fmt.Fprintln(cmd.OutOrStdout(), "🛰️  Streaming MiniSky logs (Ctrl+C to stop)...")
		lastSeenId := ""

		for {
			var response struct {
				Entries []map[string]interface{} `json:"entries"`
			}
			err := postJSON(endpoint, map[string]interface{}{}, &response)
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}

			// Print new entries
			foundLast := lastSeenId == ""
			newLastSeen := lastSeenId

			for i := len(response.Entries) - 1; i >= 0; i-- {
				e := response.Entries[i]
				id := e["insertId"].(string)

				if id == lastSeenId {
					foundLast = true
					continue
				}

				if foundLast {
					ts := e["timestamp"].(string)
					severity := e["severity"].(string)
					text := e["textPayload"].(string)
					res := e["resource"].(map[string]interface{})
					labels := res["labels"].(map[string]interface{})
					name := labels["name"].(string)

					fmt.Fprintf(cmd.OutOrStdout(), "[%s] %s | %s: %s\n", ts, severity, name, text)
					newLastSeen = id
				}
			}
			lastSeenId = newLastSeen
			time.Sleep(1 * time.Second)
		}
	},
}

func init() {
	logsCmd.AddCommand(tailLogsCmd)
	rootCmd.AddCommand(logsCmd)
}
