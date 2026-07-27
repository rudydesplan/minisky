package main

import (
	"fmt"
	"time"

	"minisky/pkg/config"

	"github.com/spf13/cobra"
)

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stops the MiniSky Daemon",
	RunE: func(cmd *cobra.Command, args []string) error {
		identity, err := runningDaemonIdentity(config.GetStateDir(), config.GetProfile())
		if err != nil {
			return fmt.Errorf("MiniSky is not running: %w", err)
		}
		fmt.Printf("Stopping MiniSky (PID %d)...\n", identity.PID)
		if err := signalDaemon(identity); err != nil {
			return fmt.Errorf("signal authenticated daemon PID %d: %w", identity.PID, err)
		}
		if err := waitForDaemonExit(identity, 15*time.Second); err != nil {
			return err
		}
		if err := waitForProfileRelease(config.GetStateDir(), config.GetProfile(), 15*time.Second); err != nil {
			return err
		}
		fmt.Println("✅ MiniSky stopped.")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(stopCmd)
}
