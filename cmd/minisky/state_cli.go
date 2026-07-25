package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"minisky/pkg/config"
	statepkg "minisky/pkg/state"
)

var stateCmd = newStateCommand("")

func init() {
	rootCmd.AddCommand(stateCmd)
}

func newStateCommand(stateRoot string) *cobra.Command {
	var profile string
	command := &cobra.Command{
		Use:   "state",
		Short: "Export or import persistent metadata",
		Long: "Export or import portable MiniSky metadata snapshots.\n" +
			"DuckDB database files and other binary service data are deliberately excluded.",
	}
	command.PersistentFlags().StringVar(&profile, "profile", "", "state profile (defaults to MINISKY_PROFILE or default)")

	exportCommand := &cobra.Command{
		Use:   "export [FILE]",
		Short: "Export a portable metadata snapshot",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := openStateStore(stateRoot, profile)
			if err != nil {
				return err
			}
			path := "-"
			if len(args) == 1 {
				path = args[0]
			}
			if path == "-" {
				return store.Export(cmd.OutOrStdout())
			}
			return exportStateFile(store, path)
		},
	}

	importCommand := &cobra.Command{
		Use:   "import [FILE]",
		Short: "Atomically import a portable metadata snapshot",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			store, err := openStateStore(stateRoot, profile)
			if err != nil {
				return err
			}
			path := "-"
			if len(args) == 1 {
				path = args[0]
			}
			if path == "-" {
				return store.Import(cmd.InOrStdin())
			}
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("open state snapshot: %w", err)
			}
			defer file.Close()
			return store.Import(file)
		},
	}

	command.AddCommand(exportCommand, importCommand)
	return command
}

func openStateStore(root, profile string) (*statepkg.Store, error) {
	if root == "" {
		root = config.GetStateDir()
	}
	if profile == "" {
		profile = config.GetProfile()
	}
	store, err := statepkg.New(root, profile)
	if err != nil {
		return nil, fmt.Errorf("open state profile: %w", err)
	}
	return store, nil
}

func exportStateFile(store *statepkg.Store, path string) error {
	directory := filepath.Dir(path)
	temp, err := os.CreateTemp(directory, ".minisky-state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary snapshot: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)

	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return fmt.Errorf("secure temporary snapshot: %w", err)
	}
	if err := store.Export(temp); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return fmt.Errorf("sync state snapshot: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close state snapshot: %w", err)
	}
	if err := os.Rename(tempName, path); err != nil {
		return fmt.Errorf("replace state snapshot: %w", err)
	}
	return syncSnapshotDirectory(directory)
}

func syncSnapshotDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open snapshot directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync snapshot directory: %w", err)
	}
	return nil
}
