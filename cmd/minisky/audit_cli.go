package main

import (
	"fmt"
	"os"
	"path/filepath"

	"minisky/pkg/config"
	localsecurity "minisky/pkg/security"
	"minisky/pkg/state"

	"github.com/spf13/cobra"
)

func newAuditCommand() *cobra.Command {
	var profile string
	var limit int
	command := &cobra.Command{
		Use:   "audit",
		Short: "Verify or export profile-scoped mutation audit records",
		Long:  "Audit records are local append-only files with tamper-evident hash chaining; they are not immutable compliance storage.",
	}
	command.PersistentFlags().StringVar(&profile, "profile", "", "state profile (defaults to MINISKY_PROFILE or default)")

	open := func() (*localsecurity.AuditLog, error) {
		selected := profile
		if selected == "" {
			selected = config.GetProfile()
		}
		store, err := state.New(config.GetStateDir(), selected)
		if err != nil {
			return nil, err
		}
		return localsecurity.OpenAuditLog(store.ProfileDir(), selected, false)
	}
	verifyCommand := &cobra.Command{
		Use:   "verify",
		Short: "Verify the complete local audit hash chain",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			audit, err := open()
			if err != nil {
				return err
			}
			defer audit.Close()
			if err := audit.Verify(); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "Audit hash chain verified")
			return nil
		},
	}
	exportCommand := &cobra.Command{
		Use:   "export [FILE]",
		Short: "Export the most recent bounded audit records",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			audit, err := open()
			if err != nil {
				return err
			}
			defer audit.Close()
			if len(args) == 0 || args[0] == "-" {
				return audit.Export(cmd.OutOrStdout(), limit)
			}
			return exportAuditFile(audit, args[0], limit)
		},
	}
	exportCommand.Flags().IntVar(&limit, "limit", 100, "maximum records to export (1-10000)")
	command.AddCommand(verifyCommand, exportCommand)
	return command
}

func exportAuditFile(audit *localsecurity.AuditLog, path string, limit int) error {
	directory := filepath.Dir(path)
	temp, err := os.CreateTemp(directory, ".minisky-audit-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary audit export: %w", err)
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if err := audit.Export(temp, limit); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncSnapshotDirectory(directory)
}

func init() {
	rootCmd.AddCommand(newAuditCommand())
}
