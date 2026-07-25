package main

import (
	"os"
	"testing"

	"github.com/spf13/cobra"
)

func TestCLIProjectDefaultPersistsSecurelyAndEnvOverrides(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "project-test")
	t.Setenv("MINISKY_PROJECT_ID", "")
	if err := saveCLIProjectConfig(cliProjectConfig{DefaultProject: "saved-project"}); err != nil {
		t.Fatal(err)
	}
	if got := activeProjectID(); got != "saved-project" {
		t.Fatalf("active project = %q", got)
	}
	info, err := os.Stat(cliProjectConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %o", info.Mode().Perm())
	}
	t.Setenv("MINISKY_PROJECT_ID", "environment-project")
	if got := activeProjectID(); got != "environment-project" {
		t.Fatalf("environment override = %q", got)
	}
}

func TestPersistedProjectBecomesServiceFlagDefaultButExplicitFlagWins(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "flag-test")
	t.Setenv("MINISKY_PROJECT_ID", "")
	if err := saveCLIProjectConfig(cliProjectConfig{DefaultProject: "saved-project"}); err != nil {
		t.Fatal(err)
	}
	command := &cobra.Command{Use: "service"}
	var project string
	command.Flags().StringVar(&project, "project", defaultProject, "")
	if err := rootCmd.PersistentPreRunE(command, nil); err != nil {
		t.Fatal(err)
	}
	if project != "saved-project" {
		t.Fatalf("project default = %q", project)
	}
	if err := command.Flags().Set("project", "explicit-project"); err != nil {
		t.Fatal(err)
	}
	if err := rootCmd.PersistentPreRunE(command, nil); err != nil {
		t.Fatal(err)
	}
	if project != "explicit-project" {
		t.Fatalf("explicit project = %q", project)
	}
}
