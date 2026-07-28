package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const presenceOnlyCommandExit = 97

func presenceOnlyRequiredCommands(t *testing.T, names ...string) string {
	t.Helper()

	bin := t.TempDir()
	for _, name := range names {
		source := fmt.Sprintf(`#!/bin/bash
printf 'Unexpected invocation of presence-only fake command %s:' >&2
printf ' %%q' "$@" >&2
printf '\n' >&2
exit %d
`, name, presenceOnlyCommandExit)
		if err := os.WriteFile(filepath.Join(bin, name), []byte(source), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return bin
}

func TestPresenceOnlyTerraformCannotRunLifecycleCommands(t *testing.T) {
	bin := presenceOnlyRequiredCommands(t, "terraform")
	for _, arguments := range [][]string{
		{"init"},
		{"apply", "-auto-approve"},
		{"destroy", "-auto-approve"},
	} {
		command := exec.Command(filepath.Join(bin, "terraform"), arguments...)
		output, err := command.CombinedOutput()
		exit, ok := err.(*exec.ExitError)
		if !ok || exit.ExitCode() != presenceOnlyCommandExit {
			t.Fatalf("fake terraform %v exit=%v, want %d\n%s",
				arguments, err, presenceOnlyCommandExit, output)
		}
		if !strings.Contains(string(output), "Unexpected invocation of presence-only fake command terraform") {
			t.Fatalf("fake terraform %v failure is not actionable:\n%s", arguments, output)
		}
	}
}

func TestIntegrationScriptsReportMissingNonTerraformPrerequisites(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		script  string
		optIn   string
		present []string
		missing string
	}{
		{
			name:    "Memcache missing go",
			script:  "memcache-integration.sh",
			optIn:   "MINISKY_MEMCACHE_INTEGRATION=1",
			present: []string{"curl", "docker"},
			missing: "go",
		},
		{
			name:    "Memcache missing python3",
			script:  "memcache-integration.sh",
			optIn:   "MINISKY_MEMCACHE_INTEGRATION=1",
			present: []string{"curl", "docker", "go"},
			missing: "python3",
		},
		{
			name:    "Binary Authorization missing go",
			script:  "phase25-binary-authorization-terraform-integration.sh",
			optIn:   "MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION=1",
			present: []string{"curl"},
			missing: "go",
		},
		{
			name:    "Binary Authorization missing python3",
			script:  "phase25-binary-authorization-terraform-integration.sh",
			optIn:   "MINISKY_PHASE25_BINARY_AUTHORIZATION_TERRAFORM_INTEGRATION=1",
			present: []string{"curl", "go"},
			missing: "python3",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bin := presenceOnlyRequiredCommands(t, test.present...)
			command := exec.Command(bash, test.script)
			command.Env = append(
				withoutEnvironment(os.Environ(), "PATH"),
				test.optIn,
				"PATH="+bin,
			)
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatalf("missing %s was accepted:\n%s", test.missing, output)
			}
			want := "Required command not found: " + test.missing
			if !strings.Contains(string(output), want) {
				t.Fatalf("missing %s error is not actionable:\n%s", test.missing, output)
			}
		})
	}
}
