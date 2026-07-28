package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestIntegrationCleanupRepairsExactOwnedContainersBeforeDaemonTermination(t *testing.T) {
	for _, script := range []string{
		"event-delivery-integration.sh",
		"phase15-emulator-integration.sh",
	} {
		t.Run(script, func(t *testing.T) {
			source := readShellScript(t, script)
			owned := shellFunction(t, source, "owned_containers")
			for _, filter := range []string{
				`--filter "label=managed-by=minisky"`,
				`--filter "label=minisky.profile=${profile}"`,
			} {
				if !strings.Contains(owned, filter) {
					t.Fatalf("owned_containers does not require exact ownership filter %q", filter)
				}
			}

			cleanup := shellFunction(t, source, "cleanup")
			repair := strings.Index(cleanup, "repair_owned_container_permissions")
			terminate := strings.Index(cleanup, "kill -TERM")
			wait := strings.Index(cleanup, `wait "${`)
			if repair < 0 {
				t.Fatal("cleanup does not repair owned container permissions")
			}
			if terminate < 0 || wait < 0 {
				t.Fatal("cleanup does not terminate and wait for MiniSky")
			}
			if repair > terminate || repair > wait {
				t.Fatal("permission repair occurs after MiniSky termination begins")
			}
			if !strings.Contains(cleanup, "done < <(owned_containers)") {
				t.Fatal("cleanup removal does not enumerate exact-owned containers")
			}
		})
	}
}

func TestPermissionRepairIsExactOwnedIdempotentAndBestEffort(t *testing.T) {
	for _, script := range []string{
		"event-delivery-integration.sh",
		"phase15-emulator-integration.sh",
	} {
		t.Run(script, func(t *testing.T) {
			source := readShellScript(t, script)
			harness := strings.Join([]string{
				"set -Eeuo pipefail",
				`profile="profile-under-test"`,
				shellFunction(t, source, "owned_containers"),
				shellFunction(t, source, "repair_owned_container_permissions"),
				"repair_owned_container_permissions",
				"repair_owned_container_permissions",
			}, "\n")

			bin := t.TempDir()
			logPath := filepath.Join(bin, "docker.log")
			fakeDocker := `#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >>"${FAKE_DOCKER_LOG}"
if [[ "$1 $2" == "ps -aq" ]]; then
  if [[ "$*" == "ps -aq --filter label=managed-by=minisky --filter label=minisky.profile=profile-under-test" ]]; then
    echo exact-owned-container
  else
    echo unrelated-container
  fi
  exit 0
fi
if [[ "$1 $2" == "exec exact-owned-container" ]]; then
  exit 73
fi
exit 74
`
			if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(fakeDocker), 0o700); err != nil {
				t.Fatal(err)
			}

			command := exec.Command("bash", "-c", harness)
			command.Env = append(os.Environ(),
				"FAKE_DOCKER_LOG="+logPath,
				"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
			)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("permission repair was not idempotent and best-effort: %v\n%s", err, output)
			}

			log, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			got := string(log)
			if strings.Contains(got, "unrelated-container") {
				t.Fatalf("permission repair targeted an unrelated container:\n%s", got)
			}
			if count := strings.Count(got, "exec exact-owned-container sh -c"); count != 2 {
				t.Fatalf("exact-owned permission repair count = %d, want 2:\n%s", count, got)
			}
			if count := strings.Count(got, "ps -aq --filter label=managed-by=minisky --filter label=minisky.profile=profile-under-test"); count != 2 {
				t.Fatalf("exact-owned inventory count = %d, want 2:\n%s", count, got)
			}
		})
	}
}

func readShellScript(t *testing.T, name string) string {
	t.Helper()
	source, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(source)
}

func shellFunction(t *testing.T, source, name string) string {
	t.Helper()
	marker := name + "() {"
	start := strings.Index(source, marker)
	if start < 0 {
		t.Fatalf("%s was not found", marker)
	}
	end := strings.Index(source[start:], "\n}\n")
	if end < 0 {
		t.Fatalf("end of %s was not found", marker)
	}
	return source[start : start+end+2]
}
