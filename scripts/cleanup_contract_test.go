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

func TestStorageTransferCleanupUsesContainedOwnedWorkDirectory(t *testing.T) {
	source := readShellScript(t, "phase20-storage-transfer-terraform-integration.sh")
	cleanup := shellFunction(t, source, "cleanup")
	if !strings.Contains(cleanup, "remove_owned_work_directory") {
		t.Fatal("Storage Transfer cleanup does not use the contained work-directory remover")
	}
	if strings.Contains(cleanup, `rm -rf "${work}"`) {
		t.Fatal("Storage Transfer cleanup still removes the mutable work path directly")
	}

	remove := shellFunction(t, source, "remove_owned_work_directory")
	parent := t.TempDir()
	ownedWork := filepath.Join(parent, "owned-work")
	outside := filepath.Join(parent, "outside")
	for _, path := range []string{
		filepath.Join(ownedWork, "state", "profiles", "phase20-transfer-tf-test", "runtime", "storage", "source"),
		outside,
	} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	outsideMarker := filepath.Join(outside, "must-remain")
	if err := os.WriteFile(outsideMarker, []byte("preserved"), 0o600); err != nil {
		t.Fatal(err)
	}

	run := func(owned, work, state string, wantSuccess bool) {
		t.Helper()
		harness := strings.Join([]string{
			"set -Eeuo pipefail",
			`profile="phase20-transfer-tf-test"`,
			`owned_work="$1"`,
			`work="$2"`,
			`state="$3"`,
			remove,
			"remove_owned_work_directory",
		}, "\n")
		command := exec.Command("bash", "-c", harness, "cleanup-test", owned, work, state)
		output, err := command.CombinedOutput()
		if wantSuccess && err != nil {
			t.Fatalf("contained cleanup failed: %v\n%s", err, output)
		}
		if !wantSuccess && err == nil {
			t.Fatalf("out-of-root cleanup succeeded:\n%s", output)
		}
	}

	run(ownedWork, outside, filepath.Join(outside, "state"), false)
	if _, err := os.Stat(outsideMarker); err != nil {
		t.Fatalf("out-of-root cleanup changed the sentinel: %v", err)
	}

	symlinkWork := filepath.Join(parent, "symlink-work")
	if err := os.Mkdir(symlinkWork, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(symlinkWork, "state")); err != nil {
		t.Fatal(err)
	}
	run(symlinkWork, symlinkWork, filepath.Join(symlinkWork, "state"), false)
	if _, err := os.Stat(outsideMarker); err != nil {
		t.Fatalf("symlinked-state cleanup changed the outside sentinel: %v", err)
	}

	run(ownedWork, ownedWork, filepath.Join(ownedWork, "state"), true)
	if _, err := os.Stat(ownedWork); !os.IsNotExist(err) {
		t.Fatalf("owned work directory remains after cleanup: %v", err)
	}
	if _, err := os.Stat(outsideMarker); err != nil {
		t.Fatalf("contained cleanup changed the outside sentinel: %v", err)
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
		marker = name + "(){"
		start = strings.Index(source, marker)
	}
	if start < 0 {
		t.Fatalf("%s function was not found", name)
	}
	end := strings.Index(source[start:], "\n}\n")
	if end < 0 {
		t.Fatalf("end of %s was not found", marker)
	}
	return source[start : start+end+2]
}
