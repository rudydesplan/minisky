package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestManagedKafkaPinnedImageUsesResolvedLocalIdentity(t *testing.T) {
	source := readShellScript(t, "phase19-managed-kafka-terraform-integration.sh")
	assertion := shellFunction(t, source, "assert_pinned_image")
	const pinnedImage = "apache/kafka:4.1.0@sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f"

	for _, test := range []struct {
		name       string
		scenario   string
		wantOK     bool
		diagnostic string
	}{
		{name: "classic Docker config identity", scenario: "matching", wantOK: true},
		{name: "different container image", scenario: "mismatch", diagnostic: "Kafka image identity mismatch"},
		{name: "container inspect failure", scenario: "container-inspect-failure", diagnostic: "Failed to inspect Kafka container image"},
		{name: "pinned image inspect failure", scenario: "image-inspect-failure", diagnostic: "Failed to resolve pinned Kafka image"},
	} {
		t.Run(test.name, func(t *testing.T) {
			bin := t.TempDir()
			fakeDocker := `#!/usr/bin/env bash
set -eu
case "${FAKE_DOCKER_SCENARIO}:$*" in
  container-inspect-failure:inspect*) exit 41 ;;
  image-inspect-failure:image\ inspect*) exit 42 ;;
  matching:inspect*|mismatch:inspect*|image-inspect-failure:inspect*) echo sha256:platform-specific-config ;;
  matching:image\ inspect*) echo sha256:platform-specific-config ;;
  mismatch:image\ inspect*) echo sha256:different-config ;;
  *) exit 43 ;;
esac
`
			if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(fakeDocker), 0o700); err != nil {
				t.Fatal(err)
			}
			harness := strings.Join([]string{
				"set -Eeuo pipefail",
				`kafka_image="` + pinnedImage + `"`,
				assertion,
				`assert_pinned_image "exact-owned-container"`,
			}, "\n")
			command := exec.Command("bash", "-c", harness)
			command.Env = append(os.Environ(),
				"FAKE_DOCKER_SCENARIO="+test.scenario,
				"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
			)
			output, err := command.CombinedOutput()
			if test.wantOK {
				if err != nil {
					t.Fatalf("matching platform-specific image identity was rejected: %v\n%s", err, output)
				}
				return
			}
			if err == nil {
				t.Fatalf("%s unexpectedly passed", test.scenario)
			}
			if !strings.Contains(string(output), test.diagnostic) {
				t.Fatalf("%s diagnostic = %q, want %q", test.scenario, output, test.diagnostic)
			}
		})
	}
}
