package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestComposerImageIdentityUsesCanonicalReferenceAndRepoDigest(t *testing.T) {
	script := "./phase19-composer-terraform-integration.sh"
	output, err := exec.Command(script, "--print-required-images").CombinedOutput()
	if err != nil {
		t.Fatalf("print Composer image: %v\n%s", err, output)
	}
	image := strings.TrimSpace(string(output))
	at := strings.LastIndex(image, "@")
	if at < 0 {
		t.Fatalf("unexpected canonical Composer image %q", image)
	}
	tagged := image[:at]
	colon := strings.LastIndex(tagged, ":")
	if colon < strings.LastIndex(tagged, "/") {
		t.Fatalf("unexpected canonical Composer image %q", image)
	}
	repoDigest := image[:colon] + image[at:]

	bin := t.TempDir()
	dockerLog := filepath.Join(t.TempDir(), "docker.log")
	fakeDocker := filepath.Join(bin, "docker")
	if err := os.WriteFile(fakeDocker, []byte(`#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >>"${DOCKER_LOG}"
if [[ "$1" == "inspect" && "$2" == "--format" && "$3" == "{{.Config.Image}}" ]]; then
  printf '%s\n' "${AIRFLOW_IMAGE}"
elif [[ "$1" == "inspect" && "$2" == "--format" && "$3" == "{{.Image}}" ]]; then
  printf '%s\n' "sha256:b6e72f8ddc684ec9f9133042168afaab3d37a64d0dfe29e4c86727aa6c788c85"
elif [[ "$1" == "image" && "$2" == "inspect" && "$3" == "--format" && "$4" == "{{json .RepoDigests}}" ]]; then
  printf '["%s"]\n' "${AIRFLOW_REPO_DIGEST}"
else
  exit 97
fi
`), 0o700); err != nil {
		t.Fatal(err)
	}
	run := func(digest string) ([]byte, error) {
		command := exec.Command(script, "--verify-container-image", "composer-container")
		command.Env = append(os.Environ(),
			"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
			"DOCKER_LOG="+dockerLog,
			"AIRFLOW_IMAGE="+image,
			"AIRFLOW_REPO_DIGEST="+digest,
		)
		return command.CombinedOutput()
	}

	if output, err = run(repoDigest); err != nil {
		t.Fatalf("portable Composer image proof failed: %v\n%s", err, output)
	}
	logBytes, err := os.ReadFile(dockerLog)
	if err != nil {
		t.Fatal(err)
	}
	logText := string(logBytes)
	if strings.Contains(logText, "{{.Image}}") {
		t.Fatalf("Composer proof compared the architecture-specific image ID:\n%s", logText)
	}
	for _, required := range []string{"{{.Config.Image}}", "{{json .RepoDigests}}"} {
		if !strings.Contains(logText, required) {
			t.Fatalf("Composer proof did not inspect %s:\n%s", required, logText)
		}
	}

	if output, err = run("apache/airflow@sha256:" + strings.Repeat("f", 64)); err == nil ||
		!strings.Contains(string(output), "Airflow repository digest mismatch") {
		t.Fatalf("wrong repository digest was accepted: err=%v\n%s", err, output)
	}
}

func TestComposerRestartGateRequiresHealthyOwnedReconciliation(t *testing.T) {
	sourceBytes, err := os.ReadFile("phase19-composer-terraform-integration.sh")
	if err != nil {
		t.Fatal(err)
	}
	source := string(sourceBytes)
	start := strings.Index(source, "start_daemon restart")
	end := strings.Index(source, "terraform -chdir=\"${terraform_dir}\" state rm")
	if start < 0 || end <= start {
		t.Fatal("Composer restart verification block is missing")
	}
	restartBlock := source[start:end]
	required := []string{
		"start_daemon restart",
		"set_vars",
		"assert_environment RUNNING",
		"assert_backend",
		"assert_no_drift",
	}
	position := 0
	for _, statement := range required {
		next := strings.Index(restartBlock[position:], statement)
		if next < 0 {
			t.Fatalf("Composer restart block does not require %q:\n%s", statement, restartBlock)
		}
		position += next + len(statement)
	}
	if strings.Contains(restartBlock, "assert_environment ERROR") {
		t.Fatalf("healthy exact-owned restart is incorrectly required to fail closed:\n%s", restartBlock)
	}
}
