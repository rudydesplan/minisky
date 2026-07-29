package main

import (
	"strings"
	"testing"
)

func TestCIQualityWorkRunsInParallelAfterUIBuild(t *testing.T) {
	workflow := readWorkflow(t, "ci.yml")
	jobs := mustMap(workflow["jobs"])

	uiJob := mustMap(jobs["quality-ui"])
	if uiJob == nil {
		t.Fatal("CI is missing quality-ui")
	}
	if _, found := uiJob["needs"]; found {
		t.Fatal("quality-ui must start immediately")
	}
	for _, command := range []string{
		"npm ci",
		"npm run lint",
		"npm test",
		"npm audit --audit-level=high",
		"npm run build",
	} {
		if !jobHasActiveCommand(uiJob, command) {
			t.Errorf("quality-ui lacks active command %q", command)
		}
	}
	upload := firstStepUsing(uiJob, uploadAction)
	if upload == nil {
		t.Fatal("quality-ui does not upload ui-dist")
	}
	uploadWith := mustMap(upload["with"])
	if scalarString(uploadWith["name"]) != "ui-dist" ||
		scalarString(uploadWith["path"]) != "ui/dist" ||
		scalarString(uploadWith["if-no-files-found"]) != "error" {
		t.Fatalf("quality-ui artifact contract is incorrect: %v", uploadWith)
	}

	parallelJobs := map[string][]string{
		"quality-go-static": {
			"make check-docs-truth",
			`test -z "$(gofmt -l cmd pkg ui)"`,
			"go vet ./cmd/... ./pkg/... ./ui",
		},
		"quality-go-race": {
			"go test -race ./cmd/... ./pkg/... ./ui",
		},
		"quality-scripts": {
			"go test -count=1 ./scripts",
		},
		"quality-build": {
			"go build -trimpath ./cmd/minisky",
			"node --test .github/actions/setup-minisky/index.test.mjs && node --check .github/actions/setup-minisky/index.mjs && node --check .github/actions/setup-minisky/cleanup.mjs",
			"docker compose -f deployments/docker-compose.yml config >/dev/null",
		},
	}
	for name, commands := range parallelJobs {
		job := mustMap(jobs[name])
		if job == nil {
			t.Errorf("CI is missing %s", name)
			continue
		}
		if !sameStringSet(stringSlice(job["needs"]), []string{"quality-ui"}) {
			t.Errorf("%s needs = %v, want [quality-ui]", name, stringSlice(job["needs"]))
		}
		download := firstStepUsing(job, promotionDownloadAction)
		if download == nil {
			t.Errorf("%s does not download ui-dist", name)
		} else {
			with := mustMap(download["with"])
			if scalarString(with["name"]) != "ui-dist" || scalarString(with["path"]) != "ui/dist" {
				t.Errorf("%s downloads the wrong UI artifact: %v", name, with)
			}
		}
		for _, command := range commands {
			if !jobHasActiveCommand(job, command) {
				t.Errorf("%s lacks active command %q", name, command)
			}
		}
	}

	aggregate := mustMap(jobs["quality"])
	if aggregate == nil {
		t.Fatal("CI is missing aggregate quality gate")
	}
	if !sameStringSet(stringSlice(aggregate["needs"]),
		[]string{
			"quality-ui",
			"quality-go-static",
			"quality-go-race",
			"quality-scripts",
			"quality-build",
			"windows-state-markers",
		}) {
		t.Fatalf("aggregate quality needs = %v", stringSlice(aggregate["needs"]))
	}
	if !conditionIsAlways(aggregate["if"]) {
		t.Fatalf("aggregate quality condition = %q, want always()", scalarString(aggregate["if"]))
	}
	for _, result := range []string{
		"needs.quality-ui.result",
		"needs.quality-go-static.result",
		"needs.quality-go-race.result",
		"needs.quality-scripts.result",
		"needs.quality-build.result",
		"needs.windows-state-markers.result",
	} {
		if !strings.Contains(allActiveJobCommands(aggregate), result) {
			t.Errorf("aggregate quality does not enforce %s", result)
		}
	}
}

func TestReleaseSnapshotReusesBuiltUI(t *testing.T) {
	workflow := readWorkflow(t, "ci.yml")
	release := promotionJob(workflow, "release-snapshot")

	if !sameStringSet(stringSlice(release["needs"]), []string{"quality-ui"}) {
		t.Fatalf("release-snapshot needs = %v, want [quality-ui]", stringSlice(release["needs"]))
	}
	if timeout, ok := integer(release["timeout-minutes"]); !ok || timeout != 15 {
		t.Fatalf("release-snapshot timeout = %v, want 15", release["timeout-minutes"])
	}
	if firstStepUsing(release, setupNodeAction) != nil {
		t.Error("release-snapshot still sets up Node")
	}
	if jobHasActiveCommand(release, "npm ci") || jobHasActiveCommand(release, "npm run build") {
		t.Error("release-snapshot still rebuilds the UI")
	}
	download := firstStepUsing(release, promotionDownloadAction)
	if download == nil {
		t.Fatal("release-snapshot does not download ui-dist")
	}
	with := mustMap(download["with"])
	if scalarString(with["name"]) != "ui-dist" || scalarString(with["path"]) != "ui/dist" {
		t.Fatalf("release-snapshot downloads the wrong UI artifact: %v", with)
	}
	build := stepByName(release, "Build Linux amd64 release snapshot")
	if build == nil || !strings.Contains(scalarString(mustMap(build["with"])["args"]), "--skip=before") {
		t.Fatal("release-snapshot does not skip redundant GoReleaser before hooks")
	}
}

func allActiveJobCommands(job map[string]any) string {
	var commands []string
	for _, step := range stepMaps(job) {
		commands = append(commands, activeCommandLines(scalarString(step["run"]))...)
	}
	return strings.Join(commands, "\n")
}
