package main

import "testing"

func TestCIWindowsStateMarkerLifecycleContract(t *testing.T) {
	job := workflowJob(t, readWorkflow(t, "ci.yml"), "windows-state-markers")
	if got := scalarString(job["runs-on"]); got != "windows-latest" {
		t.Errorf("Windows state marker runner = %q, want windows-latest", got)
	}
	if timeout, ok := integer(job["timeout-minutes"]); !ok || timeout != 10 {
		t.Errorf("Windows state marker timeout = %v, want 10 minutes", job["timeout-minutes"])
	}
	if _, found := job["needs"]; found {
		t.Error("Windows state marker job must not depend on unrelated jobs")
	}
	if _, found := job["continue-on-error"]; found {
		t.Error("Windows state marker job must not allow failure")
	}

	permissions := mustMap(job["permissions"])
	if len(permissions) != 1 || scalarString(permissions["contents"]) != "read" {
		t.Errorf("Windows state marker permissions = %v, want contents: read only", permissions)
	}
	for _, action := range []string{checkoutAction, setupGoAction} {
		if firstStepUsing(job, action) == nil {
			t.Errorf("Windows state marker job lacks pinned action %q", action)
		}
	}
	setupGo := firstStepUsing(job, setupGoAction)
	setupWith := mustMap(setupGo["with"])
	if scalarString(setupWith["go-version-file"]) != "go.mod" ||
		scalarString(setupWith["cache"]) != "true" ||
		scalarString(setupWith["cache-dependency-path"]) != "go.sum" {
		t.Errorf("Windows state marker Go setup is incomplete: %v", setupWith)
	}

	const command = "go test -count=1 -run '^TestWindowsPinnedLocalMarker' ./pkg/state"
	if !jobHasActiveCommand(job, command) {
		t.Fatalf("Windows state marker job lacks exact focused command %q", command)
	}
	step := stepByName(job, "Run Windows state marker lifecycle tests")
	if step == nil || scalarString(step["shell"]) != "pwsh" {
		t.Errorf("Windows state marker test step must use pwsh: %v", step)
	}
}

func TestCIQualityDependsOnWindowsStateMarkers(t *testing.T) {
	quality := workflowJob(t, readWorkflow(t, "ci.yml"), "quality")
	if !contains(stringSlice(quality["needs"]), "windows-state-markers") {
		t.Fatalf("quality needs = %v, want windows-state-markers", stringSlice(quality["needs"]))
	}
}

func TestCIQualityRequiresWindowsStateMarkerSuccess(t *testing.T) {
	quality := workflowJob(t, readWorkflow(t, "ci.yml"), "quality")
	if !conditionIsAlways(quality["if"]) {
		t.Fatalf("quality condition = %q, want always()", scalarString(quality["if"]))
	}
	const assertion = `test "${{ needs.windows-state-markers.result }}" = "success"`
	if !jobHasActiveCommand(quality, assertion) {
		t.Fatalf("quality lacks exact Windows success assertion %q", assertion)
	}
}
