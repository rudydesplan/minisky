package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	batch "google.golang.org/api/batch/v1"
)

func TestValidateLoopbackEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"http://127.0.0.1:8080",
		"http://[::1]:8080",
		"http://localhost:8080",
	} {
		if err := validateLoopbackEndpoint(endpoint); err != nil {
			t.Errorf("validateLoopbackEndpoint(%q): %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{
		"", "https://127.0.0.1:8080", "http://192.0.2.1:8080",
		"http://localhost", "http://user@localhost:8080",
		"http://localhost:8080/path", "http://localhost:8080?query=1",
	} {
		if err := validateLoopbackEndpoint(endpoint); err == nil {
			t.Errorf("validateLoopbackEndpoint(%q) unexpectedly succeeded", endpoint)
		}
	}
}

func TestConfigFromEnvValidatesInputsAndEvidencePath(t *testing.T) {
	setValidEnv(t)
	if _, err := configFromEnv(); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	t.Run("relative evidence path", func(t *testing.T) {
		setValidEnv(t)
		t.Setenv("MINISKY_PHASE18_25_EVIDENCE", "evidence.json")
		if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), "absolute") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("unsafe resource ID", func(t *testing.T) {
		setValidEnv(t)
		t.Setenv("MINISKY_PHASE18_25_JOB_ID", "../job")
		if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), "job ID") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("non-loopback endpoint", func(t *testing.T) {
		setValidEnv(t)
		t.Setenv("MINISKY_ENDPOINT", "http://example.com:8080")
		if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), "loopback") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("mutable Batch image", func(t *testing.T) {
		setValidEnv(t)
		t.Setenv("MINISKY_PHASE18_25_BATCH_IMAGE", "busybox:latest")
		if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), "sha256") {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestGeneratedClientsUseFullDomainCanonicalPaths(t *testing.T) {
	responses := map[string]string{
		"/_minisky/eventarc.googleapis.com/v1/projects/demo/locations/us-central1/triggers":                            `{"triggers":[]}`,
		"/_minisky/workflows.googleapis.com/v1/projects/demo/locations/us-central1/workflows":                          `{"workflows":[]}`,
		"/_minisky/workflowexecutions.googleapis.com/v1/projects/demo/locations/us-central1/workflows/flow/executions": `{"executions":[]}`,
		"/_minisky/batch.googleapis.com/v1/projects/demo/locations/us-central1/jobs":                                   `{"jobs":[]}`,
		"/_minisky/speech.googleapis.com/v1/speech:recognize":                                                          `{}`,
		"/_minisky/texttospeech.googleapis.com/v1/text:synthesize":                                                     `{}`,
	}
	seen := make(map[string]bool)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := responses[r.URL.Path]
		if !ok {
			http.Error(w, fmt.Sprintf("unexpected path %q", r.URL.Path), http.StatusNotFound)
			return
		}
		seen[r.URL.Path] = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)

	clients, err := newGeneratedClients(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	parent := "projects/demo/locations/us-central1"
	if _, err := clients.eventarc.Projects.Locations.Triggers.List(parent).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.workflows.Projects.Locations.Workflows.List(parent).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.executions.Projects.Locations.Workflows.Executions.List(parent + "/workflows/flow").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.batch.Projects.Locations.Jobs.List(parent).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.speech.Speech.Recognize(validSpeechRequest()).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.text.Text.Synthesize(validSynthesisRequest()).Do(); err != nil {
		t.Fatal(err)
	}
	for path := range responses {
		if !seen[path] {
			t.Errorf("generated client did not request %s", path)
		}
	}
}

func TestEvidenceRoundTripAndValidation(t *testing.T) {
	cfg := testConfig(t)
	record := validEvidence(cfg)
	if err := writeEvidence(cfg.evidencePath, record); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(cfg.evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("evidence mode=%o want=600", info.Mode().Perm())
	}
	got, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got != record {
		t.Fatalf("evidence=%+v want=%+v", got, record)
	}

	t.Run("mismatched resource name", func(t *testing.T) {
		bad := record
		bad.JobName = "projects/demo/locations/us-central1/jobs/other"
		if err := validateEvidence(bad); err == nil {
			t.Fatal("expected mismatched resource name error")
		}
	})
	t.Run("unknown field", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		data := strings.Replace(
			fmt.Sprintf(`{"version":1,"project":%q,"location":%q,"workflowId":%q,"workflowName":%q,"executionName":%q,"triggerId":%q,"triggerName":%q,"jobId":%q,"jobName":%q}`,
				record.Project, record.Location, record.WorkflowID, record.WorkflowName, record.ExecutionName,
				record.TriggerID, record.TriggerName, record.JobID, record.JobName),
			`"version":1`, `"version":1,"unexpected":true`, 1)
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		badCfg := cfg
		badCfg.evidencePath = path
		if _, err := readEvidence(path, badCfg); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("oversized file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		if err := os.WriteFile(path, []byte(strings.Repeat("x", maxEvidenceBytes+1)), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readEvidence(path, cfg); err == nil || !strings.Contains(err.Error(), "limit") {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestGeneratedBatchRunnableAndTerminalExitCode(t *testing.T) {
	job := batchRunnableJob(defaultBatchImage)
	data, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)
	for _, expected := range []string{
		`"taskCount":"1"`, `"parallelism":"1"`, `"runnables"`,
		`"imageUri":"` + defaultBatchImage + `"`,
		`"entrypoint":"/bin/sh"`, `"commands":["-c","printf phase18-25-generated-batch"]`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("generated Batch request %s does not contain %s", body, expected)
		}
	}

	terminal := &batch.Job{Status: &batch.JobStatus{
		State: "SUCCEEDED",
		StatusEvents: []*batch.StatusEvent{{
			Type: "JOB_STATE_CHANGED", TaskState: "SUCCEEDED",
			TaskExecution: &batch.TaskExecution{ExitCode: 0},
		}},
	}}
	exitCode, err := terminalBatchExitCode(terminal)
	if err != nil {
		t.Fatal(err)
	}
	if exitCode != 0 {
		t.Fatalf("exitCode=%d", exitCode)
	}
	terminal.Status.StatusEvents = nil
	if _, err := terminalBatchExitCode(terminal); err == nil {
		t.Fatal("expected missing terminal exit-code error")
	}
}

func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MINISKY_ENDPOINT", "http://127.0.0.1:8080")
	t.Setenv("MINISKY_PROJECT_ID", "demo")
	t.Setenv("MINISKY_PHASE18_25_LOCATION", "us-central1")
	t.Setenv("MINISKY_PHASE18_25_WORKFLOW_ID", "flow")
	t.Setenv("MINISKY_PHASE18_25_TRIGGER_ID", "trigger")
	t.Setenv("MINISKY_PHASE18_25_JOB_ID", "job")
	t.Setenv("MINISKY_PHASE18_25_BATCH_IMAGE", defaultBatchImage)
	t.Setenv("MINISKY_PHASE18_25_EVIDENCE", filepath.Join(t.TempDir(), "evidence.json"))
}

func testConfig(t *testing.T) config {
	t.Helper()
	return config{
		mode: "create", endpoint: "http://127.0.0.1:8080", project: "demo", location: "us-central1",
		workflowID: "flow", triggerID: "trigger", jobID: "job",
		batchImage:   defaultBatchImage,
		evidencePath: filepath.Join(t.TempDir(), "evidence.json"),
	}
}

func validEvidence(cfg config) evidence {
	parent := locationParent(cfg)
	workflowName := parent + "/workflows/" + cfg.workflowID
	return evidence{
		Version: evidenceVersion, Project: cfg.project, Location: cfg.location,
		WorkflowID: cfg.workflowID, WorkflowName: workflowName,
		ExecutionName: workflowName + "/executions/123",
		TriggerID:     cfg.triggerID, TriggerName: parent + "/triggers/" + cfg.triggerID,
		JobID: cfg.jobID, JobName: parent + "/jobs/" + cfg.jobID,
		BatchImage: cfg.batchImage, BatchState: "SUCCEEDED", BatchExitCode: 0,
	}
}
