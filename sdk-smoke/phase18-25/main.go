package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	batch "google.golang.org/api/batch/v1"
	eventarc "google.golang.org/api/eventarc/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	speech "google.golang.org/api/speech/v1"
	texttospeech "google.golang.org/api/texttospeech/v1"
	workflowexecutions "google.golang.org/api/workflowexecutions/v1"
	workflows "google.golang.org/api/workflows/v1"
)

const (
	experimentalOptInEnv = "MINISKY_PHASE18_25_EXPERIMENTAL_OPT_IN"
	evidenceVersion      = 2
	maxEvidenceBytes     = 16 << 10
	defaultBatchImage    = "busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662"
)

var resourceIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var digestImagePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}$`)

type config struct {
	mode         string
	endpoint     string
	project      string
	location     string
	workflowID   string
	triggerID    string
	jobID        string
	batchImage   string
	evidencePath string
}

type evidence struct {
	Version       int    `json:"version"`
	Project       string `json:"project"`
	Location      string `json:"location"`
	WorkflowID    string `json:"workflowId"`
	WorkflowName  string `json:"workflowName"`
	ExecutionName string `json:"executionName"`
	TriggerID     string `json:"triggerId"`
	TriggerName   string `json:"triggerName"`
	JobID         string `json:"jobId"`
	JobName       string `json:"jobName"`
	BatchImage    string `json:"batchImage"`
	BatchState    string `json:"batchState"`
	BatchExitCode int64  `json:"batchExitCode"`
}

type generatedClients struct {
	eventarc   *eventarc.Service
	workflows  *workflows.Service
	executions *workflowexecutions.Service
	batch      *batch.Service
	speech     *speech.Service
	text       *texttospeech.Service
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Phase 18-25 generated Go client smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clients, err := newGeneratedClients(ctx, cfg.endpoint)
	if err != nil {
		return err
	}

	switch cfg.mode {
	case "gate":
		return proveDefaultGate(ctx, clients, cfg)
	case "create":
		if os.Getenv(experimentalOptInEnv) != "1" {
			return fmt.Errorf("create mode requires explicit %s=1", experimentalOptInEnv)
		}
		return createAndRecord(ctx, clients, cfg)
	case "verify":
		return verifyRestart(ctx, clients, cfg)
	case "delete":
		return deleteAndVerify(ctx, clients, cfg)
	default:
		return fmt.Errorf("unsupported MINISKY_PHASE18_25_MODE %q", cfg.mode)
	}
}

func configFromEnv() (config, error) {
	cfg := config{
		mode:         env("MINISKY_PHASE18_25_MODE", "gate"),
		endpoint:     strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/"),
		project:      env("MINISKY_PROJECT_ID", "phase18-25-project"),
		location:     env("MINISKY_PHASE18_25_LOCATION", "us-central1"),
		workflowID:   env("MINISKY_PHASE18_25_WORKFLOW_ID", "phase18-25-workflow"),
		triggerID:    env("MINISKY_PHASE18_25_TRIGGER_ID", "phase18-25-trigger"),
		jobID:        env("MINISKY_PHASE18_25_JOB_ID", "phase18-25-job"),
		batchImage:   env("MINISKY_PHASE18_25_BATCH_IMAGE", defaultBatchImage),
		evidencePath: strings.TrimSpace(os.Getenv("MINISKY_PHASE18_25_EVIDENCE")),
	}
	if err := validateLoopbackEndpoint(cfg.endpoint); err != nil {
		return config{}, err
	}
	for name, value := range map[string]string{
		"project": cfg.project, "location": cfg.location, "workflow ID": cfg.workflowID,
		"trigger ID": cfg.triggerID, "job ID": cfg.jobID,
	} {
		if !resourceIDPattern.MatchString(value) {
			return config{}, fmt.Errorf("%s %q must match %s", name, value, resourceIDPattern)
		}
	}
	if cfg.evidencePath == "" {
		return config{}, errors.New("MINISKY_PHASE18_25_EVIDENCE is required")
	}
	if !digestImagePattern.MatchString(cfg.batchImage) {
		return config{}, errors.New("MINISKY_PHASE18_25_BATCH_IMAGE must be pinned by sha256 digest")
	}
	if !filepath.IsAbs(cfg.evidencePath) {
		return config{}, errors.New("MINISKY_PHASE18_25_EVIDENCE must be an absolute path")
	}
	return cfg, nil
}

func validateLoopbackEndpoint(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse MINISKY_ENDPOINT: %w", err)
	}
	if parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("MINISKY_ENDPOINT must be an HTTP loopback origin without path, query, fragment, or userinfo")
	}
	host := parsed.Hostname()
	if !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("MINISKY_ENDPOINT must target localhost or a loopback IP")
		}
	}
	if parsed.Port() == "" {
		return errors.New("MINISKY_ENDPOINT must include an explicit port")
	}
	return nil
}

func newGeneratedClients(ctx context.Context, endpoint string) (*generatedClients, error) {
	newOptions := func(domain string) []option.ClientOption {
		return []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithEndpoint(endpoint + "/_minisky/" + domain + "/"),
		}
	}
	eventarcClient, err := eventarc.NewService(ctx, newOptions("eventarc.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Eventarc client: %w", err)
	}
	workflowsClient, err := workflows.NewService(ctx, newOptions("workflows.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Workflows client: %w", err)
	}
	executionsClient, err := workflowexecutions.NewService(ctx, newOptions("workflowexecutions.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Workflow Executions client: %w", err)
	}
	batchClient, err := batch.NewService(ctx, newOptions("batch.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Batch client: %w", err)
	}
	speechClient, err := speech.NewService(ctx, newOptions("speech.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Speech client: %w", err)
	}
	textClient, err := texttospeech.NewService(ctx, newOptions("texttospeech.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Text-to-Speech client: %w", err)
	}
	return &generatedClients{
		eventarc: eventarcClient, workflows: workflowsClient,
		executions: executionsClient, batch: batchClient,
		speech: speechClient, text: textClient,
	}, nil
}

func proveDefaultGate(ctx context.Context, clients *generatedClients, cfg config) error {
	parent := locationParent(cfg)
	workflowName := parent + "/workflows/" + cfg.workflowID
	checks := []struct {
		name string
		call func() error
	}{
		{"Eventarc", func() error {
			_, err := clients.eventarc.Projects.Locations.Triggers.List(parent).Context(ctx).Do()
			return err
		}},
		{"Workflows", func() error {
			_, err := clients.workflows.Projects.Locations.Workflows.List(parent).Context(ctx).Do()
			return err
		}},
		{"Workflow Executions", func() error {
			_, err := clients.executions.Projects.Locations.Workflows.Executions.List(workflowName).Context(ctx).Do()
			return err
		}},
		{"Batch", func() error {
			_, err := clients.batch.Projects.Locations.Jobs.List(parent).Context(ctx).Do()
			return err
		}},
		{"Speech", func() error {
			_, err := clients.speech.Speech.Recognize(validSpeechRequest()).Context(ctx).Do()
			return err
		}},
		{"Text-to-Speech", func() error {
			_, err := clients.text.Text.Synthesize(validSynthesisRequest()).Context(ctx).Do()
			return err
		}},
	}
	for _, check := range checks {
		if err := expectGoogleStatus(check.call(), 501, "UNIMPLEMENTED"); err != nil {
			return fmt.Errorf("%s default gate: %w", check.name, err)
		}
	}
	fmt.Println("default-disabled experimental gate verified with six generated clients")
	return nil
}

func createAndRecord(ctx context.Context, clients *generatedClients, cfg config) error {
	parent := locationParent(cfg)
	workflowName := parent + "/workflows/" + cfg.workflowID
	triggerName := parent + "/triggers/" + cfg.triggerID
	jobName := parent + "/jobs/" + cfg.jobID

	if _, err := clients.workflows.Projects.Locations.Workflows.Create(parent, &workflows.Workflow{
		SourceContents: `[{"return":"phase18-25-generated-client"}]`,
	}).WorkflowId(cfg.workflowID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("create workflow: %w", err)
	}
	if got, err := clients.workflows.Projects.Locations.Workflows.Get(workflowName).Context(ctx).Do(); err != nil || workflowResourceName(got) != workflowName {
		return resourceResultError("get workflow", workflowName, workflowResourceName(got), err)
	}
	workflowList, err := clients.workflows.Projects.Locations.Workflows.List(parent).Context(ctx).Do()
	if err != nil || !containsWorkflow(workflowList, workflowName) {
		return fmt.Errorf("list workflows missing %q: %w", workflowName, err)
	}

	execution, err := clients.executions.Projects.Locations.Workflows.Executions.Create(workflowName,
		&workflowexecutions.Execution{Argument: `{"source":"generated-client"}`}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create workflow execution: %w", err)
	}
	execution, err = waitForExecution(ctx, clients.executions, execution.Name)
	if err != nil {
		return err
	}
	if _, err := clients.executions.Projects.Locations.Workflows.Executions.Get(execution.Name).Context(ctx).Do(); err != nil {
		return fmt.Errorf("get workflow execution: %w", err)
	}
	executionList, err := clients.executions.Projects.Locations.Workflows.Executions.List(workflowName).Context(ctx).Do()
	if err != nil || !containsExecution(executionList, execution.Name) {
		return fmt.Errorf("list workflow executions missing %q: %w", execution.Name, err)
	}

	if _, err := clients.eventarc.Projects.Locations.Triggers.Create(parent, &eventarc.Trigger{
		EventFilters: []*eventarc.EventFilter{{Attribute: "type", Value: "google.cloud.pubsub.topic.v1.messagePublished"}},
		Destination:  &eventarc.Destination{Workflow: workflowName},
	}).TriggerId(cfg.triggerID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("create Eventarc trigger: %w", err)
	}
	if got, err := clients.eventarc.Projects.Locations.Triggers.Get(triggerName).Context(ctx).Do(); err != nil || triggerResourceName(got) != triggerName {
		return resourceResultError("get Eventarc trigger", triggerName, triggerResourceName(got), err)
	}
	triggerList, err := clients.eventarc.Projects.Locations.Triggers.List(parent).Context(ctx).Do()
	if err != nil || !containsTrigger(triggerList, triggerName) {
		return fmt.Errorf("list Eventarc triggers missing %q: %w", triggerName, err)
	}

	if _, err := clients.batch.Projects.Locations.Jobs.Create(parent, batchRunnableJob(cfg.batchImage)).
		JobId(cfg.jobID).Context(ctx).Do(); err != nil {
		return fmt.Errorf("create Batch runnable job: %w", err)
	}
	batchJob, batchExitCode, err := waitForBatchJob(ctx, clients.batch, jobName)
	if err != nil {
		return err
	}
	if batchJob.Status.State != "SUCCEEDED" || batchExitCode != 0 {
		return fmt.Errorf("Batch terminal result state=%q exitCode=%d", batchJob.Status.State, batchExitCode)
	}
	if got, err := clients.batch.Projects.Locations.Jobs.Get(jobName).Context(ctx).Do(); err != nil || jobResourceName(got) != jobName {
		return resourceResultError("get Batch job", jobName, jobResourceName(got), err)
	}
	jobList, err := clients.batch.Projects.Locations.Jobs.List(parent).Context(ctx).Do()
	if err != nil || !containsJob(jobList, jobName) {
		return fmt.Errorf("list Batch jobs missing %q: %w", jobName, err)
	}

	if err := proveUnsupportedBoundaries(ctx, clients, cfg); err != nil {
		return err
	}
	record := evidence{
		Version: evidenceVersion, Project: cfg.project, Location: cfg.location,
		WorkflowID: cfg.workflowID, WorkflowName: workflowName, ExecutionName: execution.Name,
		TriggerID: cfg.triggerID, TriggerName: triggerName, JobID: cfg.jobID, JobName: jobName,
		BatchImage: cfg.batchImage, BatchState: batchJob.Status.State, BatchExitCode: batchExitCode,
	}
	if err := writeEvidence(cfg.evidencePath, record); err != nil {
		return err
	}
	fmt.Printf("created generated-client resources; Batch state=%s exitCode=%d image=%s\n",
		batchJob.Status.State, batchExitCode, cfg.batchImage)
	return nil
}

func batchRunnableJob(image string) *batch.Job {
	return &batch.Job{
		TaskGroups: []*batch.TaskGroup{{
			TaskCount: 1, Parallelism: 1,
			TaskSpec: &batch.TaskSpec{Runnables: []*batch.Runnable{{
				Container: &batch.Container{
					ImageUri: image, Entrypoint: "/bin/sh",
					Commands: []string{"-c", "printf phase18-25-generated-batch"},
				},
			}}},
		}},
	}
}

func proveUnsupportedBoundaries(ctx context.Context, clients *generatedClients, cfg config) error {
	parent := locationParent(cfg)
	eventarcErr := func() error {
		_, err := clients.eventarc.Projects.Locations.Triggers.Create(parent, &eventarc.Trigger{
			EventFilters: []*eventarc.EventFilter{{Attribute: "type", Value: "example.unsupported"}},
			Destination:  &eventarc.Destination{CloudRun: &eventarc.CloudRun{Service: "unsupported"}},
		}).TriggerId(cfg.triggerID + "-unsupported").Context(ctx).Do()
		return err
	}()
	if err := expectGoogleStatus(eventarcErr, 501, "UNIMPLEMENTED"); err != nil {
		return fmt.Errorf("Eventarc Cloud Run boundary: %w", err)
	}
	speechErr := func() error {
		_, err := clients.speech.Speech.Recognize(validSpeechRequest()).Context(ctx).Do()
		return err
	}()
	if err := expectGoogleStatus(speechErr, 501, "UNIMPLEMENTED"); err != nil {
		return fmt.Errorf("Speech recognition boundary: %w", err)
	}
	textErr := func() error {
		_, err := clients.text.Text.Synthesize(validSynthesisRequest()).Context(ctx).Do()
		return err
	}()
	if err := expectGoogleStatus(textErr, 501, "UNIMPLEMENTED"); err != nil {
		return fmt.Errorf("Text-to-Speech synthesis boundary: %w", err)
	}
	return nil
}

func validSpeechRequest() *speech.RecognizeRequest {
	return &speech.RecognizeRequest{
		Config: &speech.RecognitionConfig{LanguageCode: "en-US"},
		Audio:  &speech.RecognitionAudio{Content: "aGVsbG8="},
	}
}

func validSynthesisRequest() *texttospeech.SynthesizeSpeechRequest {
	return &texttospeech.SynthesizeSpeechRequest{
		Input:       &texttospeech.SynthesisInput{Text: "hello"},
		Voice:       &texttospeech.VoiceSelectionParams{LanguageCode: "en-US"},
		AudioConfig: &texttospeech.AudioConfig{AudioEncoding: "MP3"},
	}
}

func verifyRestart(ctx context.Context, clients *generatedClients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	if got, err := clients.workflows.Projects.Locations.Workflows.Get(record.WorkflowName).Context(ctx).Do(); err != nil || workflowResourceName(got) != record.WorkflowName {
		return resourceResultError("restart get workflow", record.WorkflowName, workflowResourceName(got), err)
	}
	if got, err := clients.executions.Projects.Locations.Workflows.Executions.Get(record.ExecutionName).Context(ctx).Do(); err != nil || executionResourceName(got) != record.ExecutionName {
		return resourceResultError("restart get execution", record.ExecutionName, executionResourceName(got), err)
	}
	if got, err := clients.eventarc.Projects.Locations.Triggers.Get(record.TriggerName).Context(ctx).Do(); err != nil || triggerResourceName(got) != record.TriggerName {
		return resourceResultError("restart get Eventarc trigger", record.TriggerName, triggerResourceName(got), err)
	}
	gotBatch, err := clients.batch.Projects.Locations.Jobs.Get(record.JobName).Context(ctx).Do()
	if err != nil || jobResourceName(gotBatch) != record.JobName {
		return resourceResultError("restart get Batch job", record.JobName, jobResourceName(gotBatch), err)
	}
	exitCode, err := terminalBatchExitCode(gotBatch)
	if err != nil {
		return fmt.Errorf("restart Batch terminal result: %w", err)
	}
	if gotBatch.Status.State != record.BatchState || exitCode != record.BatchExitCode ||
		batchJobImage(gotBatch) != record.BatchImage {
		return fmt.Errorf("restart Batch result state=%q exitCode=%d image=%q; evidence state=%q exitCode=%d image=%q",
			gotBatch.Status.State, exitCode, batchJobImage(gotBatch),
			record.BatchState, record.BatchExitCode, record.BatchImage)
	}
	fmt.Printf("restart persistence verified; Batch state=%s exitCode=%d image=%s\n",
		gotBatch.Status.State, exitCode, record.BatchImage)
	return nil
}

func deleteAndVerify(ctx context.Context, clients *generatedClients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	if _, err := clients.eventarc.Projects.Locations.Triggers.Delete(record.TriggerName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete Eventarc trigger: %w", err)
	}
	if _, err := clients.batch.Projects.Locations.Jobs.Delete(record.JobName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete Batch job: %w", err)
	}
	if _, err := clients.workflows.Projects.Locations.Workflows.Delete(record.WorkflowName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete workflow: %w", err)
	}
	for name, call := range map[string]func() error{
		"Eventarc trigger": func() error {
			_, err := clients.eventarc.Projects.Locations.Triggers.Get(record.TriggerName).Context(ctx).Do()
			return err
		},
		"Batch job": func() error {
			_, err := clients.batch.Projects.Locations.Jobs.Get(record.JobName).Context(ctx).Do()
			return err
		},
		"workflow": func() error {
			_, err := clients.workflows.Projects.Locations.Workflows.Get(record.WorkflowName).Context(ctx).Do()
			return err
		},
		"cascaded execution": func() error {
			_, err := clients.executions.Projects.Locations.Workflows.Executions.Get(record.ExecutionName).Context(ctx).Do()
			return err
		},
	} {
		if err := expectGoogleStatus(call(), 404, "NOT_FOUND"); err != nil {
			return fmt.Errorf("verify deleted %s: %w", name, err)
		}
	}
	fmt.Println("generated-client delete and execution cascade verified")
	return nil
}

func waitForExecution(ctx context.Context, service *workflowexecutions.Service, name string) (*workflowexecutions.Execution, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		execution, err := service.Projects.Locations.Workflows.Executions.Get(name).Context(ctx).Do()
		if err != nil {
			return nil, fmt.Errorf("poll workflow execution: %w", err)
		}
		if execution.State != "ACTIVE" {
			if execution.State != "SUCCEEDED" {
				return nil, fmt.Errorf("workflow execution reached state %q", execution.State)
			}
			return execution, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for workflow execution: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForBatchJob(ctx context.Context, service *batch.Service, name string) (*batch.Job, int64, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := service.Projects.Locations.Jobs.Get(name).Context(ctx).Do()
		if err != nil {
			return nil, 0, fmt.Errorf("poll Batch job: %w", err)
		}
		if job.Status != nil {
			switch job.Status.State {
			case "SUCCEEDED", "FAILED", "CANCELLED":
				exitCode, err := terminalBatchExitCode(job)
				if err != nil {
					return nil, 0, err
				}
				return job, exitCode, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, 0, fmt.Errorf("wait for Batch job: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func terminalBatchExitCode(job *batch.Job) (int64, error) {
	if job == nil || job.Status == nil {
		return 0, errors.New("Batch job has no status")
	}
	for index := len(job.Status.StatusEvents) - 1; index >= 0; index-- {
		event := job.Status.StatusEvents[index]
		if event != nil && event.TaskExecution != nil &&
			(event.TaskState == "SUCCEEDED" || event.TaskState == "FAILED" || event.TaskState == "CANCELLED") {
			return event.TaskExecution.ExitCode, nil
		}
	}
	return 0, fmt.Errorf("Batch terminal state %q has no task execution exit code", job.Status.State)
}

func batchJobImage(job *batch.Job) string {
	if job == nil || len(job.TaskGroups) != 1 || job.TaskGroups[0] == nil ||
		job.TaskGroups[0].TaskSpec == nil || len(job.TaskGroups[0].TaskSpec.Runnables) != 1 ||
		job.TaskGroups[0].TaskSpec.Runnables[0] == nil ||
		job.TaskGroups[0].TaskSpec.Runnables[0].Container == nil {
		return ""
	}
	return job.TaskGroups[0].TaskSpec.Runnables[0].Container.ImageUri
}

func expectGoogleStatus(err error, code int, status string) error {
	if err == nil {
		return fmt.Errorf("expected HTTP %d %s, got success", code, status)
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		return fmt.Errorf("expected googleapi.Error, got %T: %w", err, err)
	}
	if apiErr.Code != code {
		return fmt.Errorf("HTTP code=%d want=%d body=%s", apiErr.Code, code, apiErr.Body)
	}
	var envelope struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(apiErr.Body), &envelope) != nil || envelope.Error.Status != status {
		return fmt.Errorf("status=%q want=%q body=%s", envelope.Error.Status, status, apiErr.Body)
	}
	return nil
}

func writeEvidence(path string, record evidence) error {
	if err := validateEvidence(record); err != nil {
		return err
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".phase18-25-evidence-*.tmp")
	if err != nil {
		return fmt.Errorf("create evidence temp file: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(append(data, '\n')); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, path); err != nil {
		return fmt.Errorf("publish evidence: %w", err)
	}
	return nil
}

func readEvidence(path string, cfg config) (evidence, error) {
	file, err := os.Open(path)
	if err != nil {
		return evidence{}, fmt.Errorf("read evidence: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxEvidenceBytes+1))
	if err != nil {
		return evidence{}, fmt.Errorf("read evidence: %w", err)
	}
	if len(data) > maxEvidenceBytes {
		return evidence{}, fmt.Errorf("evidence exceeds %d-byte limit", maxEvidenceBytes)
	}
	var record evidence
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return evidence{}, fmt.Errorf("decode evidence: %w", err)
	}
	if err := validateEvidence(record); err != nil {
		return evidence{}, err
	}
	if record.Project != cfg.project || record.Location != cfg.location ||
		record.WorkflowID != cfg.workflowID || record.TriggerID != cfg.triggerID ||
		record.JobID != cfg.jobID || record.BatchImage != cfg.batchImage {
		return evidence{}, errors.New("evidence identifiers do not match the requested smoke configuration")
	}
	return record, nil
}

func validateEvidence(record evidence) error {
	if record.Version != evidenceVersion {
		return fmt.Errorf("evidence version=%d want=%d", record.Version, evidenceVersion)
	}
	for name, value := range map[string]string{
		"project": record.Project, "location": record.Location, "workflow ID": record.WorkflowID,
		"trigger ID": record.TriggerID, "job ID": record.JobID,
	} {
		if !resourceIDPattern.MatchString(value) {
			return fmt.Errorf("invalid evidence %s %q", name, value)
		}
	}
	if !digestImagePattern.MatchString(record.BatchImage) {
		return errors.New("evidence Batch image is not pinned by sha256 digest")
	}
	if record.BatchState != "SUCCEEDED" || record.BatchExitCode != 0 {
		return fmt.Errorf("evidence Batch terminal result state=%q exitCode=%d", record.BatchState, record.BatchExitCode)
	}
	parent := "projects/" + record.Project + "/locations/" + record.Location
	if record.WorkflowName != parent+"/workflows/"+record.WorkflowID ||
		record.TriggerName != parent+"/triggers/"+record.TriggerID ||
		record.JobName != parent+"/jobs/"+record.JobID ||
		!strings.HasPrefix(record.ExecutionName, record.WorkflowName+"/executions/") ||
		strings.TrimPrefix(record.ExecutionName, record.WorkflowName+"/executions/") == "" {
		return errors.New("evidence resource names do not match their identifiers")
	}
	return nil
}

func locationParent(cfg config) string {
	return "projects/" + cfg.project + "/locations/" + cfg.location
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func resourceResultError(action, want, got string, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", action, err)
	}
	return fmt.Errorf("%s name=%q want=%q", action, got, want)
}

func workflowResourceName(value *workflows.Workflow) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func executionResourceName(value *workflowexecutions.Execution) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func triggerResourceName(value *eventarc.Trigger) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func jobResourceName(value *batch.Job) string {
	if value == nil {
		return ""
	}
	return value.Name
}

func containsWorkflow(list *workflows.ListWorkflowsResponse, name string) bool {
	if list == nil {
		return false
	}
	for _, item := range list.Workflows {
		if item.Name == name {
			return true
		}
	}
	return false
}

func containsExecution(list *workflowexecutions.ListExecutionsResponse, name string) bool {
	if list == nil {
		return false
	}
	for _, item := range list.Executions {
		if item.Name == name {
			return true
		}
	}
	return false
}

func containsTrigger(list *eventarc.ListTriggersResponse, name string) bool {
	if list == nil {
		return false
	}
	for _, item := range list.Triggers {
		if item.Name == name {
			return true
		}
	}
	return false
}

func containsJob(list *batch.ListJobsResponse, name string) bool {
	if list == nil {
		return false
	}
	for _, item := range list.Jobs {
		if item.Name == name {
			return true
		}
	}
	return false
}
