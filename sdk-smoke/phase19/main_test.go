package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateLoopbackEndpoint(t *testing.T) {
	for _, endpoint := range []string{"http://127.0.0.1:8080", "http://[::1]:8080", "http://localhost:8080"} {
		if err := validateLoopbackEndpoint(endpoint); err != nil {
			t.Errorf("validateLoopbackEndpoint(%q): %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"", "https://127.0.0.1:8080", "http://example.com:8080", "http://localhost", "http://localhost:8080/path"} {
		if err := validateLoopbackEndpoint(endpoint); err == nil {
			t.Errorf("validateLoopbackEndpoint(%q) unexpectedly succeeded", endpoint)
		}
	}
}

func TestConfigRequiresAbsoluteEvidenceAndExplicitDockerOptIn(t *testing.T) {
	setValidEnv(t)
	if _, err := configFromEnv(); err != nil {
		t.Fatal(err)
	}

	t.Run("relative evidence", func(t *testing.T) {
		setValidEnv(t)
		t.Setenv("MINISKY_PHASE19_EVIDENCE", "evidence.json")
		if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), "absolute") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("docker mode without opt in", func(t *testing.T) {
		setValidEnv(t)
		t.Setenv("MINISKY_PHASE19_MODE", "docker-create")
		t.Setenv(experimentalOptInEnv, "")
		if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), experimentalOptInEnv) {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestGeneratedClientsUseCanonicalFullDomainPaths(t *testing.T) {
	responses := map[string]string{
		"/_minisky/dataflow.googleapis.com/v1b3/projects/demo/locations/us-central1/jobs":            `{"jobs":[]}`,
		"/_minisky/dataform.googleapis.com/v1beta1/projects/demo/locations/us-central1/repositories": `{"repositories":[]}`,
		"/_minisky/managedkafka.googleapis.com/v1/projects/demo/locations/us-central1/clusters":      `{"clusters":[]}`,
		"/_minisky/composer.googleapis.com/v1/projects/demo/locations/us-central1/environments":      `{"environments":[]}`,
		"/_minisky/pubsublite.googleapis.com/v1/admin/projects/demo/locations/us-central1/topics":    `{"topics":[]}`,
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
	calls := []func() error{
		func() error {
			_, err := clients.dataflow.Projects.Locations.Jobs.List("demo", "us-central1").Do()
			return err
		},
		func() error { _, err := clients.dataform.Projects.Locations.Repositories.List(parent).Do(); return err },
		func() error { _, err := clients.kafka.Projects.Locations.Clusters.List(parent).Do(); return err },
		func() error { _, err := clients.composer.Projects.Locations.Environments.List(parent).Do(); return err },
		func() error {
			_, err := clients.pubsublite.Admin.Projects.Locations.Topics.List(parent).Do()
			return err
		},
	}
	for _, call := range calls {
		if err := call(); err != nil {
			t.Fatal(err)
		}
	}
	for path := range responses {
		if !seen[path] {
			t.Errorf("generated client did not request %s", path)
		}
	}
}

func TestDefaultGateUsesGeneratedClientsAndPubSubLiteReceives501(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		fmt.Fprint(w, `{"error":{"code":501,"message":"experimental service disabled","status":"UNIMPLEMENTED","details":[]}}`)
	}))
	t.Cleanup(server.Close)
	clients, err := newGeneratedClients(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t)
	cfg.endpoint = server.URL
	if err := proveDefaultGate(context.Background(), clients, cfg); err != nil {
		t.Fatal(err)
	}
	if requests != 5 {
		t.Fatalf("generated-client gate requests=%d want=5", requests)
	}
}

func TestVerifyDockerBackendsAcceptsReconciledExactOwnedBackends(t *testing.T) {
	cfg := testConfig(t)
	parent := locationParent(cfg)
	record := validEvidence(cfg)
	record.KafkaCluster = parent + "/clusters/" + cfg.clusterID
	record.KafkaTopic = record.KafkaCluster + "/topics/" + cfg.topicID
	record.ComposerEnvironment = parent + "/environments/" + cfg.environmentID
	record.ComposerState = "RUNNING"
	if err := writeEvidence(cfg.evidencePath, record); err != nil {
		t.Fatal(err)
	}

	responses := map[string]string{
		"/_minisky/managedkafka.googleapis.com/v1/" + record.KafkaCluster:    `{"name":"` + record.KafkaCluster + `","state":"ACTIVE","bootstrapAddress":"127.0.0.1:19092"}`,
		"/_minisky/managedkafka.googleapis.com/v1/" + record.KafkaTopic:      `{"name":"` + record.KafkaTopic + `"}`,
		"/_minisky/composer.googleapis.com/v1/" + record.ComposerEnvironment: `{"name":"` + record.ComposerEnvironment + `","state":"RUNNING","config":{"airflowUri":"http://127.0.0.1:18080"}}`,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := responses[r.URL.Path]
		if !ok {
			http.Error(w, fmt.Sprintf("unexpected path %q", r.URL.Path), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)

	clients, err := newGeneratedClients(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyDockerBackends(context.Background(), clients, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestEvidenceRoundTripRejectsMismatchedHierarchy(t *testing.T) {
	cfg := testConfig(t)
	record := validEvidence(cfg)
	if err := writeEvidence(cfg.evidencePath, record); err != nil {
		t.Fatal(err)
	}
	got, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.DataformInvocation != record.DataformInvocation {
		t.Fatalf("invocation=%q want=%q", got.DataformInvocation, record.DataformInvocation)
	}
	record.DataformInvocation = "projects/other/locations/us/repositories/r/workflowInvocations/i"
	if err := validateEvidence(record); err == nil {
		t.Fatal("expected hierarchy mismatch")
	}
}

func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MINISKY_ENDPOINT", "http://127.0.0.1:8080")
	t.Setenv("MINISKY_PROJECT_ID", "demo")
	t.Setenv("MINISKY_PHASE19_LOCATION", "us-central1")
	t.Setenv("MINISKY_PHASE19_EVIDENCE", filepath.Join(t.TempDir(), "evidence.json"))
	t.Setenv("MINISKY_PHASE19_MODE", "create")
	t.Setenv(experimentalOptInEnv, "1")
}

func testConfig(t *testing.T) config {
	t.Helper()
	return config{
		mode: "create", endpoint: "http://127.0.0.1:8080", project: "demo", location: "us-central1",
		dataflowName: "phase19-dataflow", repositoryID: "phase19-repo", workspaceID: "phase19-workspace",
		clusterID: "phase19-kafka", topicID: "phase19-topic", environmentID: "phase19-airflow",
		evidencePath: filepath.Join(t.TempDir(), "evidence.json"),
	}
}

func validEvidence(cfg config) evidence {
	parent := locationParent(cfg)
	repository := parent + "/repositories/" + cfg.repositoryID
	workspace := repository + "/workspaces/" + cfg.workspaceID
	return evidence{
		Version: evidenceVersion, Project: cfg.project, Location: cfg.location,
		DataflowID: "1", DataflowName: cfg.dataflowName, DataflowState: "JOB_STATE_DONE",
		CancelledDataflowID: "2", CancelledDataflowState: "JOB_STATE_CANCELLED",
		DataformRepository: repository, DataformWorkspace: workspace,
		DataformCompilation: repository + "/compilationResults/cr-1",
		DataformInvocation:  repository + "/workflowInvocations/wi-1",
	}
}
