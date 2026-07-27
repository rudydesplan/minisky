package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	storage "google.golang.org/api/storage/v1"
	storagetransfer "google.golang.org/api/storagetransfer/v1"
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

func TestConfigRequiresOptInSafeIdentifiersAndPinnedImages(t *testing.T) {
	setValidEnv(t)
	if _, err := configFromEnv(); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{"non-loopback", "MINISKY_ENDPOINT", "http://example.com:8080", "loopback"},
		{"unsafe ID", "MINISKY_PHASE20_CLUSTER_ID", "../cluster", "cluster ID"},
		{"relative evidence", "MINISKY_PHASE20_EVIDENCE", "evidence.json", "absolute"},
		{"mutable PostgreSQL image", "MINISKY_PHASE20_POSTGRES_IMAGE", "postgres:15.8-bookworm", "sha256"},
		{"mutable Valkey image", "MINISKY_PHASE20_VALKEY_IMAGE", "valkey/valkey:7.2.12-alpine", "sha256"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(tt.key, tt.value)
			if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestGeneratedClientsUseCanonicalFullDomainPaths(t *testing.T) {
	responses := map[string]string{
		"/_minisky/alloydb.googleapis.com/v1/projects/demo/locations/us-central1/clusters": `{"clusters":[]}`,
		"/_minisky/identityplatform.googleapis.com/v2/projects/demo/tenants":               `{"tenants":[]}`,
		"/_minisky/file.googleapis.com/v1/projects/demo/locations/us-central1/instances":   `{"instances":[]}`,
		"/_minisky/redis.googleapis.com/v1/projects/demo/locations/us-central1/instances":  `{"instances":[]}`,
		"/_minisky/storagetransfer.googleapis.com/v1/transferJobs":                         `{"transferJobs":[]}`,
		"/_minisky/storage.googleapis.com/storage/v1/b":                                    `{"items":[]}`,
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
	if _, err := clients.alloy.Projects.Locations.Clusters.List(parent).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.identity.Projects.Tenants.List("projects/demo").Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.file.Projects.Locations.Instances.List(parent).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.redis.Projects.Locations.Instances.List(parent).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.transfer.TransferJobs.List(`{"projectId":"demo"}`).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.storage.Buckets.List("demo").Do(); err != nil {
		t.Fatal(err)
	}
	for path := range responses {
		if !seen[path] {
			t.Errorf("generated client did not request %s", path)
		}
	}
}

func TestGeneratedCustomMethodReturnsSupportedLROAndStorageUploadStaysCanonical(t *testing.T) {
	seenRun := false
	seenUpload := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/_minisky/storagetransfer.googleapis.com/v1/transferJobs/1:run":
			seenRun = true
			fmt.Fprint(w, `{"name":"transferOperations/1","done":true,"response":{}}`)
		case "/_minisky/storage.googleapis.com/upload/storage/v1/b/source/o":
			seenUpload = true
			fmt.Fprint(w, `{"bucket":"source","name":"phase20/source.txt"}`)
		default:
			http.Error(w, fmt.Sprintf("unexpected path %q", r.URL.Path), http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	clients, err := newGeneratedClients(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, runErr := clients.transfer.TransferJobs.Run("transferJobs/1",
		&storagetransfer.RunTransferJobRequest{ProjectId: "demo"}).Do()
	if runErr != nil {
		t.Fatal(runErr)
	}
	if _, err := clients.storage.Objects.Insert("source", &storage.Object{Name: transferObjectName}).
		Media(strings.NewReader(transferObjectBody)).Do(); err != nil {
		t.Fatal(err)
	}
	if !seenRun || !seenUpload {
		t.Fatalf("seen generated run=%t upload=%t", seenRun, seenUpload)
	}
}

func TestBoundaryRequiresSuccessfulStorageTransferRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost &&
			r.URL.Path == "/_minisky/storagetransfer.googleapis.com/v1/transferJobs":
			fmt.Fprint(w, `{"name":"transferJobs/1","projectId":"demo","status":"ENABLED"}`)
		case r.Method == http.MethodPost &&
			r.URL.Path == "/_minisky/storagetransfer.googleapis.com/v1/transferJobs/1:run":
			fmt.Fprint(w, `{"name":"transferOperations/1","done":true,"response":{}}`)
		case r.Method == http.MethodPatch &&
			r.URL.Path == "/_minisky/storagetransfer.googleapis.com/v1/transferJobs/1":
			fmt.Fprint(w, `{"name":"transferJobs/1","projectId":"demo","status":"DELETED"}`)
		default:
			http.Error(w, fmt.Sprintf("unexpected %s path %q", r.Method, r.URL.Path), http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	clients, err := newGeneratedClients(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t)
	if err := proveStorageTransferBoundary(context.Background(), clients, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestEvidenceRoundTripIsBoundedAndStrict(t *testing.T) {
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

	t.Run("unimplemented transfer run", func(t *testing.T) {
		invalid := record
		invalid.TransferRunStatus = "UNIMPLEMENTED"
		invalid.TransferOperationName = ""
		if err := validateEvidence(invalid); err == nil || !strings.Contains(err.Error(), "must have succeeded") {
			t.Fatalf("error=%v, want successful transfer requirement", err)
		}
	})
	t.Run("unknown field", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		if err := os.WriteFile(path, []byte(`{"version":1,"unexpected":true}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readEvidence(path, cfg); err == nil || !strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("oversized", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		if err := os.WriteFile(path, []byte(strings.Repeat("x", maxEvidenceBytes+1)), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readEvidence(path, cfg); err == nil || !strings.Contains(err.Error(), "limit") {
			t.Fatalf("error=%v", err)
		}
	})
}

func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MINISKY_ENDPOINT", "http://127.0.0.1:8080")
	t.Setenv("MINISKY_PROJECT_ID", "demo")
	t.Setenv("MINISKY_PHASE20_LOCATION", "us-central1")
	t.Setenv("MINISKY_PHASE20_CLUSTER_ID", "cluster")
	t.Setenv("MINISKY_PHASE20_ALLOYDB_INSTANCE_ID", "primary")
	t.Setenv("MINISKY_PHASE20_FILESTORE_INSTANCE_ID", "files")
	t.Setenv("MINISKY_PHASE20_REDIS_INSTANCE_ID", "cache")
	t.Setenv("MINISKY_PHASE20_SOURCE_BUCKET", "phase20-source")
	t.Setenv("MINISKY_PHASE20_SINK_BUCKET", "phase20-sink")
	t.Setenv("MINISKY_PHASE20_POSTGRES_IMAGE", defaultPostgresImage)
	t.Setenv("MINISKY_PHASE20_VALKEY_IMAGE", defaultValkeyImage)
	t.Setenv("MINISKY_PHASE20_EVIDENCE", filepath.Join(t.TempDir(), "evidence.json"))
}

func testConfig(t *testing.T) config {
	t.Helper()
	setValidEnv(t)
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func validEvidence(cfg config) evidence {
	parent := locationParent(cfg)
	cluster := parent + "/clusters/" + cfg.clusterID
	return evidence{
		Version: evidenceVersion, Project: cfg.project, Location: cfg.location,
		ClusterName: cluster, AlloyDBInstanceName: cluster + "/instances/" + cfg.alloyDBInstanceID,
		TenantName:            "projects/" + cfg.project + "/tenants/tenant-1",
		FilestoreInstanceName: parent + "/instances/" + cfg.filestoreInstanceID,
		RedisInstanceName:     parent + "/instances/" + cfg.redisInstanceID,
		TransferJobName:       "transferJobs/1", TransferOperationName: "transferOperations/1",
		TransferRunStatus: "SUCCEEDED",
		SourceBucket:      cfg.sourceBucket, SinkBucket: cfg.sinkBucket, ObjectName: transferObjectName,
		ObjectSHA256:  transferObjectSHA256,
		PostgresImage: cfg.postgresImage, ValkeyImage: cfg.valkeyImage,
	}
}
