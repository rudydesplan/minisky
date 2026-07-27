package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitUntilHTTPReadyAcceptsAnyHTTPResponse(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	if err := waitUntilHTTPReady(server.URL, time.Second); err != nil {
		t.Fatalf("wait for HTTP response: %v", err)
	}
}

func TestServiceManagerUsesPerRequestDockerDeadlines(t *testing.T) {
	manager, err := NewServiceManager()
	if err != nil {
		t.Fatal(err)
	}
	if manager.dockerClient == nil || manager.dockerClient.Timeout != 0 {
		t.Fatalf("Docker client timeout = %v, want no global stream deadline", manager.dockerClient)
	}
	var remaining time.Duration
	manager.dockerClient.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		deadline, ok := request.Context().Deadline()
		if !ok {
			t.Fatal("ordinary Docker request has no deadline")
		}
		remaining = time.Until(deadline)
		return dockerResponse(http.StatusOK, `{}`), nil
	})
	request, err := http.NewRequest(http.MethodGet, "http://localhost/version", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := manager.doDocker(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if remaining <= 0 || remaining > dockerRequestTimeout {
		t.Fatalf("ordinary Docker deadline remaining = %v", remaining)
	}

	remaining = 0
	manager.dockerClient.Transport = deadlineRoundTripper{
		timeout: dockerRequestTimeout,
		base: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			deadline, ok := request.Context().Deadline()
			if !ok {
				t.Fatal("direct Docker client request has no deadline")
			}
			remaining = time.Until(deadline)
			return dockerResponse(http.StatusOK, `{}`), nil
		}),
	}
	response, err = manager.dockerClient.Get("http://localhost/version")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if remaining <= 0 || remaining > dockerRequestTimeout {
		t.Fatalf("direct Docker deadline remaining = %v", remaining)
	}
}

func TestImagePullUsesExplicitPullDeadline(t *testing.T) {
	manager := &ServiceManager{dockerTimeout: dockerRequestTimeout}
	var remaining time.Duration
	manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		deadline, ok := request.Context().Deadline()
		if !ok {
			t.Fatal("image pull has no deadline")
		}
		remaining = time.Until(deadline)
		return dockerResponse(http.StatusOK, ""), nil
	})}
	if err := manager.pullImageInternal(context.Background(), "example/image:latest"); err != nil {
		t.Fatal(err)
	}
	if remaining <= dockerRequestTimeout || remaining > dockerImagePullTimeout {
		t.Fatalf("pull deadline remaining = %v, want explicit pull budget", remaining)
	}
}

func TestCloudSQLAdminCommandUsesExecutableQuotedSQL(t *testing.T) {
	tests := []struct {
		name     string
		version  string
		action   string
		resource string
		password string
		want     []string
	}{
		{
			name: "postgres database", version: "POSTGRES_18", action: "CREATE_DATABASE", resource: "app_db",
			want: []string{"psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", `CREATE DATABASE "app_db"`},
		},
		{
			name: "postgres user", version: "POSTGRES_18", action: "CREATE_USER", resource: "app_user", password: "pa'ss",
			want: []string{"psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
				`CREATE USER "app_user" WITH PASSWORD 'pa''ss'`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := cloudSQLAdminCommand(test.version, test.action, test.resource, test.password)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("command = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestCleanupProfileSweepsOnlyExactOwnedDockerResources(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "cleanup")
	var deleted []string
	manager := &ServiceManager{dockerTimeout: dockerRequestTimeout}
	manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/containers/json":
			return dockerResponse(http.StatusOK, `[
				{"Id":"compute","Labels":{"managed-by":"minisky","minisky.profile":"cleanup","minisky.service":"compute-instance"}},
				{"Id":"dataproc","Labels":{"managed-by":"minisky","minisky.profile":"cleanup","minisky.service":"compute-instance"}},
				{"Id":"serverless","Labels":{"managed-by":"minisky","minisky.profile":"cleanup","minisky.service":"serverless"}},
				{"Id":"cloudsql","Labels":{"managed-by":"minisky","minisky.profile":"cleanup","minisky.service":"cloudsql"}},
				{"Id":"redis-container","Labels":{"managed-by":"minisky","minisky.profile":"cleanup","minisky.service":"memorystore-redis"}},
				{"Id":"emulator","Labels":{"managed-by":"minisky","minisky.profile":"cleanup","minisky.service":"storage.googleapis.com"}},
				{"Id":"foreign","Labels":{"managed-by":"minisky","minisky.profile":"other"}}
			]`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/networks":
			return dockerResponse(http.StatusOK, `[
				{"Id":"network","Labels":{"managed-by":"minisky","minisky.profile":"cleanup","minisky.service":"compute-network"}},
				{"Id":"foreign-network","Labels":{"managed-by":"someone-else","minisky.profile":"cleanup"}}
			]`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/volumes":
			return dockerResponse(http.StatusOK, `{"Volumes":[
				{"Name":"redis","Labels":{"managed-by":"minisky","minisky.profile":"cleanup","minisky.service":"memorystore-redis"}},
				{"Name":"sql","Labels":{"managed-by":"minisky","minisky.profile":"cleanup","minisky.service":"cloudsql"}},
				{"Name":"foreign-volume","Labels":{"managed-by":"minisky","minisky.profile":"other"}}
			]}`), nil
		case request.Method == http.MethodGet &&
			(request.URL.Path == "/volumes/redis" || request.URL.Path == "/volumes/sql"):
			name := strings.TrimPrefix(request.URL.Path, "/volumes/")
			return dockerResponse(http.StatusOK, fmt.Sprintf(
				`{"Name":%q,"Labels":{"managed-by":"minisky","minisky.profile":"cleanup"}}`,
				name,
			)), nil
		case request.Method == http.MethodPost && request.URL.Path == "/volumes/prune":
			if got := request.URL.Query().Get("filters"); !strings.Contains(got, "minisky.profile=cleanup") ||
				!strings.Contains(got, "managed-by=minisky") || !strings.Contains(got, `"all":["true"]`) {
				t.Fatalf("unsafe volume prune filters: %s", got)
			}
			return dockerResponse(http.StatusOK, `{"VolumesDeleted":["redis","sql"]}`), nil
		case request.Method == http.MethodDelete:
			deleted = append(deleted, request.URL.Path)
			return dockerResponse(http.StatusNoContent, ""), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}
	if err := manager.CleanupProfile(context.Background()); err != nil {
		t.Fatal(err)
	}
	sort.Strings(deleted)
	want := []string{
		"/containers/cloudsql", "/containers/compute", "/containers/dataproc",
		"/containers/emulator", "/containers/redis-container", "/containers/serverless", "/networks/network",
	}
	if !reflect.DeepEqual(deleted, want) {
		t.Fatalf("deleted = %#v, want %#v", deleted, want)
	}
}

func TestCleanupProfilePropagatesDeletionFailure(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "cleanup")
	manager := &ServiceManager{dockerTimeout: dockerRequestTimeout}
	manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/containers/json":
			return dockerResponse(http.StatusOK, `[{"Id":"owned","Labels":{"managed-by":"minisky","minisky.profile":"cleanup"}}]`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/networks":
			return dockerResponse(http.StatusOK, `[]`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/volumes":
			return dockerResponse(http.StatusOK, `{"Volumes":[]}`), nil
		case request.Method == http.MethodDelete:
			return dockerResponse(http.StatusInternalServerError, `{"message":"busy"}`), nil
		case request.Method == http.MethodPost && request.URL.Path == "/volumes/prune":
			return dockerResponse(http.StatusOK, `{"VolumesDeleted":[]}`), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}
	if err := manager.CleanupProfile(context.Background()); err == nil {
		t.Fatal("cleanup deletion failure was hidden")
	}
}

type trackedDockerBody struct {
	io.Reader
	closed *atomic.Int32
	err    error
}

func (body *trackedDockerBody) Close() error {
	body.closed.Add(1)
	return body.err
}

func TestTeardownClosesBodiesAndJoinsDockerStatusFailures(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "teardown")
	var responses atomic.Int32
	var closed atomic.Int32
	manager := &ServiceManager{dockerTimeout: dockerRequestTimeout}
	manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		responses.Add(1)
		status := http.StatusOK
		body := `{"Id":"immutable-container-id","State":{"Status":"running"},"Config":{"Labels":{"managed-by":"minisky","minisky.profile":"teardown"}}}`
		switch request.Method {
		case http.MethodPost:
			status = http.StatusInternalServerError
			body = `{"message":"stop failed"}`
		case http.MethodDelete:
			status = http.StatusConflict
			body = `{"message":"remove failed"}`
		}
		var closeErr error
		if request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/networks/") {
			closeErr = errors.New("close failed")
		}
		return &http.Response{
			StatusCode: status,
			Body: &trackedDockerBody{
				Reader: strings.NewReader(body),
				closed: &closed,
				err:    closeErr,
			},
			Header:  make(http.Header),
			Request: request,
		}, nil
	})}

	err := manager.Teardown(context.Background())
	if err == nil || !strings.Contains(err.Error(), "stop") ||
		!strings.Contains(err.Error(), "remove") || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("teardown error = %v, want joined stop/remove/close failures", err)
	}
	if got, want := closed.Load(), responses.Load(); got != want {
		t.Fatalf("closed response bodies = %d, want %d", got, want)
	}
}

func TestTeardownDeletesCapturedContainerAndNetworkIDs(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "teardown")
	var mutations []string
	manager := &ServiceManager{dockerTimeout: dockerRequestTimeout}
	manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/containers/"):
			return dockerResponse(http.StatusOK,
				`{"Id":"container/id","State":{"Status":"running"},"Config":{"Labels":{"managed-by":"minisky","minisky.profile":"teardown"}}}`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/networks/minisky-net":
			return dockerResponse(http.StatusOK,
				`{"Id":"network/id","Labels":{"managed-by":"minisky","minisky.profile":"teardown"}}`), nil
		case request.Method == http.MethodPost || request.Method == http.MethodDelete:
			mutations = append(mutations, request.Method+" "+request.URL.EscapedPath())
			return dockerResponse(http.StatusNoContent, ""), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}
	if err := manager.Teardown(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range mutations {
		if strings.Contains(mutation, "minisky-") {
			t.Fatalf("teardown mutated by replaceable name: %s", mutation)
		}
	}
	if !slices.Contains(mutations, "POST /containers/container%2Fid/stop") ||
		!slices.Contains(mutations, "DELETE /containers/container%2Fid") ||
		!slices.Contains(mutations, "DELETE /networks/network%2Fid") {
		t.Fatalf("immutable-ID mutations = %#v", mutations)
	}
}

func TestCleanupProfilePrunesVolumesOnlyByServerSideLabels(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "cleanup")
	pruned := false
	manager := &ServiceManager{dockerTimeout: dockerRequestTimeout}
	manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/containers/json":
			return dockerResponse(http.StatusOK, `[]`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/networks":
			return dockerResponse(http.StatusOK, `[]`), nil
		case request.Method == http.MethodPost && request.URL.Path == "/volumes/prune":
			pruned = true
			filters := request.URL.Query().Get("filters")
			if !strings.Contains(filters, "managed-by=minisky") ||
				!strings.Contains(filters, "minisky.profile=cleanup") || !strings.Contains(filters, `"all":["true"]`) {
				t.Fatalf("unsafe volume prune filters: %s", filters)
			}
			return dockerResponse(http.StatusOK, `{"VolumesDeleted":["replaceable"]}`), nil
		case strings.HasPrefix(request.URL.Path, "/volumes/"):
			t.Fatalf("volume cleanup used replaceable name: %s %s", request.Method, request.URL)
			return nil, nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}
	if err := manager.CleanupProfile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !pruned {
		t.Fatal("profile cleanup did not use atomic server-side prune")
	}
}

func TestCleanupAllProfilesRequiresExactMiniSkyOwnershipLabels(t *testing.T) {
	var deleted []string
	manager := &ServiceManager{dockerTimeout: dockerRequestTimeout}
	manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/containers/json":
			return dockerResponse(http.StatusOK, `[
				{"Id":"one","Labels":{"managed-by":"minisky","minisky.profile":"one"}},
				{"Id":"two","Labels":{"managed-by":"minisky","minisky.profile":"two"}},
				{"Id":"missing-profile","Labels":{"managed-by":"minisky"}},
				{"Id":"foreign","Labels":{"managed-by":"other","minisky.profile":"one"}}
			]`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/networks":
			return dockerResponse(http.StatusOK, `[]`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/volumes":
			return dockerResponse(http.StatusOK, `{"Volumes":[]}`), nil
		case request.Method == http.MethodDelete:
			deleted = append(deleted, request.URL.Path)
			return dockerResponse(http.StatusNoContent, ""), nil
		case request.Method == http.MethodPost && request.URL.Path == "/volumes/prune":
			filters := request.URL.Query().Get("filters")
			if !strings.Contains(filters, "managed-by=minisky") ||
				!strings.Contains(filters, `"minisky.profile"`) || !strings.Contains(filters, `"all":["true"]`) {
				t.Fatalf("unsafe all-profile volume prune filters: %s", filters)
			}
			return dockerResponse(http.StatusOK, `{"VolumesDeleted":[]}`), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}
	if err := manager.CleanupAllProfiles(context.Background()); err != nil {
		t.Fatal(err)
	}
	sort.Strings(deleted)
	if !reflect.DeepEqual(deleted, []string{"/containers/one", "/containers/two"}) {
		t.Fatalf("deleted = %#v", deleted)
	}
}

func TestEmulatorVolumesUseProfileScopedRuntimePaths(t *testing.T) {
	root := t.TempDir()
	t.Setenv("MINISKY_STATE_DIR", root)
	t.Setenv("MINISKY_PROFILE", "restart")

	for _, service := range []string{"datastore", "firestore"} {
		got, err := resolveEmulatorVolume(service+".googleapis.com", "./data/"+service+":/data")
		if err != nil {
			t.Fatal(err)
		}
		want := filepath.Join(root, "profiles", "restart", "runtime", service) + ":/data"
		if got != want {
			t.Fatalf("%s volume = %q, want %q", service, got, want)
		}
	}
}

func TestDockerOwnershipRequiresManagerAndProfileLabels(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "restart")

	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{name: "owned", labels: map[string]string{"managed-by": "minisky", "minisky.profile": "restart"}, want: true},
		{name: "missing manager", labels: map[string]string{"minisky.profile": "restart"}, want: false},
		{name: "other manager", labels: map[string]string{"managed-by": "someone-else", "minisky.profile": "restart"}, want: false},
		{name: "other profile", labels: map[string]string{"managed-by": "minisky", "minisky.profile": "other"}, want: false},
		{name: "legacy unlabeled", labels: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isOwnedDockerResource(tt.labels); got != tt.want {
				t.Fatalf("owned = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestTerminalExecTargetRequiresActiveProfileOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "terminal-test")
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || !strings.Contains(request.URL.Path, "/containers/") {
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
		}
		return dockerResponse(http.StatusOK, `{
			"State":{"Status":"running"},
			"Config":{"Labels":{"managed-by":"minisky","minisky.profile":"other-profile"}}
		}`), nil
	})}}

	if err := manager.validateExecTarget("minisky-vm"); err == nil {
		t.Fatal("terminal exec accepted a container owned by another profile")
	}
}

func TestRunCommandRequiresExactOwnedComputeContainerAndRedactsArguments(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "active")
	var logs bytes.Buffer
	previousWriter := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previousWriter) })

	mutated := false
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			mutated = true
		}
		return dockerResponse(http.StatusOK,
			`{"State":{"Status":"running"},"Config":{"Labels":{"managed-by":"minisky","minisky.profile":"other","minisky.service":"compute-instance","minisky.resource":"minisky-dataproc-cluster-m"}}}`), nil
	})}}
	_, err := manager.RunCommandInContainer("minisky-dataproc-cluster-m", []string{"spark-submit", "--password=secret"})
	if err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("exec error = %v, want ownership refusal", err)
	}
	if mutated {
		t.Fatal("unowned container reached Docker exec mutation")
	}
	if strings.Contains(logs.String(), "secret") {
		t.Fatalf("command arguments leaked to logs: %s", logs.String())
	}
}

func TestListManagedContainersFiltersActiveProfileOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "active")
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return dockerResponse(http.StatusOK, `[
			{"Names":["/minisky-owned"],"Status":"Up","Image":"owned","Labels":{"managed-by":"minisky","minisky.profile":"active"}},
			{"Names":["/minisky-other"],"Status":"Up","Image":"other","Labels":{"managed-by":"minisky","minisky.profile":"other"}},
			{"Names":["/minisky-unowned"],"Status":"Up","Image":"user","Labels":{}}
		]`), nil
	})}}
	containers := manager.ListManagedContainers()
	if len(containers) != 1 || containers[0].Name != "minisky-owned" {
		t.Fatalf("managed containers = %#v", containers)
	}
}

func TestContainerLogsAndStatsRequireActiveProfileOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "active")
	mutated := false
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/json") {
			return dockerResponse(http.StatusOK, `{"State":{"Status":"running"},"Config":{"Labels":{"managed-by":"minisky","minisky.profile":"other"}}}`), nil
		}
		mutated = true
		return dockerResponse(http.StatusOK, `{}`), nil
	})}}
	if _, err := manager.GetContainerLogs("minisky-other", 10); err == nil {
		t.Fatal("logs allowed cross-profile container")
	}
	if _, err := manager.GetContainerStats("minisky-other"); err == nil {
		t.Fatal("stats allowed cross-profile container")
	}
	if mutated {
		t.Fatal("cross-profile request reached logs or stats endpoint")
	}
}

func TestCloudBuildResourcesUseExactOwnershipLabels(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "build-profile")
	const resource = "projects/demo/builds/build-1"
	var volumeLabels, containerLabels map[string]string
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/volumes/"):
			return dockerResponse(http.StatusNotFound, `{}`), nil
		case request.Method == http.MethodPost && request.URL.Path == "/volumes/create":
			var payload struct {
				Labels map[string]string `json:"Labels"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			volumeLabels = payload.Labels
			return dockerResponse(http.StatusCreated, `{}`), nil
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/containers/"):
			return dockerResponse(http.StatusNotFound, `{}`), nil
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
			return dockerResponse(http.StatusOK, `{}`), nil
		case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
			var payload struct {
				Labels map[string]string `json:"Labels"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			containerLabels = payload.Labels
			return dockerResponse(http.StatusCreated, `{}`), nil
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/start"):
			return dockerResponse(http.StatusNoContent, `{}`), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}}
	if err := manager.EnsureBuildWorkspace(context.Background(), "workspace", resource); err != nil {
		t.Fatal(err)
	}
	if err := manager.ProvisionBuildStep(context.Background(), "build-step", resource, "alpine:latest", []string{"workspace:/workspace"}, nil, nil); err != nil {
		t.Fatal(err)
	}
	expected := buildResourceLabels(resource)
	if !exactLabels(volumeLabels, expected) || !exactLabels(containerLabels, expected) {
		t.Fatalf("volume labels=%v container labels=%v expected=%v", volumeLabels, containerLabels, expected)
	}
}

func TestCloudBuildCleanupRefusesCrossProfileResources(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "active")
	deleted := false
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			return dockerResponse(http.StatusOK, `{"State":{"Status":"running"},"Config":{"Labels":{"managed-by":"minisky","minisky.profile":"other","minisky.service":"cloudbuild","minisky.resource":"projects/demo/builds/build-1"}}}`), nil
		}
		deleted = true
		return dockerResponse(http.StatusNoContent, `{}`), nil
	})}}
	if err := manager.StopAndRemoveBuildContainer(context.Background(), "build-step", "projects/demo/builds/build-1"); err == nil {
		t.Fatal("cross-profile build container cleanup succeeded")
	}
	if deleted {
		t.Fatal("cross-profile build container was mutated")
	}
}

func TestCloudBuildCrashCleanupRemovesOnlyActiveProfileResources(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "active")
	var deleted []string
	requestCount := 0
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestCount++
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/containers/json":
			return dockerResponse(http.StatusOK, `[
				{"Id":"owned-container","Labels":{"managed-by":"minisky","minisky.profile":"active","minisky.service":"cloudbuild","minisky.resource":"projects/demo/builds/1"}},
				{"Id":"other-container","Labels":{"managed-by":"minisky","minisky.profile":"other","minisky.service":"cloudbuild","minisky.resource":"projects/demo/builds/1"}}
			]`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/volumes":
			return dockerResponse(http.StatusOK, `{"Volumes":[
				{"Name":"owned-volume","Labels":{"managed-by":"minisky","minisky.profile":"active","minisky.service":"cloudbuild","minisky.resource":"projects/demo/builds/1"}},
				{"Name":"user-volume","Labels":{"managed-by":"someone-else","minisky.profile":"active","minisky.service":"cloudbuild","minisky.resource":"projects/demo/builds/1"}}
			]}`), nil
		case request.Method == http.MethodDelete:
			deleted = append(deleted, request.URL.Path)
			return dockerResponse(http.StatusNoContent, `{}`), nil
		default:
			t.Fatalf("unexpected Docker request #%d %s %s", requestCount, request.Method, request.URL)
			return nil, nil
		}
	})}}
	if err := manager.ReconcileBuildResources(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 2 || deleted[0] != "/containers/owned-container" || deleted[1] != "/volumes/owned-volume" {
		t.Fatalf("deleted resources = %v", deleted)
	}
}

func TestVPCNetworkUsesOwnedLabelsAndValidatedIPAM(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "network-test")
	var createPayload map[string]any
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && strings.Contains(request.URL.Path, "/networks/"):
			return dockerResponse(http.StatusNotFound, `{}`), nil
		case request.Method == http.MethodPost && request.URL.Path == "/networks/create":
			if err := json.NewDecoder(request.Body).Decode(&createPayload); err != nil {
				t.Fatal(err)
			}
			return dockerResponse(http.StatusCreated, `{"Id":"network"}`), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}}
	if err := manager.CreateVPCNetworkWithSubnet(context.Background(), "private", "10.42.0.0/24"); err != nil {
		t.Fatal(err)
	}
	labels := createPayload["Labels"].(map[string]any)
	if labels["managed-by"] != "minisky" || labels["minisky.profile"] != "network-test" ||
		labels["minisky.service"] != "compute-network" {
		t.Fatalf("labels = %#v", labels)
	}
	ipam := createPayload["IPAM"].(map[string]any)
	config := ipam["Config"].([]any)[0].(map[string]any)
	if config["Subnet"] != "10.42.0.0/24" {
		t.Fatalf("IPAM = %#v", ipam)
	}
	if err := manager.CreateVPCNetworkWithSubnet(context.Background(), "private", "not-a-cidr"); err == nil {
		t.Fatal("invalid CIDR was accepted")
	}
}

func TestVPCNetworkRefusesUnownedCreateAndDelete(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "network-test")
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			t.Fatalf("unowned network caused mutation: %s %s", request.Method, request.URL)
		}
		return dockerResponse(http.StatusOK, `{"Labels":{"managed-by":"someone-else"}}`), nil
	})}}
	if err := manager.CreateVPCNetwork(context.Background(), "private"); err == nil {
		t.Fatal("unowned same-name network was adopted")
	}
	if err := manager.DeleteVPCNetwork(context.Background(), "private"); err == nil {
		t.Fatal("unowned network was deleted")
	}
}

func TestEmulatorAdditionalPortsAreLoopbackPublished(t *testing.T) {
	var payload struct {
		ExposedPorts map[string]any `json:"ExposedPorts"`
		HostConfig   struct {
			PortBindings map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			} `json:"PortBindings"`
		} `json:"HostConfig"`
	}
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		return dockerResponse(http.StatusCreated, `{}`), nil
	})}}
	err := manager.createContainer(ContainerConfig{
		Name: "spanner", Image: "spanner", ContainerPort: "9020/tcp",
		AdditionalPorts: []string{"9010/tcp"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, port := range []string{"9010/tcp", "9020/tcp"} {
		if _, ok := payload.ExposedPorts[port]; !ok {
			t.Fatalf("port %s was not exposed: %#v", port, payload.ExposedPorts)
		}
		bindings := payload.HostConfig.PortBindings[port]
		if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" || bindings[0].HostPort != "0" {
			t.Fatalf("bindings for %s = %#v", port, bindings)
		}
	}
}

func TestRedisProvisioningRefusesUnownedExistingVolume(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "redis-test")
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(request.URL.Path, "/containers/"):
			return dockerResponse(http.StatusNotFound, `{}`), nil
		case strings.Contains(request.URL.Path, "/images/"):
			return dockerResponse(http.StatusOK, `{}`), nil
		case strings.Contains(request.URL.Path, "/volumes/"):
			return dockerResponse(http.StatusOK, `{"Labels":{"managed-by":"someone-else"}}`), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})}}
	if _, err := manager.ProvisionRedis(context.Background(), "resource", "redis:test"); err == nil {
		t.Fatal("unowned existing Redis volume was adopted")
	}
}

func TestPullImageUsesContextAndEncodedImageName(t *testing.T) {
	const image = "gcr.io/cloud-spanner-emulator/emulator:latest"
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Path != "/images/create" {
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
		}
		if got := request.URL.Query().Get("fromImage"); got != image {
			t.Fatalf("fromImage = %q, want %q", got, image)
		}
		return dockerResponse(http.StatusOK, `{"status":"Pull complete"}`), nil
	})}}

	if err := manager.pullImageInternal(context.Background(), image); err != nil {
		t.Fatal(err)
	}
}

func TestPullImageReportsDockerHTTPAndStreamErrors(t *testing.T) {
	tests := []struct {
		name string
		code int
		body string
		want string
	}{
		{name: "http status", code: http.StatusInternalServerError, body: `{"message":"registry unavailable"}`, want: "registry unavailable"},
		{name: "stream error", code: http.StatusOK, body: `{"errorDetail":{"message":"manifest unknown"},"error":"manifest unknown"}`, want: "manifest unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return dockerResponse(tt.code, tt.body), nil
			})}}
			err := manager.pullImageInternal(context.Background(), "example.invalid/image:tag")
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("pull error = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestPullImageHonorsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		cancel()
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}}

	err := manager.pullImageInternal(ctx, "example.invalid/image:tag")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pull error = %v, want context cancellation", err)
	}
}

func TestDeleteServerlessVMRequiresExactCurrentProfileOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "serverless-test")
	identity := ServerlessIdentity{
		ResourceType: ServerlessFunction,
		Project:      "demo",
		Location:     "us-central1",
		Name:         "Hello_World",
	}
	ownedLabels, err := identity.labels()
	if err != nil {
		t.Fatal(err)
	}
	encodedOwnedLabels, err := json.Marshal(ownedLabels)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name       string
		labels     string
		status     int
		wantError  bool
		wantDelete bool
	}{
		{
			name:       "owned",
			labels:     string(encodedOwnedLabels),
			status:     http.StatusOK,
			wantDelete: true,
		},
		{
			name:      "cross profile",
			labels:    strings.Replace(string(encodedOwnedLabels), `"serverless-test"`, `"other"`, 1),
			status:    http.StatusOK,
			wantError: true,
		},
		{
			name:      "unrelated service",
			labels:    strings.Replace(string(encodedOwnedLabels), `"serverless"`, `"compute-instance"`, 1),
			status:    http.StatusOK,
			wantError: true,
		},
		{
			name:      "different canonical resource",
			labels:    strings.Replace(string(encodedOwnedLabels), identity.CanonicalResource(), "projects/other/locations/us-central1/functions/Hello_World", 1),
			status:    http.StatusOK,
			wantError: true,
		},
		{
			name:   "missing is idempotent",
			status: http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleted := false
			manager := &ServiceManager{
				portRegistry: make(map[string][]PortMapping),
				dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					switch request.Method {
					case http.MethodGet:
						body := `{}`
						if tt.status == http.StatusOK {
							body = `{"State":{"Status":"running"},"Config":{"Labels":` + tt.labels + `}}`
						}
						return dockerResponse(tt.status, body), nil
					case http.MethodPost:
						return dockerResponse(http.StatusNoContent, `{}`), nil
					case http.MethodDelete:
						deleted = true
						return dockerResponse(http.StatusNoContent, `{}`), nil
					default:
						t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
						return nil, nil
					}
				})},
			}

			err := manager.DeleteServerlessVM(identity)
			if (err != nil) != tt.wantError {
				t.Fatalf("DeleteServerlessVM error = %v, wantError=%t", err, tt.wantError)
			}
			if deleted != tt.wantDelete {
				t.Fatalf("Docker delete called = %t, want %t", deleted, tt.wantDelete)
			}
		})
	}
}

func TestServerlessIdentitySeparatesTypeProjectAndLocation(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "serverless-test")
	identities := []ServerlessIdentity{
		{ResourceType: ServerlessFunction, Project: "project-a", Location: "us-central1", Name: "hello"},
		{ResourceType: ServerlessService, Project: "project-a", Location: "us-central1", Name: "hello"},
		{ResourceType: ServerlessFunction, Project: "project-b", Location: "us-central1", Name: "hello"},
		{ResourceType: ServerlessFunction, Project: "project-a", Location: "europe-west1", Name: "hello"},
	}
	names := make(map[string]bool, len(identities))
	images := make(map[string]bool, len(identities))
	for _, identity := range identities {
		name, err := identity.ContainerName()
		if err != nil {
			t.Fatal(err)
		}
		if names[name] {
			t.Fatalf("duplicate container name %q for identity %#v", name, identity)
		}
		names[name] = true
		image, err := identity.ImageName()
		if err != nil {
			t.Fatal(err)
		}
		if images[image] {
			t.Fatalf("duplicate image name %q for identity %#v", image, identity)
		}
		images[image] = true
	}
}

func TestProvisionServerlessVMUsesIdentityNameAndOwnershipLabels(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "serverless-test")
	identity := ServerlessIdentity{
		ResourceType: ServerlessFunction,
		Project:      "demo",
		Location:     "us-central1",
		Name:         "Hello_World",
	}
	containerName, err := identity.ContainerName()
	if err != nil {
		t.Fatal(err)
	}
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer app.Close()
	appPort := strings.TrimPrefix(app.URL, "http://127.0.0.1:")

	var createPayload struct {
		Labels map[string]string `json:"Labels"`
	}
	inspectCount := 0
	manager := &ServiceManager{
		portRegistry: make(map[string][]PortMapping),
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/json"):
				inspectCount++
				if inspectCount == 1 {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				return dockerResponse(http.StatusOK, `{"NetworkSettings":{"Ports":{"8080/tcp":[{"HostIp":"127.0.0.1","HostPort":"`+appPort+`"}]}}}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				if got := request.URL.Query().Get("name"); got != containerName {
					t.Fatalf("container name = %q", got)
				}
				if err := json.NewDecoder(request.Body).Decode(&createPayload); err != nil {
					t.Fatal(err)
				}
				return dockerResponse(http.StatusCreated, `{}`), nil
			case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/start"):
				return dockerResponse(http.StatusNoContent, `{}`), nil
			default:
				t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
				return nil, nil
			}
		})},
	}

	gotURL, err := manager.ProvisionServerlessVM(identity, "example/service:local", []string{"PORT=8080"})
	if err != nil {
		t.Fatal(err)
	}
	if gotURL != app.URL {
		t.Fatalf("service URL = %q, want %q", gotURL, app.URL)
	}
	if createPayload.Labels["managed-by"] != "minisky" ||
		createPayload.Labels["minisky.profile"] != "serverless-test" ||
		createPayload.Labels["minisky.service"] != "serverless" ||
		createPayload.Labels["minisky.resource"] != identity.CanonicalResource() ||
		createPayload.Labels["minisky.resource-type"] != string(ServerlessFunction) {
		t.Fatalf("serverless labels = %#v", createPayload.Labels)
	}
}

func TestProvisionServerlessVMCleansOwnedContainerAfterPostCreateFailure(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "serverless-cleanup")
	identity := ServerlessIdentity{
		ResourceType: ServerlessService,
		Project:      "demo",
		Location:     "us-central1",
		Name:         "cleanup",
	}
	labels, err := identity.labels()
	if err != nil {
		t.Fatal(err)
	}
	encodedLabels, err := json.Marshal(labels)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name          string
		stage         string
		cleanupStatus int
		cleanupLabels string
		want          string
		wantCleanup   bool
	}{
		{name: "start", stage: "start", cleanupStatus: http.StatusNoContent, cleanupLabels: string(encodedLabels), want: "start Serverless container", wantCleanup: true},
		{name: "port discovery", stage: "discover", cleanupStatus: http.StatusNoContent, cleanupLabels: string(encodedLabels), want: "port discovery", wantCleanup: true},
		{name: "readiness", stage: "readiness", cleanupStatus: http.StatusNoContent, cleanupLabels: string(encodedLabels), want: "readiness failed", wantCleanup: true},
		{name: "cleanup failure is appended", stage: "start", cleanupStatus: http.StatusInternalServerError, cleanupLabels: string(encodedLabels), want: "cleanup owned backend failed", wantCleanup: true},
		{name: "ownership refusal stays safe", stage: "start", cleanupStatus: http.StatusNoContent, cleanupLabels: `{"managed-by":"someone-else"}`, want: "refusing to delete unowned", wantCleanup: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			created := false
			inspectAfterCreate := 0
			deleteCalls := 0
			manager := &ServiceManager{
				portRegistry: make(map[string][]PortMapping),
				serverlessReady: func(string, time.Duration) error {
					if tt.stage == "readiness" {
						return errors.New("readiness failed")
					}
					return nil
				},
				dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					switch {
					case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/json"):
						if !created {
							return dockerResponse(http.StatusNotFound, `{}`), nil
						}
						inspectAfterCreate++
						if tt.stage == "discover" && inspectAfterCreate == 1 {
							return dockerResponse(http.StatusOK, `{"NetworkSettings":{"Ports":{}}}`), nil
						}
						if tt.stage == "readiness" && inspectAfterCreate == 1 {
							return dockerResponse(http.StatusOK, `{"NetworkSettings":{"Ports":{"8080/tcp":[{"HostIp":"127.0.0.1","HostPort":"12345"}]}}}`), nil
						}
						return dockerResponse(http.StatusOK, `{"State":{"Status":"running"},"Config":{"Labels":`+tt.cleanupLabels+`}}`), nil
					case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
						created = true
						return dockerResponse(http.StatusCreated, `{}`), nil
					case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/start"):
						if tt.stage == "start" {
							return dockerResponse(http.StatusInternalServerError, `{"message":"start failed"}`), nil
						}
						return dockerResponse(http.StatusNoContent, `{}`), nil
					case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/stop"):
						return dockerResponse(http.StatusNoContent, `{}`), nil
					case request.Method == http.MethodDelete:
						deleteCalls++
						return dockerResponse(tt.cleanupStatus, `{}`), nil
					default:
						t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
						return nil, nil
					}
				})},
			}

			_, err := manager.ProvisionServerlessVM(identity, "example/service:local", nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("provision error = %v, want text %q", err, tt.want)
			}
			if got := deleteCalls > 0; got != tt.wantCleanup {
				t.Fatalf("cleanup delete called = %t, want %t", got, tt.wantCleanup)
			}
		})
	}
}

func TestServerlessLifecycleGateRejectsSameIdentityAndAllowsUnrelatedIdentity(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "serverless-lifecycle")
	identityA := ServerlessIdentity{
		ResourceType: ServerlessFunction,
		Project:      "demo",
		Location:     "us-central1",
		Name:         "shared",
	}
	identityB := identityA
	identityB.Name = "other"
	nameA, err := identityA.ContainerName()
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	manager := &ServiceManager{
		portRegistry: make(map[string][]PortMapping),
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodGet {
				t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			}
			if strings.Contains(request.URL.Path, nameA) {
				select {
				case <-started:
				default:
					close(started)
				}
				<-release
			}
			return dockerResponse(http.StatusNotFound, `{}`), nil
		})},
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- manager.DeleteServerlessVM(identityA)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first lifecycle did not reach backend")
	}

	if err := manager.DeleteServerlessVM(identityA); !errors.Is(err, ErrServerlessLifecycleInProgress) {
		t.Fatalf("same-identity delete error = %v", err)
	}
	if err := manager.DeleteServerlessVM(identityB); err != nil {
		t.Fatalf("unrelated identity was blocked: %v", err)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first delete failed: %v", err)
	}
	manager.serverlessMu.Lock()
	active := len(manager.serverlessActive)
	manager.serverlessMu.Unlock()
	if active != 0 {
		t.Fatalf("active lifecycle entries after release = %d", active)
	}
}

func TestDeleteCloudSQLVolumeRequiresExactOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "sql-volume")
	expected := ownedDockerLabels()
	expected["minisky.service"] = "cloudsql"
	expected["minisky.resource"] = "db"
	for _, test := range []struct {
		name       string
		labels     map[string]string
		wantDelete bool
	}{
		{name: "owned", labels: expected, wantDelete: true},
		{name: "other profile", labels: map[string]string{
			"managed-by": "minisky", "minisky.profile": "other",
			"minisky.service": "cloudsql", "minisky.resource": "db",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			deleted := false
			manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				switch request.Method {
				case http.MethodGet:
					body, _ := json.Marshal(map[string]any{"Labels": test.labels})
					return dockerResponse(http.StatusOK, string(body)), nil
				case http.MethodDelete:
					deleted = true
					return dockerResponse(http.StatusNoContent, ""), nil
				default:
					t.Fatalf("unexpected request %s %s", request.Method, request.URL)
					return nil, nil
				}
			})}}
			err := manager.deleteCloudSQLVolume(context.Background(), "minisky-db-db", expected)
			if deleted != test.wantDelete {
				t.Fatalf("deleted = %t, want %t, error %v", deleted, test.wantDelete, err)
			}
			if !test.wantDelete && err == nil {
				t.Fatal("unowned volume deletion succeeded")
			}
		})
	}
}

func TestEnsureCloudSQLVolumeLabelsNewVolume(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "sql-volume")
	expected := ownedDockerLabels()
	expected["minisky.service"] = "cloudsql"
	expected["minisky.resource"] = "db"
	var created struct {
		Name   string            `json:"Name"`
		Labels map[string]string `json:"Labels"`
	}
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			return dockerResponse(http.StatusNotFound, ""), nil
		}
		if err := json.NewDecoder(request.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		return dockerResponse(http.StatusCreated, ""), nil
	})}}
	wasCreated, err := manager.ensureCloudSQLVolume(context.Background(), "minisky-db-db", expected)
	if err != nil {
		t.Fatal(err)
	}
	if !wasCreated || created.Name != "minisky-db-db" || !exactLabels(created.Labels, expected) {
		t.Fatalf("created volume = %#v, wasCreated=%t", created, wasCreated)
	}
}

func TestCloudSQLDockerIdentitySeparatesProjects(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "shared-profile")
	containerA, volumeA, resourceA := cloudSQLDockerNames("project-a", "db")
	containerB, volumeB, resourceB := cloudSQLDockerNames("project-b", "db")
	if containerA == containerB || volumeA == volumeB || resourceA == resourceB {
		t.Fatalf("cross-project identities collided: %q %q %q", containerA, volumeA, resourceA)
	}
}

func dockerResponse(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}
