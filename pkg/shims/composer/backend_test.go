package composer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingAirflowRunner struct {
	mu            sync.Mutex
	commands      [][]string
	exists        bool
	inspectErr    error
	inspectOutput []byte
	dagID         string
	dagReadyAfter int
	dagListCalls  int
	listFailures  int
}

func (r *recordingAirflowRunner) Run(_ context.Context, args ...string) ([]byte, error) {
	r.mu.Lock()
	r.commands = append(r.commands, append([]string(nil), args...))
	r.mu.Unlock()
	switch args[0] {
	case "inspect":
		if r.inspectErr != nil {
			return r.inspectOutput, r.inspectErr
		}
		if !r.exists {
			return []byte("error: no such object: " + args[1]), errors.New("exit status 1")
		}
		return []byte(`[{"Config":{"Labels":{"managed-by":"minisky","minisky.profile":"composer-backend-test","minisky.service":"composer-airflow","minisky.resource":"projects/p/locations/l/environments/e"}}}]`), nil
	case "run":
		r.mu.Lock()
		r.exists = true
		r.mu.Unlock()
		return []byte("container"), nil
	case "port":
		return []byte("127.0.0.1:18080\n"), nil
	default:
		if strings.Contains(strings.Join(args, " "), "airflow dags list") {
			r.mu.Lock()
			r.dagListCalls++
			calls := r.dagListCalls
			r.mu.Unlock()
			if calls <= r.listFailures {
				return []byte("scheduler not ready"), errors.New("exit status 1")
			}
			if r.dagID != "" && calls >= r.dagReadyAfter {
				return []byte(`[{"dag_id":"` + r.dagID + `"}]`), nil
			}
			return []byte(`[]`), nil
		}
		return []byte("ok"), nil
	}
}

func TestPinnedAirflowBackendProvisionsUploadsAndTriggersDAG(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "composer-backend-test")
	runner := &recordingAirflowRunner{dagID: "example"}
	backend := &dockerAirflowBackend{runner: runner}
	resource := "projects/p/locations/l/environments/e"

	endpoint, err := backend.Provision(context.Background(), resource)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "http://127.0.0.1:18080" {
		t.Fatalf("unexpected endpoint %q", endpoint)
	}
	if err := backend.UploadDAG(context.Background(), resource, "example",
		"from airflow import DAG\n"); err != nil {
		t.Fatal(err)
	}
	if err := backend.TriggerDAG(context.Background(), resource, "example", "run-1"); err != nil {
		t.Fatal(err)
	}

	joined := commandsText(runner.commands)
	for _, required := range []string{
		airflowImage,
		"managed-by=minisky",
		"minisky.profile=composer-backend-test",
		"minisky.service=composer-airflow",
		"minisky.resource=" + resource,
		"airflow dags list",
		"exec --user root " + airflowContainerName(resource) + " chmod 0644 /opt/airflow/dags/example.py",
		"airflow dags trigger --run-id run-1 example",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("missing command fragment %q in %s", required, joined)
		}
	}
}

func TestAirflowUploadWaitsForSpecificDAGReadiness(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "composer-backend-test")
	runner := &recordingAirflowRunner{
		exists:        true,
		dagID:         "example",
		dagReadyAfter: 2,
	}
	backend := &dockerAirflowBackend{runner: runner}

	if err := backend.UploadDAG(context.Background(),
		"projects/p/locations/l/environments/e", "example", "from airflow import DAG\n"); err != nil {
		t.Fatal(err)
	}
	if runner.dagListCalls != 2 {
		t.Fatalf("DAG list calls = %d, want 2 readiness attempts", runner.dagListCalls)
	}
}

func TestAirflowDAGReadinessRequiresExactStructuredID(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "composer-backend-test")
	runner := &recordingAirflowRunner{exists: true, dagID: "example-copy"}
	backend := &dockerAirflowBackend{runner: runner}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := backend.UploadDAG(ctx, "projects/p/locations/l/environments/e", "example",
		"from airflow import DAG\n")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("UploadDAG error = %v, want wrapped deadline", err)
	}
}

func TestAirflowDAGReadinessWrapsCancellation(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "composer-backend-test")
	runner := &recordingAirflowRunner{exists: true}
	backend := &dockerAirflowBackend{runner: runner}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := backend.UploadDAG(ctx, "projects/p/locations/l/environments/e", "example",
		"from airflow import DAG\n")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("UploadDAG error = %v, want wrapped cancellation", err)
	}
}

func TestAirflowReconcileRetriesReadinessUntilSuccess(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "composer-backend-test")
	runner := &recordingAirflowRunner{exists: true, listFailures: 2}
	backend := &dockerAirflowBackend{runner: runner}

	endpoint, owned, err := backend.Reconcile(context.Background(), "projects/p/locations/l/environments/e")
	if err != nil {
		t.Fatal(err)
	}
	if !owned || endpoint != "http://127.0.0.1:18080" || runner.dagListCalls != 3 {
		t.Fatalf("reconcile = (%q, %v), list calls=%d", endpoint, owned, runner.dagListCalls)
	}
}

func TestAirflowReconcileWrapsReadinessDeadline(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "composer-backend-test")
	runner := &recordingAirflowRunner{exists: true, listFailures: 1000}
	backend := &dockerAirflowBackend{runner: runner}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, _, err := backend.Reconcile(ctx, "projects/p/locations/l/environments/e")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reconcile error = %v, want wrapped deadline", err)
	}
}

func TestAirflowBackendPropagatesDockerInspectFailure(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "composer-backend-test")
	daemonErr := errors.New("exit status 1")
	backend := &dockerAirflowBackend{runner: &recordingAirflowRunner{
		inspectErr:    daemonErr,
		inspectOutput: []byte("permission denied while trying to connect to the Docker daemon socket"),
	}}

	err := backend.Delete(context.Background(), "projects/p/locations/l/environments/e")
	if !errors.Is(err, daemonErr) {
		t.Fatalf("Delete error = %v, want Docker inspect error", err)
	}
}

func TestAirflowReconcileDoesNotCreateMissingContainer(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "composer-backend-test")
	runner := &recordingAirflowRunner{}
	backend := &dockerAirflowBackend{runner: runner}

	endpoint, ok, err := backend.Reconcile(context.Background(), "projects/p/locations/l/environments/e")
	if err != nil {
		t.Fatal(err)
	}
	if ok || endpoint != "" {
		t.Fatalf("reconcile = (%q, %v), want missing backend", endpoint, ok)
	}
	if strings.Contains(commandsText(runner.commands), "run ") {
		t.Fatalf("reconcile created a container: %s", commandsText(runner.commands))
	}
}

func commandsText(commands [][]string) string {
	var lines []string
	for _, command := range commands {
		lines = append(lines, strings.Join(command, " "))
	}
	return strings.Join(lines, "\n")
}
