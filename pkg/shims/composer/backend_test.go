package composer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

type recordingAirflowRunner struct {
	mu            sync.Mutex
	commands      [][]string
	exists        bool
	inspectErr    error
	inspectOutput []byte
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
		return []byte("ok"), nil
	}
}

func TestPinnedAirflowBackendProvisionsUploadsAndTriggersDAG(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "composer-backend-test")
	runner := &recordingAirflowRunner{}
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
		"airflow dags trigger --run-id run-1 example",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("missing command fragment %q in %s", required, joined)
		}
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

func commandsText(commands [][]string) string {
	var lines []string
	for _, command := range commands {
		lines = append(lines, strings.Join(command, " "))
	}
	return strings.Join(lines, "\n")
}
