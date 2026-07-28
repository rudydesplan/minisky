package composer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
)

const airflowDockerService = "composer-airflow"

var (
	airflowImage      = config.GetImageRegistry().Composer.AirflowImage
	airflowIdentifier = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
)

type unavailableAirflowBackend struct{}

func (unavailableAirflowBackend) Provision(context.Context, string) (string, error) {
	return "", errors.New("Airflow backend unavailable")
}
func (unavailableAirflowBackend) Reconcile(context.Context, string) (string, bool, error) {
	return "", false, errors.New("Airflow backend unavailable")
}
func (unavailableAirflowBackend) Delete(context.Context, string) error {
	return errors.New("Airflow backend unavailable")
}

type airflowCommandRunner interface {
	Run(context.Context, ...string) ([]byte, error)
}

type dockerCLI struct{}

func (dockerCLI) Run(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "docker", args...).CombinedOutput()
}

type dockerAirflowBackend struct {
	runner airflowCommandRunner
}

func newDockerAirflowBackend() *dockerAirflowBackend {
	return &dockerAirflowBackend{runner: dockerCLI{}}
}

func airflowContainerName(resource string) string {
	sum := sha256.Sum256([]byte(config.GetProfile() + "\x00" + resource))
	return "minisky-airflow-" + hex.EncodeToString(sum[:6])
}

func (b *dockerAirflowBackend) Provision(parent context.Context, resource string) (endpoint string, resultErr error) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	name := airflowContainerName(resource)
	created := false
	defer func() {
		if resultErr != nil && created {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = b.runner.Run(cleanupCtx, "rm", "-f", name)
		}
	}()
	exists, err := b.requireOwned(ctx, name, resource)
	if err != nil {
		return "", err
	}
	if !exists {
		labels := airflowDockerLabels(resource)
		output, runErr := b.runner.Run(ctx, "run", "-d", "--name", name,
			"--label", "managed-by="+labels["managed-by"],
			"--label", "minisky.profile="+labels["minisky.profile"],
			"--label", "minisky.service="+labels["minisky.service"],
			"--label", "minisky.resource="+labels["minisky.resource"],
			"-p", "127.0.0.1::8080",
			airflowImage, "standalone")
		if runErr != nil {
			return "", fmt.Errorf("start pinned Airflow container: %w: %s", runErr, strings.TrimSpace(string(output)))
		}
		created = true
	}
	for {
		if _, commandErr := b.runner.Run(ctx, "exec", name, "airflow", "dags", "list"); commandErr == nil {
			break
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("wait for Airflow readiness: %w", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
	port, err := b.runner.Run(ctx, "port", name, "8080/tcp")
	if err != nil {
		return "", fmt.Errorf("discover Airflow port: %w", err)
	}
	address := strings.TrimSpace(string(port))
	if index := strings.LastIndex(address, ":"); index >= 0 {
		address = "127.0.0.1:" + address[index+1:]
	}
	return "http://" + address, nil
}

func (b *dockerAirflowBackend) Delete(parent context.Context, resource string) error {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	name := airflowContainerName(resource)
	exists, err := b.requireOwned(ctx, name, resource)
	if err != nil || !exists {
		return err
	}
	output, err := b.runner.Run(ctx, "rm", "-f", name)
	if err != nil {
		return fmt.Errorf("remove Airflow container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (b *dockerAirflowBackend) Reconcile(parent context.Context, resource string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	name := airflowContainerName(resource)
	exists, err := b.requireOwned(ctx, name, resource)
	if err != nil || !exists {
		return "", false, err
	}
	var lastErr error
	for {
		if output, commandErr := b.runner.Run(ctx, "exec", name, "airflow", "dags", "list"); commandErr != nil {
			lastErr = fmt.Errorf("check Airflow readiness: %w: %s", commandErr, strings.TrimSpace(string(output)))
		} else {
			port, portErr := b.runner.Run(ctx, "port", name, "8080/tcp")
			if portErr == nil {
				address := strings.TrimSpace(string(port))
				if index := strings.LastIndex(address, ":"); index >= 0 {
					address = "127.0.0.1:" + address[index+1:]
				}
				return "http://" + address, true, nil
			}
			lastErr = fmt.Errorf("discover Airflow port: %w", portErr)
		}
		select {
		case <-ctx.Done():
			return "", false, fmt.Errorf("wait for Airflow readiness: %v: %w", lastErr, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (b *dockerAirflowBackend) UploadDAG(parent context.Context, resource, dagID, source string) error {
	if !airflowIdentifier.MatchString(dagID) {
		return errors.New("invalid DAG identifier")
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	name := airflowContainerName(resource)
	exists, err := b.requireOwned(ctx, name, resource)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("Airflow environment is not running")
	}
	dir, err := os.MkdirTemp("", "minisky-airflow-dag-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, dagID+".py")
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		return err
	}
	output, err := b.runner.Run(ctx, "cp", path, name+":/opt/airflow/dags/"+dagID+".py")
	if err != nil {
		return fmt.Errorf("upload DAG: %w: %s", err, strings.TrimSpace(string(output)))
	}
	destination := "/opt/airflow/dags/" + dagID + ".py"
	if output, err = b.runner.Run(ctx, "exec", "--user", "root", name, "chmod", "0644", destination); err != nil {
		return fmt.Errorf("make DAG readable: %w: %s", err, strings.TrimSpace(string(output)))
	}
	for {
		output, err = b.runner.Run(ctx, "exec", name, "airflow", "dags", "list", "--output", "json")
		if err == nil {
			listed, parseErr := airflowDAGListed(output, dagID)
			if parseErr == nil && listed {
				return nil
			}
			if parseErr != nil {
				err = parseErr
			}
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return fmt.Errorf("wait for DAG %q readiness: %v: %w", dagID, err, ctx.Err())
			}
			return fmt.Errorf("wait for DAG %q readiness: %w", dagID, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func airflowDAGListed(output []byte, dagID string) (bool, error) {
	type dagSummary struct {
		DAGID string `json:"dag_id"`
	}
	decode := func(payload []byte) ([]dagSummary, error) {
		var dags []dagSummary
		if err := json.Unmarshal(payload, &dags); err != nil {
			return nil, err
		}
		return dags, nil
	}
	dags, err := decode(output)
	if err != nil {
		for _, line := range bytes.Split(output, []byte{'\n'}) {
			line = bytes.TrimSpace(line)
			if len(line) == 0 || line[0] != '[' {
				continue
			}
			if dags, err = decode(line); err == nil {
				break
			}
		}
	}
	if err != nil {
		return false, fmt.Errorf("decode Airflow DAG list: %w", err)
	}
	for _, dag := range dags {
		if dag.DAGID == dagID {
			return true, nil
		}
	}
	return false, nil
}

func (b *dockerAirflowBackend) TriggerDAG(parent context.Context, resource, dagID, runID string) error {
	if !airflowIdentifier.MatchString(dagID) || !airflowIdentifier.MatchString(runID) {
		return errors.New("invalid DAG or run identifier")
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	name := airflowContainerName(resource)
	exists, err := b.requireOwned(ctx, name, resource)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("Airflow environment is not running")
	}
	output, err := b.runner.Run(ctx, "exec", name, "airflow", "dags", "trigger", "--run-id", runID, dagID)
	if err != nil {
		return fmt.Errorf("trigger DAG: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (b *dockerAirflowBackend) requireOwned(ctx context.Context, name, resource string) (bool, error) {
	output, err := b.runner.Run(ctx, "inspect", name)
	if err != nil {
		if dockerInspectNotFound(output) {
			return false, nil
		}
		return false, fmt.Errorf("inspect Airflow container: %w: %s", err, strings.TrimSpace(string(output)))
	}
	var inspected []struct {
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
	}
	if err := json.Unmarshal(output, &inspected); err != nil || len(inspected) != 1 {
		return false, errors.New("decode Airflow container ownership")
	}
	labels := inspected[0].Config.Labels
	for key, value := range airflowDockerLabels(resource) {
		if labels[key] != value {
			return false, fmt.Errorf("container %q exists but is not owned by this environment", name)
		}
	}
	return true, nil
}

func airflowDockerLabels(resource string) map[string]string {
	labels := orchestrator.DockerOwnershipLabels()
	labels["minisky.service"] = airflowDockerService
	labels["minisky.resource"] = resource
	return labels
}

func dockerInspectNotFound(output []byte) bool {
	message := strings.ToLower(string(output))
	return strings.Contains(message, "no such object:") ||
		strings.Contains(message, "no such container:")
}
