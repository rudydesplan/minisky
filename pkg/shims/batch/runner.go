package batch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

const maxContainerOutput = 64 << 10

type containerOwnership struct {
	ContainerName string            `json:"containerName"`
	Labels        map[string]string `json:"labels"`
}

type containerWorkload struct {
	Ownership  containerOwnership `json:"ownership"`
	ImageURI   string             `json:"imageUri"`
	Entrypoint string             `json:"entrypoint,omitempty"`
	Commands   []string           `json:"commands,omitempty"`
}

type containerResult struct {
	ExitCode int
	Output   string
}

type containerRunner interface {
	Check(context.Context) error
	Run(context.Context, containerWorkload) (containerResult, error)
	Cleanup(context.Context, containerOwnership) error
}

type dockerCLIRunner struct{}

func (dockerCLIRunner) Check(ctx context.Context) error {
	output, err := exec.CommandContext(ctx, "docker", "info", "--format", "{{.ServerVersion}}").CombinedOutput()
	if err != nil {
		return fmt.Errorf("Docker backend unavailable: %w (%s)", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (dockerCLIRunner) Run(ctx context.Context, workload containerWorkload) (containerResult, error) {
	args := []string{"create", "--name", workload.Ownership.ContainerName}
	for _, key := range sortedLabelKeys(workload.Ownership.Labels) {
		args = append(args, "--label", key+"="+workload.Ownership.Labels[key])
	}
	if workload.Entrypoint != "" {
		args = append(args, "--entrypoint", workload.Entrypoint)
	}
	args = append(args, workload.ImageURI)
	args = append(args, workload.Commands...)

	createOutput, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return containerResult{Output: boundedString(createOutput)},
			fmt.Errorf("create owned Batch container: %w", err)
	}

	var output cappedBuffer
	command := exec.CommandContext(ctx, "docker", "start", "--attach", workload.Ownership.ContainerName)
	command.Stdout = &output
	command.Stderr = &output
	err = command.Run()
	result := containerResult{Output: output.String()}
	if err == nil {
		return result, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
		return result, nil
	}
	return result, fmt.Errorf("run owned Batch container: %w", err)
}

func (dockerCLIRunner) Cleanup(ctx context.Context, ownership containerOwnership) error {
	inspectOutput, err := exec.CommandContext(ctx, "docker", "inspect",
		"--format", "{{json .Config.Labels}}", ownership.ContainerName).CombinedOutput()
	if err != nil {
		if strings.Contains(string(inspectOutput), "No such object") {
			return nil
		}
		return fmt.Errorf("inspect owned Batch container: %w (%s)", err, strings.TrimSpace(string(inspectOutput)))
	}
	var labels map[string]string
	if err := json.Unmarshal(bytes.TrimSpace(inspectOutput), &labels); err != nil {
		return fmt.Errorf("decode Batch container ownership: %w", err)
	}
	for key, value := range ownership.Labels {
		if labels[key] != value {
			return fmt.Errorf("refusing to remove container %q: ownership label %q does not match",
				ownership.ContainerName, key)
		}
	}
	removeOutput, err := exec.CommandContext(ctx, "docker", "rm", "--force", ownership.ContainerName).CombinedOutput()
	if err != nil {
		return fmt.Errorf("remove owned Batch container: %w (%s)", err, strings.TrimSpace(string(removeOutput)))
	}
	return nil
}

func newContainerOwnership(profile, jobName, uid string) containerOwnership {
	sum := sha256.Sum256([]byte(profile + "\x00" + jobName + "\x00" + uid))
	return containerOwnership{
		ContainerName: "minisky-batch-" + hex.EncodeToString(sum[:12]),
		Labels: map[string]string{
			"minisky.owner":   "true",
			"minisky.service": "batch",
			"minisky.profile": profile,
			"minisky.job":     jobName,
			"minisky.uid":     uid,
		},
	}
}

func sortedLabelKeys(labels map[string]string) []string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func boundedString(value []byte) string {
	if len(value) > maxContainerOutput {
		value = value[:maxContainerOutput]
	}
	return string(value)
}

type cappedBuffer struct {
	data []byte
}

func (b *cappedBuffer) Write(value []byte) (int, error) {
	original := len(value)
	remaining := maxContainerOutput - len(b.data)
	if remaining > 0 {
		if len(value) > remaining {
			value = value[:remaining]
		}
		b.data = append(b.data, value...)
	}
	return original, nil
}

func (b *cappedBuffer) String() string {
	return string(b.data)
}
