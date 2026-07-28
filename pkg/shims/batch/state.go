package batch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"minisky/pkg/state"
)

const batchStateEntry = "batch/metadata"

func init() {
	state.MustRegisterEntryValidator(batchStateEntry, state.StrictEntryValidator(validateBatchMetadata))
}

type batchStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type batchMetadata struct {
	Jobs     map[string]*Job                `json:"jobs"`
	Runtimes map[string]*batchRuntimeIntent `json:"runtimes,omitempty"`
}

type batchRuntimeIntent struct {
	Ownership containerOwnership `json:"ownership"`
	Workload  containerWorkload  `json:"workload"`
}

func validateBatchMetadata(context state.EntryValidationContext, metadata *batchMetadata) error {
	if context.Store != nil && len(metadata.Runtimes) != 0 {
		return fmt.Errorf("Docker runtime cleanup intents cannot be imported")
	}
	for name, job := range metadata.Jobs {
		if job == nil {
			continue
		}
		if job.Status == nil {
			return fmt.Errorf("job %q has nil status", name)
		}
		switch job.Status.State {
		case "QUEUED", "SCHEDULED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED", "DELETION_IN_PROGRESS":
		default:
			return fmt.Errorf("job %q has invalid state %q", name, job.Status.State)
		}
	}
	for name, runtime := range metadata.Runtimes {
		if metadata.Jobs[name] == nil || runtime == nil {
			return fmt.Errorf("runtime %q references missing job", name)
		}
		if runtime.Ownership.ContainerName == "" ||
			runtime.Ownership.Labels["minisky.owner"] != "true" ||
			runtime.Ownership.Labels["minisky.service"] != "batch" ||
			runtime.Ownership.Labels["minisky.job"] != name {
			return fmt.Errorf("runtime %q has invalid ownership", name)
		}
		if runtime.Workload.Ownership.ContainerName != "" &&
			runtime.Workload.Ownership.ContainerName != runtime.Ownership.ContainerName {
			return fmt.Errorf("runtime %q workload ownership does not match", name)
		}
	}
	return nil
}

// persistState deep-copies jobs and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	jobs, runtimes := api.snapshotState()
	return api.stateStore.Save(batchStateEntry, batchMetadata{Jobs: jobs, Runtimes: runtimes})
}

// snapshotState returns deep copies for safe serialization.
func (api *API) snapshotState() (map[string]*Job, map[string]*batchRuntimeIntent) {
	api.mu.RLock()
	defer api.mu.RUnlock()
	jobs := make(map[string]*Job, len(api.jobs))
	for k, v := range api.jobs {
		jobs[k] = deepCopyJob(v)
	}
	runtimes := make(map[string]*batchRuntimeIntent, len(api.runtimes))
	for k, v := range api.runtimes {
		if v == nil {
			continue
		}
		clone := *v
		clone.Ownership.Labels = cloneLabels(v.Ownership.Labels)
		clone.Workload.Commands = append([]string(nil), v.Workload.Commands...)
		clone.Workload.Ownership.Labels = cloneLabels(v.Workload.Ownership.Labels)
		runtimes[k] = &clone
	}
	return jobs, runtimes
}

// loadState rehydrates jobs from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta batchMetadata
	if err := api.stateStore.Load(batchStateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if err := validateBatchMetadata(state.EntryValidationContext{}, &meta); err != nil {
		return fmt.Errorf("invalid Batch state: %w", err)
	}
	if meta.Jobs != nil {
		api.jobs = meta.Jobs
	}
	if meta.Runtimes != nil {
		api.runtimes = meta.Runtimes
	}
	if api.runtimes == nil {
		api.runtimes = make(map[string]*batchRuntimeIntent)
	}
	changed := false
	for name, runtime := range api.runtimes {
		if runtime == nil {
			continue
		}
		if api.runner == nil {
			return fmt.Errorf("reconcile interrupted Batch job %q: Docker backend is unavailable", name)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := api.runner.Cleanup(ctx, runtime.Ownership)
		cancel()
		if err != nil {
			return fmt.Errorf("reconcile interrupted Batch job %q: %w", name, err)
		}
		if job := api.jobs[name]; job != nil && job.Status != nil &&
			(job.Status.State == "QUEUED" || job.Status.State == "SCHEDULED" || job.Status.State == "RUNNING") {
			job.Status.State = "FAILED"
			job.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
			job.Status.StatusEvents = append(job.Status.StatusEvents,
				newStatusEvent("FAILED", -1, "", fmt.Errorf("job interrupted by MiniSky restart")))
		}
		delete(api.runtimes, name)
		changed = true
	}
	if changed {
		return api.persistState()
	}
	return nil
}

func cloneLabels(labels map[string]string) map[string]string {
	clone := make(map[string]string, len(labels))
	for key, value := range labels {
		clone[key] = value
	}
	return clone
}

// deepCopyJob returns a fully independent copy of a Job.
func deepCopyJob(j *Job) *Job {
	raw, _ := json.Marshal(j)
	var clone Job
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
