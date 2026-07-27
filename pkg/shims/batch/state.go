package batch

import (
	"encoding/json"
	"errors"
	"fmt"

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
	Jobs map[string]*Job `json:"jobs"`
}

func validateBatchMetadata(_ state.EntryValidationContext, metadata *batchMetadata) error {
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
	return nil
}

// persistState deep-copies jobs and writes to durable storage.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	snapshot := api.snapshotJobs()
	return api.stateStore.Save(batchStateEntry, batchMetadata{Jobs: snapshot})
}

// snapshotJobs returns a deep copy of all jobs for safe serialization.
func (api *API) snapshotJobs() map[string]*Job {
	api.mu.RLock()
	defer api.mu.RUnlock()
	snapshot := make(map[string]*Job, len(api.jobs))
	for k, v := range api.jobs {
		snapshot[k] = deepCopyJob(v)
	}
	return snapshot
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
	if meta.Jobs != nil {
		api.jobs = meta.Jobs
	}
	return nil
}

// deepCopyJob returns a fully independent copy of a Job.
func deepCopyJob(j *Job) *Job {
	raw, _ := json.Marshal(j)
	var clone Job
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
