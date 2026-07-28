package dataflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"minisky/pkg/state"
)

const dataflowStateEntry = "dataflow/metadata"

func init() {
	state.MustRegisterEntryValidator(dataflowStateEntry, state.StrictEntryValidator(validateDataflowMetadata))
}

type dataflowStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type dataflowMetadata struct {
	Jobs map[string]*Job `json:"jobs"`
}

func validateDataflowMetadata(_ state.EntryValidationContext, metadata *dataflowMetadata) error {
	for id, job := range metadata.Jobs {
		if job == nil {
			continue
		}
		switch job.CurrentState {
		case "JOB_STATE_PENDING", "JOB_STATE_RUNNING", "JOB_STATE_DONE", "JOB_STATE_FAILED",
			"JOB_STATE_CANCELLED", "JOB_STATE_DRAINED", "JOB_STATE_STOPPED":
		default:
			return fmt.Errorf("job %q has invalid currentState %q", id, job.CurrentState)
		}
	}
	return nil
}

// persistState deep-copies all jobs and writes to durable storage.
// Fail-closed: returns error on failure so callers can decide.
func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	snapshot := api.snapshotJobs()
	return api.stateStore.Save(dataflowStateEntry, dataflowMetadata{Jobs: snapshot})
}

// snapshotJobs returns a deep copy of all jobs for safe serialization.
func (api *API) snapshotJobs() map[string]*Job {
	api.mu.RLock()
	defer api.mu.RUnlock()
	out := make(map[string]*Job, len(api.jobs))
	for k, v := range api.jobs {
		out[k] = deepCopyJob(v)
	}
	return out
}

// loadState rehydrates jobs from durable storage.
func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta dataflowMetadata
	if err := api.stateStore.Load(dataflowStateEntry, &meta); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return nil
		}
		return err
	}
	if meta.Jobs != nil {
		api.jobs = make(map[string]*Job, len(meta.Jobs))
		for id, job := range meta.Jobs {
			restored := deepCopyJob(job)
			reconcileRestoredJob(restored)
			api.jobs[id] = restored
		}
	}
	return nil
}

func reconcileRestoredJob(job *Job) {
	if job != nil && (job.CurrentState == "JOB_STATE_PENDING" || job.CurrentState == "JOB_STATE_RUNNING") {
		stopJob(job)
	}
}

func stopJob(job *Job) {
	if job == nil {
		return
	}
	job.CurrentState = "JOB_STATE_STOPPED"
	job.CurrentStateTime = time.Now().UTC().Format(time.RFC3339Nano)
}

func deepCopyJob(j *Job) *Job {
	raw, _ := json.Marshal(j)
	var clone Job
	_ = json.Unmarshal(raw, &clone)
	return &clone
}
