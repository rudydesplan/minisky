package storagetransfer

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"minisky/pkg/state"
)

const storagetransferStateEntry = "storagetransfer/metadata"

func init() {
	state.MustRegisterEntryValidator(storagetransferStateEntry, state.StrictEntryValidator(validateStorageTransferMetadata))
}

type storagetransferStateStore interface {
	Load(string, any) error
	Save(string, any) error
}

type storagetransferMetadata struct {
	Jobs         map[string]*TransferJob       `json:"jobs"`
	SeqNum       int                           `json:"seqNum"`
	Operations   map[string]*TransferOperation `json:"operations,omitempty"`
	OperationSeq int                           `json:"operationSeq,omitempty"`
}

func validateStorageTransferMetadata(_ state.EntryValidationContext, metadata *storagetransferMetadata) error {
	if metadata.SeqNum < 0 {
		return fmt.Errorf("seqNum must not be negative")
	}
	if metadata.OperationSeq < 0 {
		return fmt.Errorf("operationSeq must not be negative")
	}
	for name := range metadata.Jobs {
		id, err := strconv.Atoi(strings.TrimPrefix(name, "transferJobs/"))
		if err == nil && id > metadata.SeqNum {
			return fmt.Errorf("seqNum %d collides with job %q", metadata.SeqNum, name)
		}
	}
	return nil
}

func (api *API) persistState() error {
	if api.stateStore == nil {
		return nil
	}
	api.persistMu.Lock()
	defer api.persistMu.Unlock()

	api.mu.RLock()
	snapshot := make(map[string]*TransferJob, len(api.jobs))
	for k, v := range api.jobs {
		snapshot[k] = cloneJob(v)
	}
	seq := api.seqNum
	operations := make(map[string]*TransferOperation, len(api.operations))
	for name, operation := range api.operations {
		operations[name] = cloneTransferOperation(operation)
	}
	operationSeq := api.operationSeq
	api.mu.RUnlock()

	return api.stateStore.Save(storagetransferStateEntry, storagetransferMetadata{
		Jobs:         snapshot,
		SeqNum:       seq,
		Operations:   operations,
		OperationSeq: operationSeq,
	})
}

// compensateState writes the restored in-memory snapshot after a Save error.
// Save errors are treated as ambiguous because the replacement may already
// have committed. If compensation is also ambiguous, readback becomes the
// visible source of truth and the operation manager remains degraded.
func (api *API) compensateState(cause error) {
	api.opMgr.MarkPersistenceFailure(cause)
	if err := api.persistState(); err == nil {
		return
	} else {
		api.opMgr.MarkPersistenceFailure(fmt.Errorf("storage transfer compensation save: %w", err))
	}
	if api.stateStore == nil {
		return
	}
	var durable storagetransferMetadata
	if err := api.stateStore.Load(storagetransferStateEntry, &durable); err != nil {
		api.opMgr.MarkPersistenceFailure(fmt.Errorf("storage transfer compensation readback: %w", err))
		return
	}
	api.mu.Lock()
	api.jobs = durable.Jobs
	if api.jobs == nil {
		api.jobs = make(map[string]*TransferJob)
	}
	api.operations = durable.Operations
	if api.operations == nil {
		api.operations = make(map[string]*TransferOperation)
	}
	api.seqNum = durable.SeqNum
	api.operationSeq = durable.OperationSeq
	api.mu.Unlock()
}

func (api *API) loadState() error {
	if api.stateStore == nil {
		return nil
	}
	var meta storagetransferMetadata
	if err := api.stateStore.Load(storagetransferStateEntry, &meta); err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	if meta.Jobs != nil {
		api.jobs = meta.Jobs
	}
	api.seqNum = meta.SeqNum
	if meta.Operations != nil {
		api.operations = meta.Operations
	}
	api.operationSeq = meta.OperationSeq
	return nil
}

func isNotFound(err error) bool {
	return errors.Is(err, state.ErrNotFound)
}

func cloneJob(j *TransferJob) *TransferJob {
	raw, _ := json.Marshal(j)
	var c TransferJob
	_ = json.Unmarshal(raw, &c)
	return &c
}
