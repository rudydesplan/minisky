package gke

import (
	"errors"
	"fmt"
	"sync"
)

const (
	maxKubeconfigEntries     = 64
	maxKubeconfigIntentSlots = maxKubeconfigEntries * 2
	maxKubeconfigTombstones  = maxKubeconfigEntries
)

var (
	errKubeconfigEntryLimit     = errors.New("kubeconfig entry limit reached")
	errKubeconfigTombstoneLimit = errKubeconfigEntryLimit
	kubeconfigLifecycleMu       sync.Mutex
	testWriteKubeconfigIntent   func(kubeconfigIntentPhase) error
)

type kubeconfigIntentPhase string

const (
	intentPrepared       kubeconfigIntentPhase = "PREPARED"
	intentCreateStarted  kubeconfigIntentPhase = "CREATE_STARTED"
	intentBackendCreated kubeconfigIntentPhase = "BACKEND_CREATED"
	intentCleanupPending kubeconfigIntentPhase = "CLEANUP_PENDING"
	intentCommitted      kubeconfigIntentPhase = "COMMITTED"
	intentDeletePending  kubeconfigIntentPhase = "DELETE_PENDING"
	intentDeleteCleaned  kubeconfigIntentPhase = "DELETE_CLEANED"
	intentTerminal       kubeconfigIntentPhase = "TERMINAL"
)

type kubeconfigIntent struct {
	Generation          uint64                         `json:"generation"`
	Phase               kubeconfigIntentPhase          `json:"phase"`
	Ownership           *kubeconfigOwnership           `json:"ownership"`
	UnmatchedQuarantine *unmatchedKubeconfigQuarantine `json:"unmatchedQuarantine,omitempty"`
	Error               string                         `json:"error,omitempty"`
}

// unmatchedKubeconfigQuarantine is stat-only evidence. It deliberately stores
// no bytes or digest derived from content that lacked trusted ownership.
type unmatchedKubeconfigQuarantine struct {
	Entry     string `json:"entry"`
	Device    uint64 `json:"device"`
	Inode     uint64 `json:"inode"`
	Size      int64  `json:"size"`
	LinkCount uint64 `json:"linkCount"`
}

type unmatchedKubeconfigQuarantineError struct {
	Evidence *unmatchedKubeconfigQuarantine
	Reason   string
}

func (err *unmatchedKubeconfigQuarantineError) Error() string {
	if err == nil {
		return "manual recovery required for unmatched kubeconfig quarantine"
	}
	if err.Evidence == nil || err.Evidence.Entry == "" {
		return "manual recovery required: kubeconfig ownership is unresolved: " + err.Reason
	}
	return fmt.Sprintf(
		"manual recovery required for unmatched kubeconfig quarantine %s "+
			"(device=%d inode=%d size=%d links=%d): %s",
		err.Evidence.Entry, err.Evidence.Device, err.Evidence.Inode,
		err.Evidence.Size, err.Evidence.LinkCount, err.Reason)
}
