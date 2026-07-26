package gke

import (
	"errors"
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
	Generation uint64                `json:"generation"`
	Phase      kubeconfigIntentPhase `json:"phase"`
	Ownership  *kubeconfigOwnership  `json:"ownership"`
	Error      string                `json:"error,omitempty"`
}
