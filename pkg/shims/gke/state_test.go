package gke

import (
	"testing"

	"minisky/pkg/state"
)

func TestClusterMetadataRehydratesWithoutBackendRecreation(t *testing.T) {
	t.Parallel()
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	api.clusters["demo:us-central1-c:test"] = &Cluster{Name: "test", Location: "us-central1-c", Status: "RUNNING"}
	if err := api.persistMetadata(); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.clusters["demo:us-central1-c:test"]; got == nil || got.Name != "test" {
		t.Fatalf("restored cluster = %#v", got)
	}
	if _, ok := restarted.backend.pendingClusters.Load("test"); ok {
		t.Fatal("rehydration must not start backend reconciliation workers")
	}
}
