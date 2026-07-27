package gke

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"minisky/pkg/state"
)

func TestClusterMetadataRehydratesWithoutBackendRecreation(t *testing.T) {
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
	if got := restarted.clusters["demo:us-central1-c:test"]; got == nil || got.Name != "test" ||
		got.Status != "ERROR" || got.StatusMessage == "" || got.Endpoint != "" {
		t.Fatalf("restored cluster = %#v", got)
	}
	if _, ok := restarted.GetBackend().pendingClusters.Load("test"); ok {
		t.Fatal("rehydration must not start backend reconciliation workers")
	}
}

func TestImportRejectsMalformedKubeconfigOwnershipBeforeReplacement(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "import")
	key := clusterKey("demo", "us-central1-c", "cluster")
	validOwnership := &kubeconfigOwnership{
		Profile: "import", Project: "demo", Zone: "us-central1-c", Cluster: "cluster",
		BackendName: "minisky-owned-" + strings.Repeat("a", 32),
		SHA256:      strings.Repeat("b", 64), Device: 1, Inode: 2,
	}
	valid := gkeMetadata{
		Backend: "kind",
		Clusters: map[string]*Cluster{
			key: {Name: "cluster", Location: "us-central1-c"},
		},
		Ownerships: map[string]*kubeconfigOwnership{key: validOwnership},
	}
	checksum, err := kubeconfigOwnershipChecksum(valid.Ownerships)
	if err != nil {
		t.Fatal(err)
	}
	valid.OwnershipChecksum = checksum

	tests := []struct {
		name   string
		mutate func(*gkeMetadata)
	}{
		{"checksum", func(metadata *gkeMetadata) { metadata.OwnershipChecksum = strings.Repeat("0", 64) }},
		{"identity slot", func(metadata *gkeMetadata) {
			metadata.Ownerships["demo:us-central1-c:other"] = metadata.Ownerships[key]
			delete(metadata.Ownerships, key)
			metadata.OwnershipChecksum, _ = kubeconfigOwnershipChecksum(metadata.Ownerships)
		}},
		{"profile identity", func(metadata *gkeMetadata) {
			metadata.Ownerships[key].Profile = "other"
			metadata.OwnershipChecksum, _ = kubeconfigOwnershipChecksum(metadata.Ownerships)
		}},
		{"backend nonce", func(metadata *gkeMetadata) {
			metadata.Ownerships[key].BackendName = "kind-cluster"
			metadata.OwnershipChecksum, _ = kubeconfigOwnershipChecksum(metadata.Ownerships)
		}},
		{"content digest", func(metadata *gkeMetadata) {
			metadata.Ownerships[key].SHA256 = "not-a-digest"
			metadata.OwnershipChecksum, _ = kubeconfigOwnershipChecksum(metadata.Ownerships)
		}},
		{"device", func(metadata *gkeMetadata) {
			metadata.Ownerships[key].Device = 0
			metadata.OwnershipChecksum, _ = kubeconfigOwnershipChecksum(metadata.Ownerships)
		}},
		{"inode", func(metadata *gkeMetadata) {
			metadata.Ownerships[key].Inode = 0
			metadata.OwnershipChecksum, _ = kubeconfigOwnershipChecksum(metadata.Ownerships)
		}},
		{"missing cluster", func(metadata *gkeMetadata) { delete(metadata.Clusters, key) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := state.New(t.TempDir(), "import")
			if err != nil {
				t.Fatal(err)
			}
			active := gkeMetadata{Backend: "simulation", Clusters: map[string]*Cluster{
				"active:us-central1-c:cluster": {Name: "cluster", Location: "us-central1-c"},
			}}
			if err := store.Save(gkeStateEntry, active); err != nil {
				t.Fatal(err)
			}
			candidate := cloneGKEMetadataForImportTest(t, valid)
			test.mutate(&candidate)
			if err := importGKEMetadataForTest(store, candidate); err == nil {
				t.Fatal("malformed ownership snapshot was accepted")
			}
			var preserved gkeMetadata
			if err := store.Load(gkeStateEntry, &preserved); err != nil {
				t.Fatal(err)
			}
			if preserved.Clusters["active:us-central1-c:cluster"] == nil {
				t.Fatal("failed import replaced active state")
			}
		})
	}
}

func TestImportAcceptsBackwardCompatibleGKESnapshotWithoutOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "legacy")
	store, err := state.New(t.TempDir(), "legacy")
	if err != nil {
		t.Fatal(err)
	}
	legacy := gkeMetadata{Clusters: map[string]*Cluster{
		clusterKey("demo", "us-central1-c", "cluster"): {
			Name: "cluster", Location: "us-central1-c", Status: "RUNNING",
		},
	}}
	if err := importGKEMetadataForTest(store, legacy); err != nil {
		t.Fatalf("backward-compatible import failed: %v", err)
	}
	var imported gkeMetadata
	if err := store.Load(gkeStateEntry, &imported); err != nil {
		t.Fatal(err)
	}
	if imported.Clusters[clusterKey("demo", "us-central1-c", "cluster")] == nil {
		t.Fatal("legacy cluster metadata was not imported")
	}
}

func TestImportValidatesGKEOwnershipAgainstDestinationProfile(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ambient")
	store, err := state.New(t.TempDir(), "destination")
	if err != nil {
		t.Fatal(err)
	}
	key := clusterKey("demo", "us-central1-c", "cluster")
	ownership := &kubeconfigOwnership{
		Profile: "destination", Project: "demo", Zone: "us-central1-c", Cluster: "cluster",
		BackendName: "minisky-owned-" + strings.Repeat("a", 32),
		SHA256:      strings.Repeat("b", 64), Device: 1, Inode: 2,
	}
	valid := gkeMetadata{
		Backend: "kind",
		Clusters: map[string]*Cluster{
			key: {Name: "cluster", Location: "us-central1-c"},
		},
		Ownerships: map[string]*kubeconfigOwnership{key: ownership},
	}
	valid.OwnershipChecksum, err = kubeconfigOwnershipChecksum(valid.Ownerships)
	if err != nil {
		t.Fatal(err)
	}
	if err := importGKEMetadataForTest(store, valid); err != nil {
		t.Fatalf("valid destination ownership rejected: %v", err)
	}

	wrong := cloneGKEMetadataForImportTest(t, valid)
	wrong.Ownerships[key].Profile = "ambient"
	wrong.OwnershipChecksum, _ = kubeconfigOwnershipChecksum(wrong.Ownerships)
	if err := importGKEMetadataForTest(store, wrong); err == nil {
		t.Fatal("ambient-profile ownership was accepted for destination")
	}
	var preserved gkeMetadata
	if err := store.Load(gkeStateEntry, &preserved); err != nil {
		t.Fatal(err)
	}
	if got := preserved.Ownerships[key]; got == nil || got.Profile != "destination" {
		t.Fatalf("rejected import replaced destination state: %#v", got)
	}
}

func importGKEMetadataForTest(store *state.Store, metadata gkeMetadata) error {
	payload, _ := json.Marshal(metadata)
	snapshot, _ := json.Marshal(state.Snapshot{
		Format: state.SnapshotFormat, Version: state.Version,
		Entries: map[string]json.RawMessage{gkeStateEntry: payload},
	})
	return store.Import(bytes.NewReader(snapshot))
}

func cloneGKEMetadataForImportTest(t *testing.T, metadata gkeMetadata) gkeMetadata {
	t.Helper()
	payload, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	var clone gkeMetadata
	if err := json.Unmarshal(payload, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}
