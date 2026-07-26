package gke

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestKindBackendUsesFullProfileOnlyWhenDependenciesExist(t *testing.T) {
	binDir := t.TempDir()
	writeRuntimeExecutable(t, filepath.Join(binDir, "kind"))
	writeRuntimeExecutable(t, filepath.Join(binDir, "docker"))
	t.Setenv("PATH", binDir)
	t.Setenv("MINISKY_RUNTIME_PROFILE", "full")
	t.Setenv("MINISKY_GKE_BACKEND", "")

	backend := NewKindBackend()
	if state := backend.Status(); !state.Enabled || state.Backend != "kind" || state.Source != "profile" {
		t.Fatalf("backend state = %#v, want profile-selected Kind", state)
	}
}

func TestKindBackendDoesNotBecomeDefaultInSimulationProfile(t *testing.T) {
	binDir := t.TempDir()
	writeRuntimeExecutable(t, filepath.Join(binDir, "kind"))
	writeRuntimeExecutable(t, filepath.Join(binDir, "docker"))
	t.Setenv("PATH", binDir)
	t.Setenv("MINISKY_RUNTIME_PROFILE", "simulation")
	t.Setenv("MINISKY_GKE_BACKEND", "")

	backend := NewKindBackend()
	if state := backend.Status(); state.Enabled || state.Backend != "simulation" {
		t.Fatalf("backend state = %#v, want simulation without Kind cluster provisioning", state)
	}
}

func TestKindBackendFallsBackWithDependencyDiagnostic(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MINISKY_RUNTIME_PROFILE", "full")
	t.Setenv("MINISKY_GKE_BACKEND", "")

	backend := NewKindBackend()
	if state := backend.Status(); state.Enabled || state.Diagnostic == "" {
		t.Fatalf("backend state = %#v, want diagnosed simulation fallback", state)
	}
}

func TestDeleteClusterRemovesTrackedKubeconfig(t *testing.T) {
	binDir := t.TempDir()
	t.Setenv("PATH", binDir)
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "kubeconfig-restart")
	identity := ClusterIdentity{Profile: "kubeconfig-restart", Project: "demo", Zone: "zone", Cluster: "cluster"}
	path := kindKubeconfigPath(identity)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("credentials"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted := &KindBackend{enabled: true}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := kubeconfigOwnershipFromFileInfo(identity, info)
	if err != nil {
		t.Fatal(err)
	}
	deleted := filepath.Join(binDir, "deleted")
	script := "#!/bin/sh\nif [ \"$1\" = get ]; then if [ ! -f " + deleted +
		" ]; then echo " + ownership.BackendName + "; fi; else printf deleted > " + deleted + "; fi\nexit 0\n"
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	restarted.RestoreKubeconfigOwnership(identity, ownership)
	if err := restarted.DeleteClusterContext(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("kubeconfig still exists: %v", err)
	}
}

func TestKindSameNameLifecycleIsSerialized(t *testing.T) {
	backend := &KindBackend{}
	unlock := backend.lockName("cluster")
	acquired := make(chan struct{})
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		release := backend.lockName("cluster")
		close(acquired)
		release()
	}()
	select {
	case <-acquired:
		t.Fatal("same-name lifecycle lock was not serialized")
	case <-time.After(25 * time.Millisecond):
	}
	unlock()
	group.Wait()
}

func TestDeleteFailureRetainsDeterministicKubeconfig(t *testing.T) {
	binDir := t.TempDir()
	t.Setenv("PATH", binDir)
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "kubeconfig-failure")
	identity := ClusterIdentity{Profile: "kubeconfig-failure", Project: "demo", Zone: "zone", Cluster: "cluster"}
	kindName, _ := kindBackendName(identity)
	if err := os.WriteFile(filepath.Join(binDir, "kind"),
		[]byte("#!/bin/sh\nif [ \"$1\" = get ]; then echo \""+kindName+"\"; exit 0; fi\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	path := kindKubeconfigPath(identity)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("credentials"), 0o600); err != nil {
		t.Fatal(err)
	}
	backend := &KindBackend{enabled: true}
	if err := backend.DeleteClusterContext(context.Background(), identity); err == nil {
		t.Fatal("delete unexpectedly succeeded")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("kubeconfig tracking removed after failed deletion: %v", err)
	}
}

func TestKindBackendNamesSeparateCanonicalIdentityAndTruncation(t *testing.T) {
	base := ClusterIdentity{Profile: "profile", Project: "project-a", Zone: "zone-a", Cluster: strings.Repeat("cluster", 20)}
	names := map[string]bool{}
	for _, identity := range []ClusterIdentity{
		base,
		{Profile: "profile", Project: "project-b", Zone: "zone-a", Cluster: base.Cluster},
		{Profile: "profile", Project: "project-a", Zone: "zone-b", Cluster: base.Cluster},
		{Profile: "other", Project: "project-a", Zone: "zone-a", Cluster: base.Cluster},
		{Profile: "profile", Project: "project-a", Zone: "zone-a", Cluster: base.Cluster + "x"},
	} {
		name, err := kindBackendName(identity)
		if err != nil {
			t.Fatal(err)
		}
		if len(name) > 63 || names[name] {
			t.Fatalf("unsafe or colliding Kind name %q", name)
		}
		names[name] = true
	}
}

func TestLegacyCleanupRefusesUnverifiableOwnership(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "legacy")
	identity := ClusterIdentity{Profile: "legacy", Project: "demo", Zone: "zone", Cluster: "cluster"}
	legacyName := sanitizeKindName(identity.Cluster)
	binDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(binDir, "kind"),
		[]byte("#!/bin/sh\nif [ \"$1\" = get ]; then echo \""+legacyName+"\"; fi\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	path := legacyKindKubeconfigPath(legacyName)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	backend := &KindBackend{enabled: true}
	if err := backend.DeleteClusterContext(context.Background(), identity); err == nil {
		t.Fatal("legacy cluster deleted without ownership claim")
	}
	other := identity
	other.Project = "other"
	if err := backend.DeleteClusterContext(context.Background(), other); err == nil {
		t.Fatal("legacy cluster deleted for another identity")
	}
	if err := backend.DeleteClusterContext(context.Background(), identity); err == nil {
		t.Fatal("arbitrary caller claimed legacy cluster ownership")
	}
}

func TestReadKubeconfigRejectsTraversalAndSymlink(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "safe-read")
	backend := &KindBackend{}
	if _, err := backend.ReadKubeconfig(ClusterIdentity{
		Profile: "safe-read", Project: "demo", Zone: "zone", Cluster: "../escape",
	}); err == nil {
		t.Fatal("traversal identity was accepted")
	}
	identity := ClusterIdentity{Profile: "safe-read", Project: "demo", Zone: "zone", Cluster: "cluster"}
	path := kindKubeconfigPath(identity)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.ReadKubeconfig(identity); err == nil {
		t.Fatal("symlink kubeconfig was accepted")
	}
}

func writeRuntimeExecutable(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatal(err)
	}
}
