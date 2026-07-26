//go:build unix

package gke

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/state"
)

func TestSecurePrepareDoesNotOverwriteExistingFinalFile(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "cluster.kubeconfig")
	if err := os.WriteFile(final, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := securePrepareKubeconfig(final); err == nil {
		t.Fatal("overwrote existing final kubeconfig")
	}
	data, err := os.ReadFile(final)
	if err != nil || string(data) != "existing" {
		t.Fatalf("existing data=%q err=%v", data, err)
	}
}

func TestSecurePublishFailureRemovesFinalCredential(t *testing.T) {
	for _, test := range []struct {
		name   string
		inject func(*secureKubeconfigTarget)
	}{
		{name: "file fsync", inject: func(target *secureKubeconfigTarget) {
			target.testFileSync = func() error { return errors.New("file fsync failed") }
		}},
		{name: "directory fsync", inject: func(target *secureKubeconfigTarget) {
			target.testDirSync = func() error { return errors.New("directory fsync failed") }
		}},
		{name: "file close", inject: func(target *secureKubeconfigTarget) {
			target.testFileClose = func() error { return errors.New("file close failed") }
		}},
		{name: "directory close", inject: func(target *secureKubeconfigTarget) {
			target.testDirClose = func() error { return errors.New("directory close failed") }
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			final := filepath.Join(root, "cluster.kubeconfig")
			target, err := securePrepareKubeconfig(final)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := target.file.Write([]byte("credential")); err != nil {
				t.Fatal(err)
			}
			test.inject(target)
			if err := securePublishKubeconfig(target, final); err == nil {
				t.Fatal("injected publication failure succeeded")
			}
			if _, err := os.Stat(final); !os.IsNotExist(err) {
				t.Fatalf("credential remains after failure: %v", err)
			}
		})
	}
}

func TestSecureDiscardRemovesCredentialAfterKindFailure(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "cluster.kubeconfig")
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("credential")); err != nil {
		t.Fatal(err)
	}
	if err := secureDiscardKubeconfig(target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("credential remains after Kind failure cleanup: %v", err)
	}
	assertZeroizedQuarantine(t, root, ".quarantine-*")
}

func TestSecurePublishRejectsPreReturnSwap(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "cluster.kubeconfig")
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("credential")); err != nil {
		t.Fatal(err)
	}
	target.testBeforeFinalCheck = func() error {
		if err := os.Remove(final); err != nil {
			return err
		}
		return os.WriteFile(final, []byte("attacker"), 0o600)
	}
	if err := securePublishKubeconfig(target, final); err == nil {
		t.Fatal("accepted pre-return inode swap")
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("replacement remained at final path: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(root, ".quarantine-*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("quarantine matches=%v err=%v", matches, err)
	}
	data, err := os.ReadFile(matches[0])
	if err != nil || string(data) != "attacker" {
		t.Fatalf("quarantined replacement=%q err=%v", data, err)
	}
}

func TestSecurePublishRejectsPostDigestContentMutation(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "cluster.kubeconfig")
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("credential")); err != nil {
		t.Fatal(err)
	}
	identity := ClusterIdentity{
		Profile: "publish", Project: "demo", Zone: "zone", Cluster: "cluster",
	}
	ownership, err := kubeconfigOwnershipFromTarget(identity, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizeKubeconfigOwnership(target, ownership); err != nil {
		t.Fatal(err)
	}
	target.testBeforeFinalCheck = func() error {
		return os.WriteFile(final, []byte("attacker"), 0o600)
	}
	if err := securePublishKubeconfig(target, final); err == nil {
		t.Fatal("published content changed after digest sealing")
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("mutated final remains: %v", err)
	}
	assertZeroizedQuarantine(t, root, ".quarantine-*")
}

func TestSecureCleanupQuarantinesReplacementAndPreservesNewFinal(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "cluster.kubeconfig")
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("credential")); err != nil {
		t.Fatal(err)
	}
	ownership, err := kubeconfigOwnershipFromTarget(ClusterIdentity{
		Profile: "cleanup", Project: "demo", Zone: "zone", Cluster: "cluster",
	}, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizeKubeconfigOwnership(target, ownership); err != nil {
		t.Fatal(err)
	}
	target.testFileSync = func() error { return errors.New("force cleanup") }
	target.testBeforeQuarantine = func() error {
		if err := os.Remove(final); err != nil {
			return err
		}
		return os.WriteFile(final, []byte("attacker"), 0o600)
	}
	target.testAfterQuarantine = func(string) error {
		return os.WriteFile(final, []byte("new-final"), 0o600)
	}
	err = securePublishKubeconfig(target, final)
	if err == nil || !strings.Contains(err.Error(), "inode mismatch") {
		t.Fatalf("cleanup error=%v", err)
	}
	data, err := os.ReadFile(final)
	if err != nil || string(data) != "new-final" {
		t.Fatalf("new final was touched: data=%q err=%v", data, err)
	}
	matches, err := filepath.Glob(filepath.Join(root, ".quarantine-*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("quarantine matches=%v err=%v", matches, err)
	}
	data, err = os.ReadFile(matches[0])
	if err != nil || string(data) != "attacker" {
		t.Fatalf("quarantined replacement=%q err=%v", data, err)
	}
	witnesses, err := filepath.Glob(filepath.Join(root, ".ownership-*"))
	if err != nil || len(witnesses) != 0 {
		t.Fatalf("unexpected ownership links=%v err=%v", witnesses, err)
	}
}

func TestPostSuccessSymlinkMutationIsRejectedOnRead(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "cluster.kubeconfig")
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("credential")); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(target, final); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "outside")
	if err := os.WriteFile(outside, []byte("attacker"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(final); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, final); err != nil {
		t.Fatal(err)
	}
	if _, err := secureReadKubeconfig(final); err == nil {
		t.Fatal("accepted post-success symlink mutation")
	}
}

func TestPostSuccessRegularMutationIsRejectedOnTrackedRead(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	identity := ClusterIdentity{Profile: "tracked", Project: "demo", Zone: "zone", Cluster: "cluster"}
	final := kindKubeconfigPath(identity)
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("credential")); err != nil {
		t.Fatal(err)
	}
	ownership, err := kubeconfigOwnershipFromTarget(identity, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizeKubeconfigOwnership(target, ownership); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(target, final); err != nil {
		t.Fatal(err)
	}
	backend := &KindBackend{}
	name, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	backend.kubeconfigOwners.Store(name, ownership)
	if err := os.Remove(final); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(final, []byte("attacker"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.ReadKubeconfig(identity); err == nil {
		t.Fatal("accepted post-success regular-file inode replacement")
	}
}

func TestContentDigestRejectsReplacementWithReusedFileIdentity(t *testing.T) {
	// Threat boundary: the digest proves trusted-state content equivalence, not
	// provenance. A byte-identical, single-link replacement with reused
	// device/inode identity is indistinguishable; this case changes the bytes.
	identity, backend, final, root := prepareOwnedDeletionTest(t)
	logicalName, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := backend.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	if !ok || ownership == nil {
		t.Fatal("missing original ownership")
	}
	if err := os.Remove(final); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(final, []byte("attacker"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(final)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("replacement file identity unavailable")
	}
	forged := *ownership
	forged.Device = uint64(stat.Dev)
	forged.Inode = uint64(stat.Ino)
	if data, err := secureReadKubeconfigOwnership(final, &forged); err == nil || data != nil {
		t.Fatalf("replacement read data=%q err=%v", data, err)
	}
	err = secureQuarantineOwnedKubeconfig(final, &forged)
	if err == nil || !strings.Contains(err.Error(), "manual recovery required") {
		t.Fatalf("quarantine error=%v", err)
	}
	matches, globErr := filepath.Glob(filepath.Join(root, ".quarantine-*"))
	if globErr != nil || len(matches) != 1 {
		t.Fatalf("quarantines=%v err=%v", matches, globErr)
	}
	data, readErr := os.ReadFile(matches[0])
	if readErr != nil || string(data) != "attacker" {
		t.Fatalf("attacker quarantine=%q err=%v", data, readErr)
	}
	witnesses, globErr := filepath.Glob(filepath.Join(root, ".ownership-*"))
	if globErr != nil || len(witnesses) != 0 {
		t.Fatalf("unexpected ownership links=%v err=%v", witnesses, globErr)
	}
}

func TestContentDigestRejectsSameInodeContentTamper(t *testing.T) {
	identity, backend, final, root := prepareOwnedDeletionTest(t)
	logicalName, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := backend.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	if !ok || ownership == nil {
		t.Fatal("missing original ownership")
	}
	if err := os.WriteFile(final, []byte("attacker"), 0o600); err != nil {
		t.Fatal(err)
	}
	if data, err := secureReadKubeconfigOwnership(final, ownership); err == nil || data != nil {
		t.Fatalf("tampered read data=%q err=%v", data, err)
	}
	err = secureQuarantineOwnedKubeconfig(final, ownership)
	if err == nil || !strings.Contains(err.Error(), "manual recovery required") {
		t.Fatalf("quarantine error=%v", err)
	}
	matches, globErr := filepath.Glob(filepath.Join(root, ".quarantine-*"))
	if globErr != nil || len(matches) != 1 {
		t.Fatalf("quarantines=%v err=%v", matches, globErr)
	}
	data, readErr := os.ReadFile(matches[0])
	if readErr != nil || string(data) != "attacker" {
		t.Fatalf("tampered quarantine=%q err=%v", data, readErr)
	}
}

func TestDualLinkReplacementIsNeverReadOrZeroized(t *testing.T) {
	identity, backend, final, root := prepareOwnedDeletionTest(t)
	logicalName, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := backend.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	if !ok || ownership == nil {
		t.Fatal("missing original ownership")
	}
	if err := os.Remove(final); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "attacker")
	attacker := []byte("dual-link-attacker")
	if err := os.WriteFile(outside, attacker, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(outside, final); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(final)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("replacement file identity unavailable")
	}
	forged := *ownership
	forged.Device = uint64(stat.Dev)
	forged.Inode = uint64(stat.Ino)
	sum := sha256.Sum256(attacker)
	forged.SHA256 = fmt.Sprintf("%x", sum[:])
	if data, err := secureReadKubeconfigOwnership(final, &forged); err == nil || data != nil {
		t.Fatalf("dual-link read data=%q err=%v", data, err)
	}
	err = secureQuarantineOwnedKubeconfig(final, &forged)
	if err == nil || !strings.Contains(err.Error(), "manual recovery required") {
		t.Fatalf("quarantine error=%v", err)
	}
	for _, path := range []string{outside} {
		data, readErr := os.ReadFile(path)
		if readErr != nil || string(data) != string(attacker) {
			t.Fatalf("attacker link %q=%q err=%v", path, data, readErr)
		}
	}
	matches, globErr := filepath.Glob(filepath.Join(root, ".quarantine-*"))
	if globErr != nil || len(matches) != 1 {
		t.Fatalf("quarantines=%v err=%v", matches, globErr)
	}
	data, readErr := os.ReadFile(matches[0])
	if readErr != nil || string(data) != string(attacker) {
		t.Fatalf("attacker quarantine=%q err=%v", data, readErr)
	}
}

func TestOwnershipDigestContainsNoCredentialMaterial(t *testing.T) {
	identity, backend, _, _ := prepareOwnedDeletionTest(t)
	logicalName, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := backend.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	if !ok || ownership == nil {
		t.Fatal("missing ownership")
	}
	sum := sha256.Sum256([]byte("credential"))
	if ownership.SHA256 != fmt.Sprintf("%x", sum[:]) {
		t.Fatalf("digest=%q", ownership.SHA256)
	}
	encoded, err := json.Marshal(ownership)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "credential") {
		t.Fatalf("ownership metadata contains credential material: %s", encoded)
	}
}

func TestLegacyOwnershipWithoutDigestFailsClosed(t *testing.T) {
	identity, backend, final, _ := prepareOwnedDeletionTest(t)
	logicalName, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := backend.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	legacy := *ownership
	legacy.SHA256 = ""
	restarted := &KindBackend{enabled: true}
	restarted.RestoreKubeconfigOwnership(identity, &legacy)
	if _, err := restarted.ReadKubeconfig(identity); err == nil {
		t.Fatal("legacy ownership authorized kubeconfig read")
	}
	if err := restarted.DeleteClusterContext(t.Context(), identity); err == nil {
		t.Fatal("legacy ownership authorized cluster deletion")
	}
	data, err := os.ReadFile(final)
	if err != nil || string(data) != "credential" {
		t.Fatalf("legacy refusal changed credential=%q err=%v", data, err)
	}
}

func TestCorruptMainOwnershipChecksumFailsClosedAfterRestart(t *testing.T) {
	identity, backend, final, _ := prepareOwnedDeletionTest(t)
	logicalName, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := backend.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	clone := *ownership
	key := clusterKey(identity.Project, identity.Zone, identity.Cluster)
	metadata := checkedGKEMetadata(t,
		map[string]*Cluster{key: {Name: identity.Cluster, Location: identity.Zone}},
		map[string]*kubeconfigOwnership{key: &clone})
	metadata.Ownerships[key].SHA256 = strings.Repeat("0", 64)
	store, err := state.New(t.TempDir(), identity.Profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(gkeStateEntry, metadata); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAPIWithStore(nil, store); err == nil {
		t.Fatal("corrupt main ownership checksum was silently discarded")
	}
	data, err := os.ReadFile(final)
	if err != nil || string(data) != "credential" {
		t.Fatalf("checksum refusal changed credential=%q err=%v", data, err)
	}
}

func TestKindFailurePropagatesAmbiguousCleanup(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	identity := ClusterIdentity{Profile: "cleanup", Project: "demo", Zone: "zone", Cluster: "cluster"}
	binDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(binDir, "kind"),
		[]byte("#!/bin/sh\nif [ \"$1\" = get ]; then exit 0; fi\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	backend := &KindBackend{
		enabled: true,
		testConfigureKubeconfigTarget: func(target *secureKubeconfigTarget) {
			target.testBeforeQuarantine = func() error {
				if err := os.Remove(target.path); err != nil {
					return err
				}
				return os.WriteFile(target.path, []byte("attacker"), 0o600)
			}
		},
	}
	_, err := backend.CreateClusterContext(t.Context(), identity)
	if err == nil || !strings.Contains(err.Error(), "inode mismatch") {
		t.Fatalf("Kind failure did not propagate cleanup ambiguity: %v", err)
	}
}

func TestKindWritesOnlyPrivateOwnedKubeconfigTarget(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root = filepath.Join(root, "private")
	final := filepath.Join(root, "cluster.kubeconfig")
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	defer secureDiscardKubeconfig(target)
	cmd := exec.Command("sh", "-c", `printf lock > "$1.lock"; printf complete > "$1"; rm "$1.lock"`, "sh")
	commandPath, err := secureKubeconfigCommandPath(target, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if commandPath != target.path {
		t.Fatalf("command path=%q want private owned path %q", commandPath, target.path)
	}
	cmd.Args = append(cmd.Args, commandPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("write inherited target: %v: %s", err, output)
	}
	if err := securePublishKubeconfig(target, final); err != nil {
		t.Fatal(err)
	}
	data, err := secureReadKubeconfig(final)
	if err != nil || string(data) != "complete" {
		t.Fatalf("data=%q err=%v", data, err)
	}
}

func TestSecurePublishRejectsTemporarySymlinkReplacement(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "cluster.kubeconfig")
	temporary, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(temporary.path); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, temporary.path); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(temporary, final); err == nil {
		t.Fatal("published symlink-replaced temporary kubeconfig")
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "secret" {
		t.Fatalf("outside file changed: %q, %v", data, err)
	}
}

func TestSecurePublishRejectsRegularTemporaryReplacement(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "cluster.kubeconfig")
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	defer secureDiscardKubeconfig(target)
	if err := os.Remove(target.path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target.path, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("original")); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(target, final); err == nil {
		t.Fatal("published replacement regular file")
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("replacement remained at final path: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(root, ".quarantine-*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("quarantine matches=%v err=%v", matches, err)
	}
	data, err := os.ReadFile(matches[0])
	if err != nil || string(data) != "replacement" {
		t.Fatalf("quarantined replacement=%q err=%v", data, err)
	}
}

func TestSecurePublishRejectsDirectoryReplacementDuringKindWrite(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ownedDir := filepath.Join(root, "gke")
	final := filepath.Join(ownedDir, "cluster.kubeconfig")
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	defer secureDiscardKubeconfig(target)
	movedDir := filepath.Join(root, "gke-moved")
	if err := os.Rename(ownedDir, movedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(ownedDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("complete")); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(target, final); err == nil {
		t.Fatal("published through replaced kubeconfig directory")
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("replacement directory received kubeconfig: %v", err)
	}
}

func TestSecurePrepareDoesNotOverwriteFinalSymlink(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	final := filepath.Join(root, "cluster.kubeconfig")
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, final); err != nil {
		t.Fatal(err)
	}
	if _, err := securePrepareKubeconfig(final); err == nil {
		t.Fatal("overwrote existing final symlink")
	}
	outsideData, err := os.ReadFile(outside)
	if err != nil || string(outsideData) != "outside" {
		t.Fatalf("outside data=%q err=%v", outsideData, err)
	}
}

func TestKubeconfigReadWaitsForAtomicPublish(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "logical")
	identity := ClusterIdentity{Profile: "race", Project: "demo", Zone: "zone", Cluster: "cluster"}
	backend := &KindBackend{}
	name, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	unlock := backend.lockName(name)
	result := make(chan []byte, 1)
	fail := make(chan error, 1)
	go func() {
		data, err := backend.ReadKubeconfig(identity)
		if err != nil {
			fail <- err
			return
		}
		result <- data
	}()
	select {
	case <-result:
		t.Fatal("reader bypassed lifecycle lock")
	case err := <-fail:
		t.Fatalf("reader bypassed lifecycle lock: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	final := kindKubeconfigPath(identity)
	temporary, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := temporary.file.Write([]byte("complete")); err != nil {
		t.Fatal(err)
	}
	ownership, err := kubeconfigOwnershipFromTarget(identity, temporary)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizeKubeconfigOwnership(temporary, ownership); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(temporary, final); err != nil {
		t.Fatal(err)
	}
	backend.RestoreKubeconfigOwnership(identity, ownership)
	unlock()
	select {
	case data := <-result:
		if string(data) != "complete" {
			t.Fatalf("read partial kubeconfig %q", data)
		}
	case err := <-fail:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("reader remained blocked")
	}
}

func TestAPIReadsKubeconfigOnlyForPersistedLogicalIdentity(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "logical")
	identity := ClusterIdentity{Profile: config.GetProfile(), Project: "demo", Zone: "zone", Cluster: "cluster"}
	backend := &KindBackend{}
	api := newAPIWithBackend(nil, backend, "", nil, nil)
	final := kindKubeconfigPath(identity)
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("complete")); err != nil {
		t.Fatal(err)
	}
	ownership, err := kubeconfigOwnershipFromTarget(identity, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizeKubeconfigOwnership(target, ownership); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(target, final); err != nil {
		t.Fatal(err)
	}
	if _, err := api.ReadKubeconfig(identity.Project, identity.Zone, identity.Cluster); err == nil {
		t.Fatal("read kubeconfig for unpersisted logical identity")
	}
	api.clusters[clusterKey(identity.Project, identity.Zone, identity.Cluster)] = &Cluster{
		Name: identity.Cluster, Location: identity.Zone,
	}
	api.ownerships[clusterKey(identity.Project, identity.Zone, identity.Cluster)] = ownership
	backend.RestoreKubeconfigOwnership(identity, ownership)
	data, err := api.ReadKubeconfig(identity.Project, identity.Zone, identity.Cluster)
	if err != nil || string(data) != "complete" {
		t.Fatalf("data=%q err=%v", data, err)
	}
}

func TestKubeconfigDeleteWaitsForLifecycleLock(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	identity := ClusterIdentity{Profile: "delete-race", Project: "demo", Zone: "zone", Cluster: "cluster"}
	name, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	final := kindKubeconfigPath(identity)
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("complete")); err != nil {
		t.Fatal(err)
	}
	ownership, err := kubeconfigOwnershipFromTarget(identity, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizeKubeconfigOwnership(target, ownership); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(target, final); err != nil {
		t.Fatal(err)
	}
	binDir := t.TempDir()
	marker := filepath.Join(binDir, "deleted")
	script := "#!/bin/sh\nif [ \"$1\" = get ]; then echo " + name +
		"; else printf deleted > " + marker + "; fi\n"
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	backend := &KindBackend{enabled: true}
	backend.kubeconfigOwners.Store(name, ownership)
	unlock := backend.lockName(name)
	done := make(chan error, 1)
	go func() { done <- backend.DeleteClusterContext(t.Context(), identity) }()
	select {
	case err := <-done:
		t.Fatalf("delete bypassed lifecycle lock: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("delete command ran while lifecycle lock held: %v", err)
	}
	unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := secureReadKubeconfig(final); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("kubeconfig remains after delete: %v", err)
	}
}

func TestExpectedKubeconfigDeletionLeavesZeroizedTombstone(t *testing.T) {
	identity, backend, final, root := prepareOwnedDeletionTest(t)
	if err := backend.DeleteClusterContext(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("final kubeconfig remains: %v", err)
	}
	assertZeroizedQuarantine(t, root, ".quarantine-*")
}

func TestKubeconfigDeletionQuarantinesReplacementWithoutDeletingIt(t *testing.T) {
	identity, backend, final, root := prepareOwnedDeletionTest(t)
	if err := os.Remove(final); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(final, []byte("attacker"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := backend.DeleteClusterContext(t.Context(), identity)
	if err == nil || !strings.Contains(err.Error(), "manual recovery required") {
		t.Fatalf("delete error=%v", err)
	}
	matches, globErr := filepath.Glob(filepath.Join(root, ".quarantine-*"))
	if globErr != nil || len(matches) != 1 {
		t.Fatalf("quarantines=%v err=%v", matches, globErr)
	}
	data, readErr := os.ReadFile(matches[0])
	if readErr != nil || string(data) != "attacker" {
		t.Fatalf("attacker quarantine=%q err=%v", data, readErr)
	}
}

func TestRestartReloadsOwnershipForExpectedDeletion(t *testing.T) {
	identity, original, _, root := prepareOwnedDeletionTest(t)
	logicalName, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := original.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	if !ok || ownership == nil {
		t.Fatal("missing original ownership")
	}
	store, err := state.New(t.TempDir(), identity.Profile)
	if err != nil {
		t.Fatal(err)
	}
	key := clusterKey(identity.Project, identity.Zone, identity.Cluster)
	metadata := checkedGKEMetadata(t,
		map[string]*Cluster{key: {Name: identity.Cluster, Location: identity.Zone}},
		map[string]*kubeconfigOwnership{key: ownership})
	if err := store.Save(gkeStateEntry, metadata); err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	backend := api.GetBackend()
	backend.enabled = true
	configureKindDeleteScript(t, ownership)
	if err := backend.DeleteClusterContext(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	assertZeroizedQuarantine(t, root, ".quarantine-*")
}

func TestRestartReplacementIsQuarantinedAgainstPersistedOwnership(t *testing.T) {
	identity, original, final, root := prepareOwnedDeletionTest(t)
	logicalName, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := original.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	if !ok || ownership == nil {
		t.Fatal("missing original ownership")
	}
	backend := &KindBackend{enabled: true}
	backend.RestoreKubeconfigOwnership(identity, ownership)
	configureKindDeleteScript(t, ownership)
	if err := os.Remove(final); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(final, []byte("restart-attacker"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = backend.DeleteClusterContext(t.Context(), identity)
	if err == nil || !strings.Contains(err.Error(), "manual recovery required") {
		t.Fatalf("delete error=%v", err)
	}
	matches, globErr := filepath.Glob(filepath.Join(root, ".quarantine-*"))
	if globErr != nil || len(matches) != 1 {
		t.Fatalf("quarantines=%v err=%v", matches, globErr)
	}
	data, readErr := os.ReadFile(matches[0])
	if readErr != nil || string(data) != "restart-attacker" {
		t.Fatalf("attacker quarantine=%q err=%v", data, readErr)
	}
}

func TestLegacyDeletionWithoutOwnershipMarkerRefusesPathMutation(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	identity := ClusterIdentity{Profile: "legacy", Project: "demo", Zone: "zone", Cluster: "cluster"}
	final := kindKubeconfigPath(identity)
	if err := os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(final, []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	backend := &KindBackend{enabled: true}
	if err := backend.DeleteClusterContext(t.Context(), identity); err == nil {
		t.Fatal("legacy deletion succeeded without ownership marker")
	}
	data, err := os.ReadFile(final)
	if err != nil || string(data) != "legacy" {
		t.Fatalf("legacy path changed: data=%q err=%v", data, err)
	}
}

func TestTransientKindDeleteFailurePreservesCredentialForRetry(t *testing.T) {
	identity, backend, final, root := prepareOwnedDeletionTest(t)
	name := ownedBackendName(t, backend, identity)
	binDir := t.TempDir()
	attempt := filepath.Join(binDir, "attempted")
	deleted := filepath.Join(binDir, "deleted")
	script := "#!/bin/sh\nif [ \"$1\" = get ]; then if [ ! -f " + deleted + " ]; then echo " + name +
		"; fi; elif [ ! -f " + attempt + " ]; then printf attempted > " + attempt +
		"; exit 1; else printf deleted > " + deleted + "; fi\nexit 0\n"
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	if err := backend.DeleteClusterContext(t.Context(), identity); err == nil {
		t.Fatal("transient Kind failure unexpectedly succeeded")
	}
	data, err := os.ReadFile(final)
	if err != nil || string(data) != "credential" {
		t.Fatalf("credential changed after transient Kind failure: data=%q err=%v", data, err)
	}
	if err := backend.DeleteClusterContext(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	assertZeroizedQuarantine(t, root, ".quarantine-*")
}

func TestPostKindCleanupFailureRetriesAgainstAbsentBackend(t *testing.T) {
	identity, backend, final, _ := prepareOwnedDeletionTest(t)
	name := ownedBackendName(t, backend, identity)
	binDir := t.TempDir()
	deleted := filepath.Join(binDir, "deleted")
	script := "#!/bin/sh\nif [ \"$1\" = get ]; then if [ ! -f " + deleted + " ]; then echo " + name +
		"; fi; else printf deleted > " + deleted + "; fi\nexit 0\n"
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	injected := errors.New("injected quarantine failure")
	testSecureQuarantineOwned = func() error { return injected }
	t.Cleanup(func() { testSecureQuarantineOwned = nil })
	err := backend.DeleteClusterContext(t.Context(), identity)
	if !errors.Is(err, injected) {
		t.Fatalf("first delete error=%v", err)
	}
	data, err := os.ReadFile(final)
	if err != nil || string(data) != "credential" {
		t.Fatalf("credential changed after cap failure: data=%q err=%v", data, err)
	}
	testSecureQuarantineOwned = nil
	if err := backend.DeleteClusterContext(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(final)
	if !os.IsNotExist(err) || info != nil {
		t.Fatalf("final remains after retry: info=%v err=%v", info, err)
	}
}

func TestPostKindCleanupFailureRetriesAfterRestart(t *testing.T) {
	identity, backend, _, root := prepareOwnedDeletionTest(t)
	logicalName, _ := kindBackendName(identity)
	rawOwnership, _ := backend.kubeconfigOwners.Load(logicalName)
	ownership, _ := rawOwnership.(*kubeconfigOwnership)
	name := ownership.BackendName
	binDir := t.TempDir()
	deleted := filepath.Join(binDir, "deleted")
	script := "#!/bin/sh\nif [ \"$1\" = get ]; then if [ ! -f " + deleted + " ]; then echo " + name +
		"; fi; else printf deleted > " + deleted + "; fi\nexit 0\n"
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	injected := errors.New("injected restart quarantine failure")
	testSecureQuarantineOwned = func() error { return injected }
	t.Cleanup(func() { testSecureQuarantineOwned = nil })
	if err := backend.DeleteClusterContext(t.Context(), identity); !errors.Is(err, injected) {
		t.Fatalf("first delete error=%v", err)
	}
	store, err := state.New(t.TempDir(), identity.Profile)
	if err != nil {
		t.Fatal(err)
	}
	key := clusterKey(identity.Project, identity.Zone, identity.Cluster)
	metadata := checkedGKEMetadata(t,
		map[string]*Cluster{key: {Name: identity.Cluster, Location: identity.Zone}},
		map[string]*kubeconfigOwnership{key: ownership})
	if err := store.Save(gkeStateEntry, metadata); err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	restarted := api.GetBackend()
	restarted.enabled = true
	testSecureQuarantineOwned = nil
	if err := restarted.DeleteClusterContext(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	assertZeroizedQuarantine(t, root, ".quarantine-*")
}

func prepareOwnedDeletionTest(t *testing.T) (ClusterIdentity, *KindBackend, string, string) {
	t.Helper()
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "owned")
	identity := ClusterIdentity{Profile: "owned", Project: "demo", Zone: "zone", Cluster: "cluster"}
	final := kindKubeconfigPath(identity)
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("credential")); err != nil {
		t.Fatal(err)
	}
	ownership, err := kubeconfigOwnershipFromTarget(identity, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizeKubeconfigOwnership(target, ownership); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(target, final); err != nil {
		t.Fatal(err)
	}
	backend := &KindBackend{enabled: true}
	backend.RestoreKubeconfigOwnership(identity, ownership)
	configureKindDeleteScript(t, ownership)
	return identity, backend, final, filepath.Dir(final)
}

func ownedBackendName(t *testing.T, backend *KindBackend, identity ClusterIdentity) string {
	t.Helper()
	logicalName, err := kindBackendName(identity)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := backend.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	if !ok || ownership == nil {
		t.Fatal("missing test backend ownership")
	}
	return ownership.BackendName
}

func configureKindDeleteScript(t *testing.T, ownership *kubeconfigOwnership) {
	t.Helper()
	binDir := t.TempDir()
	deleted := filepath.Join(binDir, "deleted")
	script := "#!/bin/sh\nif [ \"$1\" = get ]; then if [ ! -f " + deleted +
		" ]; then echo " + ownership.BackendName + "; fi; else printf deleted > " + deleted + "; fi\nexit 0\n"
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
}

func assertZeroizedQuarantine(t *testing.T, root, pattern string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(root, pattern))
	if err != nil || len(matches) != 1 {
		t.Fatalf("quarantines=%v err=%v", matches, err)
	}
	info, err := os.Stat(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 || info.Mode().Perm() != 0 {
		t.Fatalf("quarantine size=%d mode=%o", info.Size(), info.Mode().Perm())
	}
}

func countTrackedKubeconfigEntries(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".kubeconfig") ||
			strings.HasPrefix(name, ".quarantine-") ||
			strings.HasPrefix(name, ".deleted-") ||
			strings.HasPrefix(name, ".ownership-") {
			count++
		}
	}
	return count
}

func TestKubeconfigTombstoneCapIsStableAcrossRetries(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxKubeconfigEntries; i++ {
		name := filepath.Join(root, fmt.Sprintf("active-%032x.kubeconfig", i))
		if err := os.WriteFile(name, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	final := filepath.Join(root, "cluster.kubeconfig")
	for attempt := 0; attempt < 3; attempt++ {
		_, err := securePrepareKubeconfig(final)
		if !errors.Is(err, errKubeconfigEntryLimit) {
			t.Fatalf("attempt %d error=%v", attempt, err)
		}
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("rejected creation left final entry: %v", err)
	}
}

func TestOwnedKubeconfigAddsNoWitnessEntryAtCapacity(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	identity := ClusterIdentity{
		Profile: "capacity", Project: "demo", Zone: "zone", Cluster: "cluster",
	}
	root := filepath.Dir(kindKubeconfigPath(identity))
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxKubeconfigEntries-1; i++ {
		name := filepath.Join(root, fmt.Sprintf("active-%032x.kubeconfig", i))
		if err := os.WriteFile(name, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	target, _, err := prepareKubeconfigWithIntent(identity)
	if err != nil {
		t.Fatalf("prepare error=%v", err)
	}
	witnesses, err := filepath.Glob(filepath.Join(root, ".ownership-*"))
	if err != nil || len(witnesses) != 0 {
		t.Fatalf("unexpected ownership links=%v err=%v", witnesses, err)
	}
	if err := secureDiscardKubeconfig(target); err != nil {
		t.Fatal(err)
	}
}

func TestKubeconfigEntryCapSerializesDistinctIdentities(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	failures := 0
	for i := 0; i < maxKubeconfigEntries+16; i++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			target, err := securePrepareKubeconfig(
				filepath.Join(root, fmt.Sprintf("cluster-%d.kubeconfig", index)))
			if err != nil {
				if !errors.Is(err, errKubeconfigEntryLimit) {
					t.Errorf("prepare %d: %v", index, err)
					return
				}
				mu.Lock()
				failures++
				mu.Unlock()
				return
			}
			if err := securePublishKubeconfig(target, target.path); err != nil {
				t.Errorf("publish %d: %v", index, err)
				return
			}
			mu.Lock()
			successes++
			mu.Unlock()
		}(i)
	}
	group.Wait()
	if successes != maxKubeconfigEntries || failures != 16 {
		t.Fatalf("successes=%d failures=%d", successes, failures)
	}
	matches, err := filepath.Glob(filepath.Join(root, "*.kubeconfig"))
	if err != nil || len(matches) != maxKubeconfigEntries {
		t.Fatalf("entries=%d err=%v", len(matches), err)
	}
}

func TestOwnershipIntentCrashBoundariesReconcile(t *testing.T) {
	cases := []struct {
		name       string
		phase      kubeconfigIntentPhase
		credential bool
		expected   kubeconfigIntentPhase
	}{
		{name: "after-intent-before-kind", phase: intentPrepared, expected: intentPrepared},
		{name: "during-kind-before-create-started", phase: intentPrepared, credential: true, expected: intentPrepared},
		{name: "after-create-started", phase: intentCreateStarted, credential: true, expected: intentCleanupPending},
		{name: "after-backend-created-phase", phase: intentBackendCreated, credential: true, expected: intentCleanupPending},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MINISKY_STATE_DIR", t.TempDir())
			t.Setenv("MINISKY_PROFILE", "intent-crash")
			identity := ClusterIdentity{
				Profile: "intent-crash", Project: "demo", Zone: "zone", Cluster: tc.name,
			}
			final := kindKubeconfigPath(identity)
			target, err := securePrepareKubeconfig(final)
			if err != nil {
				t.Fatal(err)
			}
			ownership, err := kubeconfigOwnershipFromTarget(identity, target)
			if err != nil {
				t.Fatal(err)
			}
			if err := writeKubeconfigIntent(identity, ownership, intentPrepared); err != nil {
				t.Fatal(err)
			}
			if tc.credential {
				if _, err := target.file.Write([]byte("credential")); err != nil {
					t.Fatal(err)
				}
			}
			if tc.phase == intentBackendCreated {
				if err := finalizeKubeconfigOwnership(target, ownership); err != nil {
					t.Fatal(err)
				}
			}
			if err := securePublishKubeconfig(target, final); err != nil {
				t.Fatal(err)
			}
			if tc.phase != intentPrepared {
				if err := writeKubeconfigIntent(identity, ownership, tc.phase); err != nil {
					t.Fatal(err)
				}
			}
			backend := &KindBackend{}
			reconcileErr := backend.ReconcileKubeconfigIntents(t.Context(), "intent-crash", nil)
			if reconcileErr == nil {
				t.Fatal("disabled backend cleanup unexpectedly terminalized")
			}
			intent, err := loadKubeconfigIntent(identity)
			if err != nil || intent.Phase != tc.expected || intent.Error == "" {
				t.Fatalf("intent=%#v err=%v", intent, err)
			}
		})
	}
}

func TestCrashBeforeDigestRetainsUnmatchedQuarantineAcrossRestarts(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "unmatched-recovery")
	identity := ClusterIdentity{
		Profile: "unmatched-recovery", Project: "demo", Zone: "zone", Cluster: "cluster",
	}
	final := kindKubeconfigPath(identity)
	target, ownership, err := prepareKubeconfigWithIntent(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeKubeconfigIntent(identity, ownership, intentCreateStarted); err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("credential")); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(target, final); err != nil {
		t.Fatal(err)
	}
	binDir := t.TempDir()
	deleted := filepath.Join(binDir, "deleted")
	script := "#!/bin/sh\nif [ \"$1\" = get ]; then if [ ! -f " + deleted +
		" ]; then echo " + ownership.BackendName +
		"; fi; else printf x >> " + deleted + "; fi\nexit 0\n"
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)

	injected := errors.New("simulated crash before unmatched quarantine intent save")
	testWriteKubeconfigIntent = func(phase kubeconfigIntentPhase) error {
		if phase == intentCleanupPending {
			return injected
		}
		return nil
	}
	t.Cleanup(func() { testWriteKubeconfigIntent = nil })
	first := &KindBackend{enabled: true}
	if err := first.ReconcileKubeconfigIntents(
		t.Context(), identity.Profile, nil,
	); !errors.Is(err, injected) {
		t.Fatalf("first restart error=%v", err)
	}
	testWriteKubeconfigIntent = nil
	intent, err := loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentCreateStarted ||
		intent.UnmatchedQuarantine != nil {
		t.Fatalf("post-crash intent=%#v err=%v", intent, err)
	}
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(final), ".quarantine-*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("post-crash quarantines=%v err=%v", matches, err)
	}
	quarantinePath := matches[0]
	if data, err := os.ReadFile(quarantinePath); err != nil || string(data) != "credential" {
		t.Fatalf("post-crash quarantine=%q err=%v", data, err)
	}
	if err := os.WriteFile(final, []byte("unrelated-replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	entryCount := countTrackedKubeconfigEntries(t, filepath.Dir(final))

	var evidence *unmatchedKubeconfigQuarantine
	for restart := 0; restart < 3; restart++ {
		backend := &KindBackend{enabled: true}
		err := backend.ReconcileKubeconfigIntents(t.Context(), identity.Profile, nil)
		if err == nil || !strings.Contains(err.Error(), "manual recovery") {
			t.Fatalf("restart %d error=%v", restart, err)
		}
		intent, loadErr := loadKubeconfigIntent(identity)
		if loadErr != nil || intent.Phase != intentCleanupPending ||
			intent.UnmatchedQuarantine == nil || intent.Error == "" {
			t.Fatalf("restart %d intent=%#v err=%v", restart, intent, loadErr)
		}
		if !strings.Contains(intent.Error, intent.UnmatchedQuarantine.Entry) ||
			!strings.Contains(intent.Error, "device=") ||
			!strings.Contains(intent.Error, "manual recovery") {
			t.Fatalf("restart %d error is not actionable: %q", restart, intent.Error)
		}
		if restart == 0 {
			evidence = intent.UnmatchedQuarantine
			if got := filepath.Join(filepath.Dir(final), evidence.Entry); got != quarantinePath {
				t.Fatalf("evidence path=%q want %q", got, quarantinePath)
			}
		} else if *intent.UnmatchedQuarantine != *evidence {
			t.Fatalf("restart %d evidence changed: %#v want %#v",
				restart, intent.UnmatchedQuarantine, evidence)
		}
		if got := countTrackedKubeconfigEntries(t, filepath.Dir(final)); got != entryCount {
			t.Fatalf("restart %d tracked entries=%d want %d", restart, got, entryCount)
		}
		if data, readErr := os.ReadFile(quarantinePath); readErr != nil ||
			string(data) != "credential" {
			t.Fatalf("restart %d quarantine=%q err=%v", restart, data, readErr)
		}
		if data, readErr := os.ReadFile(final); readErr != nil ||
			string(data) != "unrelated-replacement" {
			t.Fatalf("restart %d replacement=%q err=%v", restart, data, readErr)
		}
		if data, readErr := os.ReadFile(deleted); readErr != nil || string(data) != "x" {
			t.Fatalf("restart %d backend deletes=%q err=%v", restart, data, readErr)
		}
		if data, readErr := backend.ReadKubeconfig(identity); readErr == nil || data != nil {
			t.Fatalf("restart %d exposed data=%q err=%v", restart, data, readErr)
		}
	}
}

func TestPreparedIntentNeverDeletesAnyKindBackend(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "prepared-only")
	identity := ClusterIdentity{Profile: "prepared-only", Project: "demo", Zone: "zone", Cluster: "cluster"}
	target, ownership, err := prepareKubeconfigWithIntent(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(target.file.Close(), target.dir.Close()); err != nil {
		t.Fatal(err)
	}
	called := filepath.Join(t.TempDir(), "kind-called")
	binDir := filepath.Dir(called)
	script := "#!/bin/sh\nprintf called > " + called + "\nexit 0\n"
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	backend := &KindBackend{enabled: true}
	if err := backend.ReconcileKubeconfigIntents(t.Context(), identity.Profile, nil); err == nil {
		t.Fatal("digest-less PREPARED intent unexpectedly reconciled")
	}
	if _, err := os.Stat(called); !os.IsNotExist(err) {
		t.Fatalf("PREPARED reconciliation invoked Kind: %v", err)
	}
	intent, err := loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentPrepared || intent.Error == "" ||
		intent.Ownership.BackendName != ownership.BackendName {
		t.Fatalf("intent=%#v err=%v", intent, err)
	}
}

func TestLegacyDeterministicIntentFailsClosed(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "legacy-intent")
	identity := ClusterIdentity{Profile: "legacy-intent", Project: "demo", Zone: "zone", Cluster: "cluster"}
	final := kindKubeconfigPath(identity)
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := kubeconfigOwnershipFromTarget(identity, target)
	if err != nil {
		t.Fatal(err)
	}
	ownership.BackendName, _ = kindBackendName(identity)
	if err := writeKubeconfigIntent(identity, ownership, intentCreateStarted); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(target.file.Close(), target.dir.Close()); err != nil {
		t.Fatal(err)
	}
	backend := &KindBackend{enabled: true}
	if err := backend.ReconcileKubeconfigIntents(t.Context(), identity.Profile, nil); err == nil {
		t.Fatal("legacy deterministic intent was accepted")
	}
	intent, err := loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentCreateStarted {
		t.Fatalf("legacy intent mutated: %#v err=%v", intent, err)
	}
	if _, err := os.Stat(final); err != nil {
		t.Fatalf("legacy credential entry mutated: %v", err)
	}
}

func TestDisabledKindRetainsRetryableCleanupIntent(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "disabled-cleanup")
	identity := ClusterIdentity{Profile: "disabled-cleanup", Project: "demo", Zone: "zone", Cluster: "cluster"}
	target, ownership, err := prepareKubeconfigWithIntent(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeKubeconfigIntent(identity, ownership, intentCreateStarted); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(target.file.Close(), target.dir.Close()); err != nil {
		t.Fatal(err)
	}
	backend := &KindBackend{}
	if err := backend.ReconcileKubeconfigIntents(t.Context(), identity.Profile, nil); err == nil {
		t.Fatal("disabled cleanup unexpectedly succeeded")
	}
	intent, err := loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentCleanupPending || intent.Error == "" {
		t.Fatalf("intent=%#v err=%v", intent, err)
	}
	if _, err := os.Stat(kindKubeconfigPath(identity)); err != nil {
		t.Fatalf("retryable credential evidence removed: %v", err)
	}
}

func TestNormalDeletionTerminalizesOnlyAfterMetadataDurability(t *testing.T) {
	identity, backend, _, _ := prepareOwnedDeletionTest(t)
	logicalName, _ := kindBackendName(identity)
	raw, _ := backend.kubeconfigOwners.Load(logicalName)
	ownership, _ := raw.(*kubeconfigOwnership)
	if err := backend.DeleteClusterContext(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	intent, err := loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentDeleteCleaned {
		t.Fatalf("pre-durability intent=%#v err=%v", intent, err)
	}
	if err := backend.FinalizeDeleteIntent(identity, ownership); err != nil {
		t.Fatal(err)
	}
	intent, err = loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentTerminal {
		t.Fatalf("post-durability intent=%#v err=%v", intent, err)
	}
}

func TestCreateSuccessBeforeBackendPhaseSaveCleansExactNonceBackend(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "phase-save")
	binDir := t.TempDir()
	stateFile := filepath.Join(binDir, "backend")
	script := `#!/bin/sh
if [ "$1" = get ]; then
  if [ -s "` + stateFile + `" ]; then IFS= read -r backend < "` + stateFile + `"; printf '%s\n' "$backend"; fi
elif [ "$1" = create ]; then
  shift 2
  while [ "$#" -gt 0 ]; do
    if [ "$1" = --name ]; then name="$2"; shift 2
    elif [ "$1" = --kubeconfig ]; then kubeconfig="$2"; shift 2
    else shift
    fi
  done
  printf '%s\n' "$name" > "` + stateFile + `"
  printf credential > "$kubeconfig"
elif [ "$1" = delete ]; then
  : > "` + stateFile + `"
fi
exit 0
`
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	injected := errors.New("injected BACKEND_CREATED save failure")
	testWriteKubeconfigIntent = func(phase kubeconfigIntentPhase) error {
		if phase == intentBackendCreated {
			return injected
		}
		return nil
	}
	t.Cleanup(func() { testWriteKubeconfigIntent = nil })
	identity := ClusterIdentity{Profile: "phase-save", Project: "demo", Zone: "zone", Cluster: "cluster"}
	backend := &KindBackend{enabled: true}
	result, err := backend.CreateClusterContext(t.Context(), identity)
	if !errors.Is(err, injected) || result.Ownership == nil || !result.Ownership.isDurable() {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	sum := sha256.Sum256([]byte("credential"))
	if result.Ownership.SHA256 != fmt.Sprintf("%x", sum[:]) {
		t.Fatalf("ownership digest=%q", result.Ownership.SHA256)
	}
	if data, err := os.ReadFile(stateFile); err != nil || len(data) != 0 {
		t.Fatalf("owned backend remains: %q err=%v", data, err)
	}
	intent, err := loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentTerminal {
		t.Fatalf("intent=%#v err=%v", intent, err)
	}
}

func TestCrashBeforeOwnershipIntentLeavesNoCredentialOrAdoptionEvidence(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	identity := ClusterIdentity{Profile: "pre-intent", Project: "demo", Zone: "zone", Cluster: "cluster"}
	final := kindKubeconfigPath(identity)
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(target.file.Close(), target.dir.Close()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(final)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("pre-intent file contains %d credential bytes", info.Size())
	}
	if _, err := loadKubeconfigIntent(identity); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected pre-intent adoption evidence: %v", err)
	}
}

func TestOwnershipIntentCommitsAfterDurableMetadata(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "intent-commit")
	identity := ClusterIdentity{Profile: "intent-commit", Project: "demo", Zone: "zone", Cluster: "cluster"}
	final := kindKubeconfigPath(identity)
	target, err := securePrepareKubeconfig(final)
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := kubeconfigOwnershipFromTarget(identity, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeKubeconfigIntent(identity, ownership, intentPrepared); err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("credential")); err != nil {
		t.Fatal(err)
	}
	if err := finalizeKubeconfigOwnership(target, ownership); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(target, final); err != nil {
		t.Fatal(err)
	}
	if err := writeKubeconfigIntent(identity, ownership, intentBackendCreated); err != nil {
		t.Fatal(err)
	}
	key := clusterKey(identity.Project, identity.Zone, identity.Cluster)
	backend := &KindBackend{}
	if err := backend.ReconcileKubeconfigIntents(
		t.Context(), identity.Profile, map[string]*kubeconfigOwnership{key: ownership},
	); err != nil {
		t.Fatal(err)
	}
	intent, err := loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentCommitted ||
		intent.Ownership.SHA256 != ownership.SHA256 {
		t.Fatalf("intent=%#v err=%v", intent, err)
	}
	data, err := os.ReadFile(final)
	if err != nil || string(data) != "credential" {
		t.Fatalf("committed credential=%q err=%v", data, err)
	}
}

func TestOwnershipIntentIgnoresTornInactivePhaseSlot(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	identity := ClusterIdentity{Profile: "intent-torn", Project: "demo", Zone: "zone", Cluster: "cluster"}
	ownership := &kubeconfigOwnership{
		Profile: identity.Profile, Project: identity.Project, Zone: identity.Zone, Cluster: identity.Cluster,
		BackendName: "minisky-owned-0123456789abcdef0123456789abcdef",
		Device:      1, Inode: 2,
	}
	if err := writeKubeconfigIntent(identity, ownership, intentPrepared); err != nil {
		t.Fatal(err)
	}
	prefix, err := kubeconfigIntentPrefix(identity)
	if err != nil {
		t.Fatal(err)
	}
	torn := filepath.Join(filepath.Dir(kindKubeconfigPath(identity)), prefix+".0")
	if err := os.WriteFile(torn, []byte(`{"intent":`), 0o600); err != nil {
		t.Fatal(err)
	}
	intent, err := loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentPrepared {
		t.Fatalf("intent=%#v err=%v", intent, err)
	}
	if err := writeKubeconfigIntent(identity, ownership, intentBackendCreated); err != nil {
		t.Fatal(err)
	}
	intent, err = loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentBackendCreated {
		t.Fatalf("intent=%#v err=%v", intent, err)
	}
}

func TestDisabledKindDeleteSurvivesRestartAndRetriesWhenEnabled(t *testing.T) {
	api, store, identity, ownership, final := preparePersistedOwnedClusterAPI(t)
	backend := api.GetBackend()
	backend.enabled = false
	path := "/v1/projects/demo/zones/zone/clusters/cluster"

	for attempt := 0; attempt < 2; attempt++ {
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, path, nil))
		if rec.Code != http.StatusServiceUnavailable ||
			!strings.Contains(rec.Body.String(), `"status":"UNAVAILABLE"`) {
			t.Fatalf("attempt %d response=%d %s", attempt, rec.Code, rec.Body.String())
		}
		intent, err := loadKubeconfigIntent(identity)
		if err != nil || intent.Phase != intentDeletePending || intent.Error == "" {
			t.Fatalf("attempt %d intent=%#v err=%v", attempt, intent, err)
		}
		if data, err := os.ReadFile(final); err != nil || string(data) != "credential" {
			t.Fatalf("attempt %d credential=%q err=%v", attempt, data, err)
		}
		if api.clusters[clusterKey("demo", "zone", "cluster")] == nil ||
			api.ownerships[clusterKey("demo", "zone", "cluster")] == nil {
			t.Fatalf("attempt %d removed durable metadata", attempt)
		}
	}

	restarted, err := NewAPIWithStore(orchestrator.NewOperationManager(), store)
	if err != nil {
		t.Fatal(err)
	}
	restartedBackend := restarted.GetBackend()
	restartedBackend.mu.Lock()
	restartedBackend.enabled = true
	restartedBackend.mu.Unlock()
	configureKindDeleteScript(t, ownership)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(gateway.Close)
	restarted.ConfigureGateway(gateway.URL, gateway.Client())
	rec := httptest.NewRecorder()
	restarted.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("retry response=%d %s", rec.Code, rec.Body.String())
	}
	var initial GkeOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	if op := waitGKEOperation(t, restarted.opMgr, initial.Name); op.Error != nil {
		t.Fatalf("retry operation error=%#v", op.Error)
	}
	if restarted.clusters[clusterKey("demo", "zone", "cluster")] != nil ||
		restarted.ownerships[clusterKey("demo", "zone", "cluster")] != nil {
		t.Fatal("successful retry retained metadata")
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("successful retry retained credential: %v", err)
	}
	intent, err := loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentTerminal {
		t.Fatalf("terminal intent=%#v err=%v", intent, err)
	}
	terminalGeneration := intent.Generation
	rec = httptest.NewRecorder()
	restarted.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, path, nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("repeat successful delete=%d %s", rec.Code, rec.Body.String())
	}
	intent, err = loadKubeconfigIntent(identity)
	if err != nil || intent.Generation != terminalGeneration {
		t.Fatalf("intent terminalized more than once: %#v err=%v", intent, err)
	}
}

func TestKindBecomingUnavailableProducesRetryableLRO(t *testing.T) {
	api, _, identity, ownership, final := preparePersistedOwnedClusterAPI(t)
	backend := api.GetBackend()
	backend.mu.Lock()
	backend.enabled = true
	backend.mu.Unlock()
	binDir := t.TempDir()
	counter := filepath.Join(binDir, "calls")
	script := `#!/bin/sh
count=0
if [ -f "` + counter + `" ]; then IFS= read -r count < "` + counter + `"; fi
count=$((count + 1))
printf '%s' "$count" > "` + counter + `"
if [ "$count" -eq 1 ]; then printf '%s\n' "` + ownership.BackendName + `"; exit 0; fi
printf '%s\n' 'Cannot connect to the Docker daemon. Is the docker daemon running?' >&2
exit 1
`
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete,
		"/v1/projects/demo/zones/zone/clusters/cluster", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("initial response=%d %s", rec.Code, rec.Body.String())
	}
	var initial GkeOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	op := waitGKEOperation(t, api.opMgr, initial.Name)
	if op.Error == nil || op.Error.Code != http.StatusServiceUnavailable ||
		!strings.Contains(op.Error.Message, "UNAVAILABLE") {
		t.Fatalf("operation=%#v", op)
	}
	if api.clusters[clusterKey("demo", "zone", "cluster")] == nil ||
		api.ownerships[clusterKey("demo", "zone", "cluster")] == nil {
		t.Fatal("unavailable LRO removed metadata")
	}
	if data, err := os.ReadFile(final); err != nil || string(data) != "credential" {
		t.Fatalf("credential=%q err=%v", data, err)
	}
	intent, err := loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentDeletePending {
		t.Fatalf("intent=%#v err=%v", intent, err)
	}
}

func TestDeleteAvailabilityPreflightReturnsCanonicalUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		script string
	}{
		{name: "executable-disappeared"},
		{
			name: "docker-daemon-unavailable",
			script: "#!/bin/sh\nprintf '%s\n' " +
				"'Cannot connect to the Docker daemon. Is the docker daemon running?' >&2\nexit 1\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api, _, identity, _, final := preparePersistedOwnedClusterAPI(t)
			backend := api.GetBackend()
			backend.mu.Lock()
			backend.enabled = true
			backend.mu.Unlock()
			binDir := t.TempDir()
			if tc.script != "" {
				if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(tc.script), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("PATH", binDir)
			rec := httptest.NewRecorder()
			api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete,
				"/v1/projects/demo/zones/zone/clusters/cluster", nil))
			if rec.Code != http.StatusServiceUnavailable ||
				!strings.Contains(rec.Body.String(), `"status":"UNAVAILABLE"`) {
				t.Fatalf("response=%d %s", rec.Code, rec.Body.String())
			}
			if api.clusters[clusterKey("demo", "zone", "cluster")] == nil ||
				api.ownerships[clusterKey("demo", "zone", "cluster")] == nil {
				t.Fatal("availability failure removed metadata")
			}
			if data, err := os.ReadFile(final); err != nil || string(data) != "credential" {
				t.Fatalf("credential=%q err=%v", data, err)
			}
			intent, err := loadKubeconfigIntent(identity)
			if err != nil || intent.Phase != intentDeletePending {
				t.Fatalf("intent=%#v err=%v", intent, err)
			}
		})
	}
}

func TestSemanticKindDeleteFailureIsNotUnavailable(t *testing.T) {
	api, _, identity, ownership, final := preparePersistedOwnedClusterAPI(t)
	backend := api.GetBackend()
	backend.mu.Lock()
	backend.enabled = true
	backend.mu.Unlock()
	binDir := t.TempDir()
	script := `#!/bin/sh
if [ "$1" = get ]; then printf '%s\n' "` + ownership.BackendName + `"; exit 0; fi
printf '%s\n' 'invalid cluster state' >&2
exit 1
`
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete,
		"/v1/projects/demo/zones/zone/clusters/cluster", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("response=%d %s", rec.Code, rec.Body.String())
	}
	var initial GkeOperation
	if err := json.Unmarshal(rec.Body.Bytes(), &initial); err != nil {
		t.Fatal(err)
	}
	op := waitGKEOperation(t, api.opMgr, initial.Name)
	if op.Error == nil || op.Error.Code == http.StatusServiceUnavailable {
		t.Fatalf("operation=%#v", op)
	}
	intent, err := loadKubeconfigIntent(identity)
	if err != nil || intent.Phase != intentCommitted {
		t.Fatalf("semantic failure intent=%#v err=%v", intent, err)
	}
	if data, err := os.ReadFile(final); err != nil || string(data) != "credential" {
		t.Fatalf("credential=%q err=%v", data, err)
	}
}

func TestBackendAvailabilityClassifierExcludesPermissionAndCancellation(t *testing.T) {
	binDir := t.TempDir()
	script := "#!/bin/sh\nprintf '%s\n' 'permission denied' >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(binDir, "kind"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	if _, err := runKindCommand(t.Context(), "get", "clusters"); err == nil ||
		isBackendUnavailable(err) {
		t.Fatalf("permission error misclassified: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := runKindCommand(ctx, "get", "clusters"); !errors.Is(err, context.Canceled) ||
		isBackendUnavailable(err) {
		t.Fatalf("cancellation misclassified: %v", err)
	}
}

func preparePersistedOwnedClusterAPI(
	t *testing.T,
) (*API, *state.Store, ClusterIdentity, *kubeconfigOwnership, string) {
	t.Helper()
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "disabled-delete")
	identity := ClusterIdentity{
		Profile: "disabled-delete", Project: "demo", Zone: "zone", Cluster: "cluster",
	}
	final := kindKubeconfigPath(identity)
	target, ownership, err := prepareKubeconfigWithIntent(identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.file.Write([]byte("credential")); err != nil {
		t.Fatal(err)
	}
	if err := finalizeKubeconfigOwnership(target, ownership); err != nil {
		t.Fatal(err)
	}
	if err := securePublishKubeconfig(target, final); err != nil {
		t.Fatal(err)
	}
	if err := writeKubeconfigIntent(identity, ownership, intentCommitted); err != nil {
		t.Fatal(err)
	}
	store, err := state.New(config.GetStateDir(), identity.Profile)
	if err != nil {
		t.Fatal(err)
	}
	key := clusterKey(identity.Project, identity.Zone, identity.Cluster)
	metadata := checkedGKEMetadata(t, map[string]*Cluster{key: {
		Name: identity.Cluster, Location: identity.Zone, Status: "RUNNING",
		SelfLink: "https://container.googleapis.com/v1/projects/demo/zones/zone/clusters/cluster",
	}}, map[string]*kubeconfigOwnership{key: ownership})
	if err := store.Save(gkeStateEntry, metadata); err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(orchestrator.NewOperationManager(), store)
	if err != nil {
		t.Fatal(err)
	}
	return api, store, identity, ownership, final
}

func checkedGKEMetadata(
	t *testing.T,
	clusters map[string]*Cluster,
	ownerships map[string]*kubeconfigOwnership,
) gkeMetadata {
	t.Helper()
	checksum, err := kubeconfigOwnershipChecksum(ownerships)
	if err != nil {
		t.Fatal(err)
	}
	return gkeMetadata{
		Clusters: clusters, Ownerships: ownerships, OwnershipChecksum: checksum,
	}
}
