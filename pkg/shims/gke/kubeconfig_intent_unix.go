//go:build unix

package gke

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"minisky/pkg/config"

	"golang.org/x/sys/unix"
)

type kubeconfigIntentEnvelope struct {
	Intent   kubeconfigIntent `json:"intent"`
	Checksum string           `json:"checksum"`
}

func kubeconfigIntentPrefix(identity ClusterIdentity) (string, error) {
	name, err := kindBackendName(identity)
	if err != nil {
		return "", err
	}
	return ".intent-" + name, nil
}

func encodeKubeconfigIntent(intent kubeconfigIntent) ([]byte, error) {
	payload, err := json.Marshal(intent)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(payload)
	return json.Marshal(kubeconfigIntentEnvelope{Intent: intent, Checksum: hex.EncodeToString(sum[:])})
}

func decodeKubeconfigIntent(data []byte) (*kubeconfigIntent, error) {
	var envelope kubeconfigIntentEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(envelope.Intent)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(payload)
	if envelope.Checksum != hex.EncodeToString(sum[:]) || envelope.Intent.Ownership == nil {
		return nil, fmt.Errorf("invalid kubeconfig intent checksum")
	}
	return &envelope.Intent, nil
}

func readKubeconfigIntentSlot(dirfd int, name string) (*kubeconfigIntent, error) {
	fd, err := unix.Openat(dirfd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	data, readErr := io.ReadAll(io.LimitReader(file, 4097))
	closeErr := file.Close()
	if len(data) > 4096 {
		readErr = errors.Join(readErr, fmt.Errorf("kubeconfig intent exceeds 4096 bytes"))
	}
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	return decodeKubeconfigIntent(data)
}

func loadKubeconfigIntent(identity ClusterIdentity) (*kubeconfigIntent, error) {
	prefix, err := kubeconfigIntentPrefix(identity)
	if err != nil {
		return nil, err
	}
	dirfd, err := openKubeconfigDir(filepath.Dir(kindKubeconfigPath(identity)), false)
	if err != nil {
		return nil, err
	}
	defer unix.Close(dirfd)
	var latest *kubeconfigIntent
	for slot := 0; slot < 2; slot++ {
		intent, err := readKubeconfigIntentSlot(dirfd, fmt.Sprintf("%s.%d", prefix, slot))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			continue // the other checksum-valid slot remains authoritative
		}
		if intent.Ownership.matchesIdentity(identity) &&
			(latest == nil || intent.Generation > latest.Generation) {
			latest = intent
		}
	}
	if latest == nil {
		return nil, os.ErrNotExist
	}
	return latest, nil
}

func writeKubeconfigIntent(
	identity ClusterIdentity,
	ownership *kubeconfigOwnership,
	phase kubeconfigIntentPhase,
) error {
	return writeKubeconfigIntentError(identity, ownership, phase, "")
}

func writeKubeconfigIntentError(
	identity ClusterIdentity,
	ownership *kubeconfigOwnership,
	phase kubeconfigIntentPhase,
	phaseError string,
) error {
	return writeKubeconfigIntentErrorEvidence(identity, ownership, phase, phaseError, nil)
}

func writeKubeconfigIntentErrorEvidence(
	identity ClusterIdentity,
	ownership *kubeconfigOwnership,
	phase kubeconfigIntentPhase,
	phaseError string,
	unmatched *unmatchedKubeconfigQuarantine,
) error {
	kubeconfigLifecycleMu.Lock()
	defer kubeconfigLifecycleMu.Unlock()
	return writeKubeconfigIntentUnlocked(identity, ownership, phase, phaseError, unmatched)
}

func writeKubeconfigIntentUnlocked(
	identity ClusterIdentity,
	ownership *kubeconfigOwnership,
	phase kubeconfigIntentPhase,
	phaseError string,
	unmatched *unmatchedKubeconfigQuarantine,
) error {
	if testWriteKubeconfigIntent != nil {
		if err := testWriteKubeconfigIntent(phase); err != nil {
			return err
		}
	}
	if !ownership.matchesIdentity(identity) {
		return fmt.Errorf("kubeconfig intent ownership binding mismatch")
	}
	var generation uint64 = 1
	if previous, err := loadKubeconfigIntent(identity); err == nil {
		generation = previous.Generation + 1
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	intent := kubeconfigIntent{
		Generation: generation, Phase: phase, Ownership: ownership,
		UnmatchedQuarantine: unmatched, Error: phaseError,
	}
	data, err := encodeKubeconfigIntent(intent)
	if err != nil {
		return err
	}
	prefix, err := kubeconfigIntentPrefix(identity)
	if err != nil {
		return err
	}
	dirfd, err := openKubeconfigDir(filepath.Dir(kindKubeconfigPath(identity)), true)
	if err != nil {
		return err
	}
	name := fmt.Sprintf("%s.%d", prefix, generation%2)
	fd, err := unix.Openat(dirfd, name,
		unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return errors.Join(err, unix.Close(dirfd))
	}
	file := os.NewFile(uintptr(fd), name)
	info, statErr := file.Stat()
	if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return errors.Join(os.ErrPermission, statErr, file.Close(), unix.Close(dirfd))
	}
	writeErr := file.Truncate(0)
	if writeErr == nil {
		_, writeErr = file.WriteAt(data, 0)
	}
	return errors.Join(writeErr, file.Sync(), unix.Fsync(dirfd), file.Close(), unix.Close(dirfd))
}

func prepareKubeconfigWithIntent(
	identity ClusterIdentity,
) (*secureKubeconfigTarget, *kubeconfigOwnership, error) {
	kubeconfigLifecycleMu.Lock()
	defer kubeconfigLifecycleMu.Unlock()
	path := kindKubeconfigPath(identity)
	dirfd, err := openKubeconfigDir(filepath.Dir(path), true)
	if err != nil {
		return nil, nil, err
	}
	if err := checkKubeconfigEntryCapacity(dirfd); err != nil {
		return nil, nil, errors.Join(err, unix.Close(dirfd))
	}
	prefix, err := kubeconfigIntentPrefix(identity)
	if err != nil {
		return nil, nil, errors.Join(err, unix.Close(dirfd))
	}
	if err := checkKubeconfigIntentCapacity(dirfd, prefix); err != nil {
		return nil, nil, errors.Join(err, unix.Close(dirfd))
	}
	if err := unix.Close(dirfd); err != nil {
		return nil, nil, err
	}
	target, err := securePrepareKubeconfigUnlocked(path)
	if err != nil {
		return nil, nil, err
	}
	ownership, err := kubeconfigOwnershipFromTarget(identity, target)
	if err != nil {
		return nil, nil, errors.Join(err, secureDiscardKubeconfigUnlocked(target))
	}
	if err := writeKubeconfigIntentUnlocked(identity, ownership, intentPrepared, "", nil); err != nil {
		return nil, nil, errors.Join(err, secureDiscardKubeconfigUnlocked(target))
	}
	return target, ownership, nil
}

func checkKubeconfigIntentCapacity(dirfd int, ownPrefix string) error {
	duplicate, err := unix.Dup(dirfd)
	if err != nil {
		return err
	}
	dir := os.NewFile(uintptr(duplicate), "kubeconfig-intent-capacity")
	names, readErr := dir.Readdirnames(-1)
	closeErr := dir.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return err
	}
	count := 0
	ownSlots := 0
	for _, name := range names {
		if strings.HasPrefix(name, ".intent-") &&
			(strings.HasSuffix(name, ".0") || strings.HasSuffix(name, ".1")) {
			count++
			if strings.HasPrefix(name, ownPrefix+".") {
				ownSlots++
			}
		}
	}
	if count+2-ownSlots > maxKubeconfigIntentSlots {
		return fmt.Errorf("kubeconfig intent slot limit reached: count=%d cap=%d",
			count, maxKubeconfigIntentSlots)
	}
	return nil
}

func loadAllKubeconfigIntents(profile string) ([]kubeconfigIntent, error) {
	dir := filepath.Join(config.GetStateDir(), "profiles", profile, "runtime", "gke")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	var intents []kubeconfigIntent
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, ".intent-") || !(strings.HasSuffix(name, ".0") || strings.HasSuffix(name, ".1")) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		intent, err := decodeKubeconfigIntent(data)
		if err != nil || intent.Ownership.Profile != profile {
			continue
		}
		key := clusterKey(intent.Ownership.Project, intent.Ownership.Zone, intent.Ownership.Cluster)
		if _, ok := seen[key]; ok {
			continue
		}
		identity := ClusterIdentity{
			Profile: profile, Project: intent.Ownership.Project,
			Zone: intent.Ownership.Zone, Cluster: intent.Ownership.Cluster,
		}
		latest, err := loadKubeconfigIntent(identity)
		if err == nil {
			intents = append(intents, *latest)
			seen[key] = struct{}{}
		}
	}
	return intents, nil
}
