package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"minisky/pkg/state"
)

const daemonIdentityVersion = 1

type daemonIdentity struct {
	Version      int    `json:"version"`
	PID          int    `json:"pid"`
	Profile      string `json:"profile"`
	ProcessToken string `json:"processToken"`
	Executable   string `json:"executable"`
	ControlToken string `json:"controlToken"`
}

func daemonIdentityPath(profileDir string) string {
	return filepath.Join(profileDir, "daemon.json")
}

func newDaemonIdentity(profile string) (daemonIdentity, error) {
	processToken, executable, err := platformProcessIdentity(os.Getpid())
	if err != nil {
		return daemonIdentity{}, fmt.Errorf("capture daemon process identity: %w", err)
	}
	controlBytes := make([]byte, 32)
	if _, err := rand.Read(controlBytes); err != nil {
		return daemonIdentity{}, fmt.Errorf("generate daemon control token: %w", err)
	}
	identity := daemonIdentity{
		Version:      daemonIdentityVersion,
		PID:          os.Getpid(),
		Profile:      profile,
		ProcessToken: processToken,
		Executable:   executable,
		ControlToken: hex.EncodeToString(controlBytes),
	}
	if err := validateDaemonIdentity(profile, identity); err != nil {
		return daemonIdentity{}, err
	}
	return identity, nil
}

func writeDaemonIdentity(profileDir string, identity daemonIdentity) error {
	if err := validateDaemonIdentity(identity.Profile, identity); err != nil {
		return err
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(profileDir, ".daemon-identity-*")
	if err != nil {
		return fmt.Errorf("create daemon identity: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return fmt.Errorf("secure daemon identity: %w", err)
	}
	if _, err := temp.Write(payload); err != nil {
		temp.Close()
		return fmt.Errorf("write daemon identity: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return fmt.Errorf("sync daemon identity: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close daemon identity: %w", err)
	}
	if err := os.Rename(tempPath, daemonIdentityPath(profileDir)); err != nil {
		return fmt.Errorf("replace daemon identity: %w", err)
	}
	directory, err := os.Open(profileDir)
	if err != nil {
		return fmt.Errorf("open daemon identity directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync daemon identity directory: %w", err)
	}
	return nil
}

func readDaemonIdentity(profileDir string) (daemonIdentity, error) {
	var identity daemonIdentity
	path := daemonIdentityPath(profileDir)
	info, err := os.Lstat(path)
	if err != nil {
		return identity, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return identity, errors.New("daemon identity must be a private regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return identity, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return identity, fmt.Errorf("decode daemon identity: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return identity, errors.New("daemon identity contains trailing data")
	}
	if err := validateDaemonIdentity(identity.Profile, identity); err != nil {
		return identity, err
	}
	return identity, nil
}

func validateDaemonIdentity(expectedProfile string, identity daemonIdentity) error {
	if identity.Version != daemonIdentityVersion {
		return fmt.Errorf("unsupported daemon identity version %d", identity.Version)
	}
	if identity.PID <= 1 {
		return fmt.Errorf("unsafe daemon PID %d", identity.PID)
	}
	if strings.TrimSpace(expectedProfile) == "" || identity.Profile != expectedProfile {
		return fmt.Errorf("daemon profile %q does not match expected profile %q", identity.Profile, expectedProfile)
	}
	if identity.ProcessToken == "" || identity.Executable == "" || len(identity.ControlToken) != 64 {
		return errors.New("daemon identity is incomplete")
	}
	if _, err := hex.DecodeString(identity.ControlToken); err != nil {
		return errors.New("daemon control token is invalid")
	}
	return nil
}

func verifyDaemonProcess(identity daemonIdentity) error {
	processToken, executable, err := platformProcessIdentity(identity.PID)
	if err != nil {
		return err
	}
	if processToken != identity.ProcessToken || executable != identity.Executable {
		return errors.New("PID belongs to a different process")
	}
	return nil
}

func removeDaemonIdentity(profileDir string, expected daemonIdentity) error {
	current, err := readDaemonIdentity(profileDir)
	if err != nil {
		return err
	}
	if current != expected {
		return errors.New("daemon identity was replaced")
	}
	return removeDaemonIdentityFile(profileDir)
}

func removeDaemonIdentityFile(profileDir string) error {
	if err := os.Remove(daemonIdentityPath(profileDir)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	directory, err := os.Open(profileDir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func clearInactiveDaemonIdentity(store *state.Store, afterRemoval func()) error {
	ownership, err := store.AcquireOwnership()
	if err != nil {
		return err
	}
	return clearDaemonIdentityWithOwnership(store.ProfileDir(), ownership, afterRemoval)
}

func clearDaemonIdentityWithOwnership(
	profileDir string,
	ownership *state.Ownership,
	afterRemoval func(),
) error {
	removeErr := removeDaemonIdentityFile(profileDir)
	if afterRemoval != nil {
		afterRemoval()
	}
	closeErr := ownership.Close()
	return errors.Join(removeErr, closeErr)
}

func runningDaemonIdentity(root, profile string) (daemonIdentity, error) {
	store, err := state.New(root, profile)
	if err != nil {
		return daemonIdentity{}, err
	}
	ownership, err := store.AcquireOwnership()
	if err == nil {
		clearErr := clearDaemonIdentityWithOwnership(store.ProfileDir(), ownership, nil)
		return daemonIdentity{}, errors.Join(errors.New("profile is not owned by a running daemon"), clearErr)
	}
	if !errors.Is(err, state.ErrProfileInUse) {
		return daemonIdentity{}, err
	}
	identity, err := readDaemonIdentity(store.ProfileDir())
	if err != nil {
		return daemonIdentity{}, err
	}
	if err := validateDaemonIdentity(profile, identity); err != nil {
		return daemonIdentity{}, err
	}
	if identity.PID == os.Getpid() {
		return daemonIdentity{}, errors.New("refusing to control current process")
	}
	if err := verifyDaemonProcess(identity); err != nil {
		return daemonIdentity{}, fmt.Errorf("authenticate daemon PID %d: %w", identity.PID, err)
	}
	return identity, nil
}
