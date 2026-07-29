package state

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxLocalMarkerSize = 4 << 10

// ReadLocalMarker reads a small profile-local marker through pinned,
// no-follow directory handles. Missing namespaces and markers are reported as
// found=false.
func (s *Store) ReadLocalMarker(namespace, name string) ([]byte, bool, error) {
	if err := validateLocalMarkerLocation(namespace, name); err != nil {
		return nil, false, err
	}
	if !supportsPinnedOwnership() {
		return nil, false, fmt.Errorf("%w for local marker reads on this platform", ErrSafeOwnershipUnsupported)
	}
	var (
		payload []byte
		found   bool
	)
	err := s.withStateLock(false, func(profileDir *pinnedDirectory) error {
		if profileDir == nil {
			return nil
		}
		markerDir, err := profileDir.openDirectory(namespace, false)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("pin local marker directory: %w", err)
		}
		defer markerDir.close()
		file, err := markerDir.openRegularFileForRead(name)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("open local marker: %w", err)
		}
		defer file.Close()
		payload, err = io.ReadAll(io.LimitReader(file, maxLocalMarkerSize+1))
		if err != nil {
			return fmt.Errorf("read local marker: %w", err)
		}
		if len(payload) > maxLocalMarkerSize {
			return fmt.Errorf("%w: local marker exceeds %d bytes", ErrInvalidPath, maxLocalMarkerSize)
		}
		if err := markerDir.sameFileAt(file, name); err != nil {
			return fmt.Errorf("verify local marker identity: %w", err)
		}
		if err := markerDir.samePath(); err != nil {
			return fmt.Errorf("verify local marker directory: %w", err)
		}
		found = true
		return nil
	})
	return payload, found, err
}

// WriteLocalMarker atomically writes a small profile-local marker through a
// pinned directory and verifies both the committed file and directory identity
// before reporting success.
func (s *Store) WriteLocalMarker(namespace, name string, payload []byte) error {
	if err := validateLocalMarkerLocation(namespace, name); err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > maxLocalMarkerSize {
		return fmt.Errorf("%w: local marker payload size %d", ErrInvalidPath, len(payload))
	}
	if !supportsPinnedOwnership() {
		return fmt.Errorf("%w for local marker writes on this platform", ErrSafeOwnershipUnsupported)
	}
	return s.withMutationLock(func(profileDir *pinnedDirectory) error {
		if profileDir == nil {
			return fmt.Errorf("%w: local marker profile is unavailable", ErrInvalidPath)
		}
		markerDir, err := profileDir.openDirectory(namespace, true)
		if err != nil {
			return fmt.Errorf("pin local marker directory: %w", err)
		}
		defer markerDir.close()
		file, tempName, err := markerDir.createTemp(".marker-")
		if err != nil {
			return fmt.Errorf("create local marker temporary file: %w", err)
		}
		defer markerDir.remove(tempName)
		defer file.Close()
		if _, err := file.Write(payload); err != nil {
			return fmt.Errorf("write local marker: %w", err)
		}
		if err := file.Sync(); err != nil {
			return fmt.Errorf("sync local marker: %w", err)
		}
		if s.beforeLocalMarkerCommit != nil {
			s.beforeLocalMarkerCommit()
		}
		if err := markerDir.samePath(); err != nil {
			return fmt.Errorf("verify local marker directory before commit: %w", err)
		}
		if err := markerDir.rename(tempName, name); err != nil {
			return fmt.Errorf("commit local marker: %w", err)
		}
		if s.afterLocalMarkerCommit != nil {
			s.afterLocalMarkerCommit()
		}
		if err := markerDir.sameFileAt(file, name); err != nil {
			return fmt.Errorf("verify committed local marker: %w", err)
		}
		if err := markerDir.sync(); err != nil {
			return fmt.Errorf("sync local marker directory: %w", err)
		}
		if err := markerDir.samePath(); err != nil {
			return fmt.Errorf("verify local marker directory after commit: %w", err)
		}
		return nil
	})
}

// RemoveLocalMarker removes name only when its pinned, no-follow contents
// exactly match expected. A different generation is never removed.
func (s *Store) RemoveLocalMarker(namespace, name string, expected []byte) error {
	if err := validateLocalMarkerLocation(namespace, name); err != nil {
		return err
	}
	if len(expected) == 0 || len(expected) > maxLocalMarkerSize {
		return fmt.Errorf("%w: local marker expected payload size %d", ErrInvalidPath, len(expected))
	}
	if !supportsPinnedOwnership() {
		return fmt.Errorf("%w for local marker removal on this platform", ErrSafeOwnershipUnsupported)
	}
	return s.withMutationLock(func(profileDir *pinnedDirectory) error {
		if profileDir == nil {
			return nil
		}
		markerDir, err := profileDir.openDirectory(namespace, false)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("pin local marker directory: %w", err)
		}
		defer markerDir.close()
		file, err := markerDir.openRegularFileForRemoval(name)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("open local marker for removal: %w", err)
		}
		defer file.Close()
		payload, err := io.ReadAll(io.LimitReader(file, maxLocalMarkerSize+1))
		if err != nil {
			return fmt.Errorf("read local marker for removal: %w", err)
		}
		if !bytes.Equal(payload, expected) {
			return ErrMarkerMismatch
		}
		if err := markerDir.sameFileAt(file, name); err != nil {
			return fmt.Errorf("verify local marker before removal: %w", err)
		}
		if err := markerDir.samePath(); err != nil {
			return fmt.Errorf("verify local marker directory before removal: %w", err)
		}
		if err := markerDir.removeOpenFile(file, name); err != nil {
			return fmt.Errorf("remove local marker: %w", err)
		}
		if err := markerDir.sync(); err != nil {
			return fmt.Errorf("sync local marker removal: %w", err)
		}
		if err := markerDir.samePath(); err != nil {
			return fmt.Errorf("verify local marker directory after removal: %w", err)
		}
		return nil
	})
}

func validateLocalMarkerLocation(namespace, name string) error {
	for _, component := range []string{namespace, name} {
		if component == "" || component == "." || component == ".." ||
			filepath.Base(component) != component ||
			strings.ContainsAny(component, `/\`) {
			return fmt.Errorf("%w: invalid local marker component %q", ErrInvalidPath, component)
		}
	}
	return nil
}
