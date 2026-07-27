package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func secureRemoveAll(path string) error {
	return secureRemoveAllWithHook(path, nil)
}

func secureRemoveAllWithHook(path string, afterOpen func()) error {
	return secureRemoveAllWithHooks(path, nil, afterOpen)
}

func secureRemoveAllWithAncestryHook(path string, beforeOpen func(string)) error {
	return secureRemoveAllWithHooks(path, beforeOpen, nil)
}

func secureRemoveAllWithHooks(path string, beforeOpen func(string), afterOpen func()) error {
	path, err := canonicalSecureRemovalPath(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	name := filepath.Base(path)
	parent, err := openPinnedRemovalParent(path, beforeOpen)
	if err != nil {
		return err
	}
	defer parent.Close()
	before, err := parent.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect removal target %q: %w", path, err)
	}
	if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("removal target %q is not a real directory", path)
	}
	target, err := parent.OpenRoot(name)
	if err != nil {
		return fmt.Errorf("pin removal target %q: %w", path, err)
	}
	defer target.Close()
	opened, err := target.Stat(".")
	if err != nil {
		return fmt.Errorf("inspect pinned removal target %q: %w", path, err)
	}
	after, err := parent.Lstat(name)
	if err != nil {
		return fmt.Errorf("revalidate opened removal target %q: %w", path, err)
	}
	if after.Mode()&os.ModeSymlink != 0 || !os.SameFile(before, opened) || !os.SameFile(opened, after) {
		return fmt.Errorf("removal target %q changed while opening", path)
	}
	if afterOpen != nil {
		afterOpen()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := removeRootContents(ctx, target); err != nil {
		return fmt.Errorf("clear removal target %q: %w", path, err)
	}
	current, err := parent.Lstat(name)
	if err != nil {
		return fmt.Errorf("revalidate removal target %q: %w", path, err)
	}
	if !os.SameFile(opened, current) {
		return fmt.Errorf("removal target %q was replaced", path)
	}
	if err := parent.Remove(name); err != nil {
		return fmt.Errorf("remove pinned target %q: %w", path, err)
	}
	return nil
}

func openPinnedRemovalParent(path string, beforeOpen func(string)) (*os.Root, error) {
	rootPath := filepath.VolumeName(path) + string(os.PathSeparator)
	for _, candidate := range []string{os.TempDir(), userHomeDirectory()} {
		if candidate == "" {
			continue
		}
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			continue
		}
		resolved, err = filepath.Abs(resolved)
		if err != nil {
			continue
		}
		resolved = filepath.Clean(resolved)
		if path != resolved && strings.HasPrefix(path, resolved+string(os.PathSeparator)) &&
			len(resolved) > len(rootPath) {
			rootPath = resolved
		}
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf("pin trusted removal root %q: %w", rootPath, err)
	}
	parentRelative, err := filepath.Rel(rootPath, filepath.Dir(path))
	if err != nil {
		root.Close()
		return nil, err
	}
	if parentRelative == "." {
		return root, nil
	}
	for _, component := range splitPathComponents(parentRelative) {
		before, err := root.Lstat(component)
		if err != nil {
			root.Close()
			return nil, fmt.Errorf("inspect removal ancestor %q: %w", component, err)
		}
		if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
			root.Close()
			return nil, fmt.Errorf("removal ancestor %q is not a real directory", component)
		}
		if beforeOpen != nil {
			beforeOpen(component)
		}
		next, err := root.OpenRoot(component)
		if err != nil {
			root.Close()
			return nil, fmt.Errorf("pin removal ancestor %q: %w", component, err)
		}
		opened, openedErr := next.Stat(".")
		after, afterErr := root.Lstat(component)
		if openedErr != nil || afterErr != nil || after.Mode()&os.ModeSymlink != 0 ||
			!os.SameFile(before, opened) || !os.SameFile(opened, after) {
			next.Close()
			root.Close()
			return nil, fmt.Errorf("removal ancestor %q changed while opening", component)
		}
		if err := root.Close(); err != nil {
			next.Close()
			return nil, err
		}
		root = next
	}
	return root, nil
}

func splitPathComponents(path string) []string {
	return strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\'
	})
}

func removeRootContents(ctx context.Context, root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := directory.ReadDir(128)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			name := entry.Name()
			info, statErr := root.Lstat(name)
			if errors.Is(statErr, os.ErrNotExist) {
				continue
			}
			if statErr != nil {
				return statErr
			}
			if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
				child, openErr := root.OpenRoot(name)
				if openErr != nil {
					return openErr
				}
				opened, statErr := child.Stat(".")
				if statErr == nil {
					statErr = removeRootContents(ctx, child)
				}
				if statErr == nil {
					current, currentErr := root.Lstat(name)
					if currentErr != nil {
						statErr = currentErr
					} else if !os.SameFile(opened, current) {
						statErr = fmt.Errorf("directory %q was replaced", name)
					}
				}
				if statErr == nil {
					statErr = root.Remove(name)
				}
				closeErr := child.Close()
				if err := errors.Join(statErr, closeErr); err != nil {
					return err
				}
				continue
			}
			if err := root.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}

func canonicalSecureRemovalPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("path is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	absolute = filepath.Clean(absolute)
	for _, trustedRoot := range []string{os.TempDir(), userHomeDirectory()} {
		if trustedRoot == "" {
			continue
		}
		trustedRoot, err = filepath.Abs(trustedRoot)
		if err != nil {
			continue
		}
		trustedRoot = filepath.Clean(trustedRoot)
		if absolute != trustedRoot && !strings.HasPrefix(absolute, trustedRoot+string(os.PathSeparator)) {
			continue
		}
		resolvedRoot, resolveErr := filepath.EvalSymlinks(trustedRoot)
		if resolveErr != nil {
			return "", resolveErr
		}
		relative, relativeErr := filepath.Rel(trustedRoot, absolute)
		if relativeErr != nil {
			return "", relativeErr
		}
		absolute = filepath.Join(resolvedRoot, relative)
		break
	}
	return absolute, nil
}

func userHomeDirectory() string {
	home, _ := os.UserHomeDir()
	return home
}
