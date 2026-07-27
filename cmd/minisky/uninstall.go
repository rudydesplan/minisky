package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/orchestrator"
	"minisky/pkg/state"

	"github.com/spf13/cobra"
)

var uninstallCmd = &cobra.Command{
	Use:   "uninstall",
	Short: "Uninstalls MiniSky (removes containers, networks, and data)",
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Println("🛑 Uninstalling MiniSky...")

		stateRoot := config.GetStateDir()
		miniskyDir := config.GetMiniskyDir()
		profiles, err := stopAllProfileDaemons(stateRoot, 15*time.Second)
		if err != nil {
			return fmt.Errorf("stop profile daemons: %w", err)
		}
		log.Printf("Stopped %d profile daemon(s); sweeping exact MiniSky Docker ownership labels...", len(profiles))
		svcMgr, err := orchestrator.NewServiceManager()
		if err != nil {
			return fmt.Errorf("connect to Docker for uninstall cleanup: %w", err)
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		cleanupErr := svcMgr.CleanupAllProfiles(cleanupCtx)
		cancel()
		if cleanupErr != nil {
			return fmt.Errorf("clean MiniSky Docker resources: %w", cleanupErr)
		}

		if err := removeSelectedState(stateRoot, miniskyDir); err != nil {
			return err
		}
		log.Println("✅ Data directory removed.")

		exe, err := os.Executable()
		if err == nil {
			log.Printf("✨ Uninstall complete! You can now safely delete the executable at: %s", exe)
		} else {
			log.Println("✨ Uninstall complete! You can now safely delete the minisky binary.")
		}
		return nil
	},
}

func stopAllProfileDaemons(root string, timeout time.Duration) ([]string, error) {
	profilesDir := filepath.Join(root, "profiles")
	entries, err := os.ReadDir(profilesDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	profiles := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		profile := entry.Name()
		store, err := state.New(root, profile)
		if err != nil {
			return nil, err
		}
		ownership, err := store.AcquireOwnership()
		if err == nil {
			if err := clearDaemonIdentityWithOwnership(store.ProfileDir(), ownership, nil); err != nil {
				return nil, fmt.Errorf("clear inactive profile %s daemon identity: %w", profile, err)
			}
			profiles = append(profiles, profile)
			continue
		}
		if !errors.Is(err, state.ErrProfileInUse) {
			return nil, err
		}
		identity, err := runningDaemonIdentity(root, profile)
		if err != nil {
			return nil, fmt.Errorf("profile %s has no authenticated daemon identity: %w", profile, err)
		}
		if err := signalDaemon(identity); err != nil {
			return nil, fmt.Errorf("stop profile %s: %w", profile, err)
		}
		if err := waitForDaemonExit(identity, timeout); err != nil {
			return nil, fmt.Errorf("wait for profile %s process: %w", profile, err)
		}
		if err := waitForProfileRelease(root, profile, timeout); err != nil {
			return nil, fmt.Errorf("wait for profile %s: %w", profile, err)
		}
		profiles = append(profiles, profile)
	}
	return profiles, nil
}

func removeSelectedState(stateRoot, miniskyDir string) error {
	stateRoot, err := safeRemovalPath(stateRoot)
	if err != nil {
		return fmt.Errorf("unsafe state root: %w", err)
	}
	miniskyDir, err = safeRemovalPath(miniskyDir)
	if err != nil {
		return fmt.Errorf("unsafe MiniSky directory: %w", err)
	}
	if stateRoot != miniskyDir && strings.HasPrefix(miniskyDir, stateRoot+string(os.PathSeparator)) {
		return fmt.Errorf("refusing to remove state root %q because it contains managed directory %q", stateRoot, miniskyDir)
	}
	if stateRoot != miniskyDir && !strings.HasPrefix(stateRoot, miniskyDir+string(os.PathSeparator)) {
		if err := secureRemoveAll(stateRoot); err != nil {
			return fmt.Errorf("remove custom state root %s: %w", stateRoot, err)
		}
	}
	if err := secureRemoveAll(miniskyDir); err != nil {
		return fmt.Errorf("remove MiniSky directory %s: %w", miniskyDir, err)
	}
	return nil
}

func safeRemovalPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("path is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	absolute = filepath.Clean(absolute)
	home, _ := os.UserHomeDir()
	volumeRoot := filepath.VolumeName(absolute) + string(os.PathSeparator)
	if absolute == volumeRoot || absolute == "." || absolute == filepath.Clean(home) {
		return "", fmt.Errorf("refusing broad removal path %q", absolute)
	}
	relative := strings.TrimPrefix(absolute, volumeRoot)
	components := strings.FieldsFunc(relative, func(r rune) bool {
		return r == '/' || r == '\\'
	})
	if len(components) < 2 {
		return "", fmt.Errorf("refusing shallow removal path %q", absolute)
	}
	if info, err := os.Lstat(absolute); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("refusing symlinked removal path %q", absolute)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return absolute, nil
}

func init() {
	rootCmd.AddCommand(uninstallCmd)
}
