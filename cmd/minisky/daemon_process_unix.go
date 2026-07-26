//go:build !windows

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func daemonSignalContext(parent context.Context, _ string) (context.Context, context.CancelFunc, error) {
	ctx, cancel := signal.NotifyContext(parent, syscall.SIGTERM, syscall.SIGINT)
	return ctx, cancel, nil
}

func signalDaemon(identity daemonIdentity) error {
	if err := verifyDaemonProcess(identity); err != nil {
		return err
	}
	process, err := os.FindProcess(identity.PID)
	if err != nil {
		return err
	}
	return process.Signal(syscall.SIGTERM)
}

func waitForDaemonExit(identity daemonIdentity, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := syscall.Kill(identity.PID, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("check process %d: %w", identity.PID, err)
		}
		if currentToken, executable, identityErr := platformProcessIdentity(identity.PID); identityErr == nil &&
			(currentToken != identity.ProcessToken || executable != identity.Executable) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for PID %d", identity.PID)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
