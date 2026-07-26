//go:build windows

package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"golang.org/x/sys/windows"
)

func platformProcessIdentity(pid int) (string, string, error) {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return "", "", fmt.Errorf("open process %d: %w", pid, err)
	}
	defer windows.CloseHandle(handle)
	var creation, exit, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(handle, &creation, &exit, &kernel, &user); err != nil {
		return "", "", fmt.Errorf("read process %d creation time: %w", pid, err)
	}
	buffer := make([]uint16, 32768)
	size := uint32(len(buffer))
	if err := windows.QueryFullProcessImageName(handle, 0, &buffer[0], &size); err != nil {
		return "", "", fmt.Errorf("read process %d executable: %w", pid, err)
	}
	return strconv.FormatInt(creation.Nanoseconds(), 10), windows.UTF16ToString(buffer[:size]), nil
}

func daemonControlEventName(controlToken string) string {
	return `Local\MiniSky-` + controlToken
}

func daemonSignalContext(parent context.Context, controlToken string) (context.Context, context.CancelFunc, error) {
	name, err := windows.UTF16PtrFromString(daemonControlEventName(controlToken))
	if err != nil {
		return nil, nil, err
	}
	event, err := windows.CreateEvent(nil, 1, 0, name)
	if err != nil {
		return nil, nil, fmt.Errorf("create daemon shutdown event: %w", err)
	}
	ctx, cancel := context.WithCancel(parent)
	go func() {
		if result, waitErr := windows.WaitForSingleObject(event, windows.INFINITE); waitErr == nil &&
			result == windows.WAIT_OBJECT_0 {
			cancel()
		}
	}()
	cleanup := func() {
		cancel()
		_ = windows.SetEvent(event)
		_ = windows.CloseHandle(event)
	}
	return ctx, cleanup, nil
}

func signalDaemon(identity daemonIdentity) error {
	if err := verifyDaemonProcess(identity); err != nil {
		return err
	}
	name, err := windows.UTF16PtrFromString(daemonControlEventName(identity.ControlToken))
	if err != nil {
		return err
	}
	event, err := windows.OpenEvent(windows.EVENT_MODIFY_STATE, false, name)
	if err != nil {
		return fmt.Errorf("open daemon shutdown event: %w", err)
	}
	defer windows.CloseHandle(event)
	if err := windows.SetEvent(event); err != nil {
		return fmt.Errorf("signal daemon shutdown event: %w", err)
	}
	return nil
}

func waitForDaemonExit(identity daemonIdentity, timeout time.Duration) error {
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(identity.PID))
	if err != nil {
		if err == windows.ERROR_INVALID_PARAMETER {
			return nil
		}
		return fmt.Errorf("open process %d for exit wait: %w", identity.PID, err)
	}
	defer windows.CloseHandle(handle)
	milliseconds := timeout.Milliseconds()
	if milliseconds < 0 {
		milliseconds = 0
	}
	if milliseconds > int64(^uint32(0)-1) {
		milliseconds = int64(^uint32(0) - 1)
	}
	result, err := windows.WaitForSingleObject(handle, uint32(milliseconds))
	if err != nil {
		return fmt.Errorf("wait for process %d: %w", identity.PID, err)
	}
	if result != windows.WAIT_OBJECT_0 {
		return fmt.Errorf("timeout waiting for PID %d", identity.PID)
	}
	return nil
}
