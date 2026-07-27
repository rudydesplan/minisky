//go:build darwin

package main

import (
	"bytes"
	"fmt"
	"strconv"

	"golang.org/x/sys/unix"
)

func platformProcessIdentity(pid int) (string, string, error) {
	process, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", "", fmt.Errorf("query process %d: %w", pid, err)
	}
	if process == nil || process.Proc.P_pid != int32(pid) {
		return "", "", fmt.Errorf("process %d was not found", pid)
	}
	start := process.Proc.P_starttime
	token := strconv.FormatInt(start.Sec, 10) + ":" + strconv.FormatInt(int64(start.Usec), 10)
	executable := string(bytes.TrimRight(process.Proc.P_comm[:], "\x00"))
	if executable == "" {
		return "", "", fmt.Errorf("process %d has no executable identity", pid)
	}
	return token, executable, nil
}
