//go:build linux

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func platformProcessIdentity(pid int) (string, string, error) {
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return "", "", fmt.Errorf("read process %d stat: %w", pid, err)
	}
	endCommand := strings.LastIndexByte(string(stat), ')')
	if endCommand < 0 || endCommand+2 >= len(stat) {
		return "", "", fmt.Errorf("process %d stat is malformed", pid)
	}
	fields := strings.Fields(string(stat[endCommand+2:]))
	// After the command, field 3 (state) is index 0 and field 22
	// (starttime, in clock ticks since boot) is index 19.
	if len(fields) <= 19 || fields[19] == "" {
		return "", "", fmt.Errorf("process %d stat has no start time", pid)
	}
	executableInfo, err := os.Stat("/proc/" + strconv.Itoa(pid) + "/exe")
	if err != nil {
		return "", "", fmt.Errorf("stat process %d executable: %w", pid, err)
	}
	executableStat, ok := executableInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return "", "", fmt.Errorf("process %d executable identity is unavailable", pid)
	}
	executable := strconv.FormatUint(uint64(executableStat.Dev), 10) + ":" +
		strconv.FormatUint(executableStat.Ino, 10)
	return fields[19], executable, nil
}
