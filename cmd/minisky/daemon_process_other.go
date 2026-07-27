//go:build !windows && !linux && !darwin

package main

import "fmt"

func platformProcessIdentity(pid int) (string, string, error) {
	return "", "", fmt.Errorf("native process identity is unsupported for PID %d on this platform", pid)
}
