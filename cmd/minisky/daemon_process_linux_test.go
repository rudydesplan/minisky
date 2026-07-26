//go:build linux

package main

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLinuxProcessIdentityHelper(t *testing.T) {
	if os.Getenv("MINISKY_PROCESS_IDENTITY_HELPER") != "1" {
		return
	}
	time.Sleep(30 * time.Second)
}

func TestLinuxProcessIdentitySurvivesBinaryReplacement(t *testing.T) {
	source, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	daemonPath := filepath.Join(dir, "minisky-test-daemon")
	copyExecutable(t, source, daemonPath)

	command := exec.Command(daemonPath, "-test.run=^TestLinuxProcessIdentityHelper$")
	command.Env = append(os.Environ(), "MINISKY_PROCESS_IDENTITY_HELPER=1")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	}()

	beforeToken, beforeExecutable, err := platformProcessIdentity(command.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(dir, "replacement")
	copyExecutable(t, source, replacement)
	if err := os.Rename(replacement, daemonPath); err != nil {
		t.Fatal(err)
	}
	link, err := os.Readlink("/proc/" + strconv.Itoa(command.Process.Pid) + "/exe")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(link, " (deleted)") {
		t.Fatalf("replaced executable link = %q, want deleted suffix", link)
	}
	afterToken, afterExecutable, err := platformProcessIdentity(command.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if afterToken != beforeToken || afterExecutable != beforeExecutable {
		t.Fatalf(
			"process identity changed after replacement: before=(%q,%q) after=(%q,%q)",
			beforeToken,
			beforeExecutable,
			afterToken,
			afterExecutable,
		)
	}
}

func copyExecutable(t *testing.T, source, destination string) {
	t.Helper()
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(output, input); err != nil {
		output.Close()
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
}
