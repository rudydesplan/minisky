//go:build !windows

package main

import (
	"testing"
)

func TestDaemonIdentityDoesNotRequireExternalPS(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	identity, err := newDaemonIdentity("native-process")
	if err != nil {
		t.Fatalf("native process identity failed without ps on PATH: %v", err)
	}
	if err := verifyDaemonProcess(identity); err != nil {
		t.Fatal(err)
	}
}
