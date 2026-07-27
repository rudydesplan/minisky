//go:build windows

package main

import (
	"context"
	"testing"
	"time"
)

func TestWindowsDaemonControlEventSignalsShutdown(t *testing.T) {
	identity, err := newDaemonIdentity("windows")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cleanup, err := daemonSignalContext(context.Background(), identity.ControlToken)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err := signalDaemon(identity); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Windows daemon control event did not cancel context")
	}
}
