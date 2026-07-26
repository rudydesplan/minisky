package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	localsecurity "minisky/pkg/security"
)

func TestDaemonListenerTargetsPreserveConfiguredAddresses(t *testing.T) {
	runtime := daemonRuntime{
		Args: []string{"start", "--bind=0.0.0.0", "--port", "9443", "--ui-port=9444", "--audit-strict"},
		Environment: map[string]string{
			"MINISKY_BIND":    "127.0.0.2",
			"MINISKY_PORT":    "8088",
			"MINISKY_UI_PORT": "8089",
		},
	}
	got, client, err := daemonReadinessTargets(runtime, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if client == nil {
		t.Fatal("readiness client is nil")
	}
	want := []daemonReadinessTarget{
		{Role: "ui", URL: "http://127.0.0.1:9444"},
		{Role: "api", URL: "http://127.0.0.1:9443"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("listeners = %#v, want %#v", got, want)
	}
}

func TestDaemonReadinessTargetsPreserveAutoTLS(t *testing.T) {
	profileDir := t.TempDir()
	if _, _, err := localsecurity.PrepareTLS(localsecurity.TLSOptions{
		Mode:       localsecurity.TLSAuto,
		ProfileDir: profileDir,
		ServerName: "localhost",
	}); err != nil {
		t.Fatal(err)
	}
	targets, client, err := daemonReadinessTargets(daemonRuntime{
		Args: []string{"start", "--tls=auto", "--port=9443", "--ui-port=9444"},
	}, profileDir)
	if err != nil {
		t.Fatal(err)
	}
	if client == nil || len(targets) != 2 {
		t.Fatalf("client=%#v targets=%#v", client, targets)
	}
	for _, target := range targets {
		if !strings.HasPrefix(target.URL, "https://") {
			t.Fatalf("TLS readiness target = %q", target.URL)
		}
	}
}

func TestWaitForReplacementDaemonRequiresIdentityAndBothListeners(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	identityReady := false
	ready := map[string]bool{}
	checks := 0
	childExit := newChildExitState()
	err := waitForReplacementDaemon(ctx, 42, "secure", []daemonReadinessTarget{
		{Role: "api"}, {Role: "ui"},
	}, childExit, func() (daemonIdentity, error) {
		if !identityReady {
			identityReady = true
			return daemonIdentity{}, errors.New("not written")
		}
		return daemonIdentity{PID: 42, Profile: "secure", ControlToken: strings.Repeat("a", 64)}, nil
	}, func(identity daemonIdentity) error {
		if identity.PID != 42 {
			return errors.New("wrong process")
		}
		return nil
	}, func(target daemonReadinessTarget, controlToken string) error {
		if controlToken != strings.Repeat("a", 64) {
			return errors.New("wrong control token")
		}
		checks++
		if checks >= 3 {
			ready["api"] = true
		}
		if checks >= 5 {
			ready["ui"] = true
		}
		if !ready[target.Role] {
			return errors.New("not ready")
		}
		return nil
	}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !identityReady || !ready["api"] || !ready["ui"] {
		t.Fatalf("identityReady=%t listeners=%v", identityReady, ready)
	}
}

func TestWaitForReplacementDaemonFailsWhenChildExitsFirst(t *testing.T) {
	childExit := newChildExitState()
	childExit.complete(errors.New("exit status 2"))
	err := waitForReplacementDaemon(context.Background(), 42, "secure", []daemonReadinessTarget{
		{Role: "api"}, {Role: "ui"},
	}, childExit,
		func() (daemonIdentity, error) { return daemonIdentity{}, errors.New("not written") },
		func(daemonIdentity) error { return nil },
		func(daemonReadinessTarget, string) error { return errors.New("not ready") },
		time.Millisecond,
	)
	if err == nil || !strings.Contains(err.Error(), "exited before becoming ready") {
		t.Fatalf("error = %v", err)
	}
}

func TestChildExitStateSharesSingleWaitResult(t *testing.T) {
	exit := newChildExitState()
	sentinel := errors.New("exit status 7")
	exit.complete(sentinel)
	exit.complete(errors.New("must be ignored"))
	<-exit.done
	if !errors.Is(exit.result(), sentinel) || !errors.Is(exit.result(), sentinel) {
		t.Fatalf("shared child result = %v", exit.result())
	}
}

func TestReadinessProbeRejectsUnrelatedListener(t *testing.T) {
	unrelated := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer unrelated.Close()
	err := probeDaemonReadiness(unrelated.Client(), daemonReadinessTarget{Role: "api", URL: unrelated.URL},
		strings.Repeat("a", 64))
	if err == nil || !strings.Contains(err.Error(), "authenticate") {
		t.Fatalf("unrelated listener error = %v", err)
	}
}
