package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/state"

	"github.com/spf13/cobra"
)

var restartCmd = &cobra.Command{
	Use:   "restart",
	Short: "Restarts the MiniSky Daemon",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("🔄 Restarting MiniSky...")
		runtime, err := readDaemonRuntime(config.GetProfileDir())
		if err != nil {
			return fmt.Errorf("preserve daemon configuration: %w", err)
		}
		identity, err := runningDaemonIdentity(config.GetStateDir(), config.GetProfile())
		if err != nil {
			return fmt.Errorf("authenticate daemon: %w", err)
		}
		if err := signalDaemon(identity); err != nil {
			return fmt.Errorf("stop authenticated daemon PID %d: %w", identity.PID, err)
		}
		if err := waitForDaemonExit(identity, 15*time.Second); err != nil {
			return fmt.Errorf("wait for daemon exit: %w", err)
		}
		if err := waitForProfileRelease(config.GetStateDir(), config.GetProfile(), 15*time.Second); err != nil {
			return fmt.Errorf("wait for profile lock release: %w", err)
		}
		executable, err := os.Executable()
		if err != nil {
			return fmt.Errorf("locate MiniSky executable: %w", err)
		}
		newCmd := exec.Command(executable, runtime.Args...)
		newCmd.Env = mergeDaemonEnvironment(os.Environ(), runtime.Environment)
		newCmd.Dir = runtime.Directory
		newCmd.Stdout = os.Stdout
		newCmd.Stderr = os.Stderr

		fmt.Println("🚀 Starting new instance...")
		if err := newCmd.Start(); err != nil {
			return fmt.Errorf("restart MiniSky: %w", err)
		}
		childExit := newChildExitState()
		go func() {
			childExit.complete(newCmd.Wait())
		}()
		listeners, readinessClient, err := daemonReadinessTargets(runtime, config.GetProfileDir())
		if err != nil {
			cleanupErr := cleanupReplacementProcess(newCmd.Process, childExit)
			return errors.Join(fmt.Errorf("resolve replacement listeners: %w", err), cleanupErr)
		}
		readyCtx, cancelReady := context.WithTimeout(cmd.Context(), 15*time.Second)
		defer cancelReady()
		err = waitForReplacementDaemon(
			readyCtx,
			newCmd.Process.Pid,
			config.GetProfile(),
			listeners,
			childExit,
			func() (daemonIdentity, error) {
				return readDaemonIdentity(config.GetProfileDir())
			},
			verifyDaemonProcess,
			func(target daemonReadinessTarget, controlToken string) error {
				return probeDaemonReadiness(readinessClient, target, controlToken)
			},
			50*time.Millisecond,
		)
		if err != nil {
			cleanupErr := cleanupReplacementProcess(newCmd.Process, childExit)
			return errors.Join(fmt.Errorf("replacement daemon readiness: %w", err), cleanupErr)
		}

		fmt.Printf("✅ MiniSky restarted (PID: %d)\n", newCmd.Process.Pid)
		fmt.Println("Note: This process is now running in the background.")
		return nil
	},
}

type childExitState struct {
	done chan struct{}
	once sync.Once
	mu   sync.RWMutex
	err  error
}

func newChildExitState() *childExitState {
	return &childExitState{done: make(chan struct{})}
}

func (state *childExitState) complete(err error) {
	state.once.Do(func() {
		state.mu.Lock()
		state.err = err
		state.mu.Unlock()
		close(state.done)
	})
}

func (state *childExitState) result() error {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.err
}

func cleanupReplacementProcess(process *os.Process, childExit *childExitState) error {
	killErr := process.Kill()
	if errors.Is(killErr, os.ErrProcessDone) {
		killErr = nil
	}
	select {
	case <-childExit.done:
		return killErr
	case <-time.After(5 * time.Second):
		return errors.Join(killErr, errors.New("timeout waiting for failed replacement process cleanup"))
	}
}

type daemonReadinessTarget struct {
	Role string
	URL  string
}

func daemonReadinessTargets(runtime daemonRuntime, profileDir string) ([]daemonReadinessTarget, *http.Client, error) {
	values := map[string]string{
		"bind":            "127.0.0.1",
		"port":            "8080",
		"ui-port":         "8081",
		"tls":             "",
		"tls-cert":        "",
		"tls-client-ca":   "",
		"tls-client-cert": "",
		"tls-client-key":  "",
	}
	for environment, flag := range map[string]string{
		"MINISKY_BIND":            "bind",
		"MINISKY_PORT":            "port",
		"MINISKY_UI_PORT":         "ui-port",
		"MINISKY_TLS_MODE":        "tls",
		"MINISKY_TLS_CERT":        "tls-cert",
		"MINISKY_TLS_CLIENT_CA":   "tls-client-ca",
		"MINISKY_TLS_CLIENT_CERT": "tls-client-cert",
		"MINISKY_TLS_CLIENT_KEY":  "tls-client-key",
	} {
		if value := strings.TrimSpace(runtime.Environment[environment]); value != "" {
			values[flag] = value
		}
	}
	for index := 0; index < len(runtime.Args); index++ {
		argument := runtime.Args[index]
		for _, flag := range []string{
			"bind", "port", "ui-port", "tls", "tls-cert", "tls-client-ca", "tls-client-cert", "tls-client-key",
		} {
			prefix := "--" + flag + "="
			if strings.HasPrefix(argument, prefix) {
				values[flag] = strings.TrimSpace(strings.TrimPrefix(argument, prefix))
				continue
			}
			if argument == "--"+flag {
				if index+1 >= len(runtime.Args) {
					return nil, nil, fmt.Errorf("%s requires a value", argument)
				}
				index++
				values[flag] = strings.TrimSpace(runtime.Args[index])
			}
		}
	}
	for _, flag := range []string{"bind", "port", "ui-port"} {
		if values[flag] == "" {
			return nil, nil, fmt.Errorf("--%s cannot be empty", flag)
		}
	}
	probeHost := values["bind"]
	if ip := net.ParseIP(probeHost); ip != nil && ip.IsUnspecified() {
		probeHost = "127.0.0.1"
	}
	scheme := "http"
	client := &http.Client{Timeout: time.Second}
	tlsEnabled := values["tls"] != "" || values["tls-cert"] != "" || values["tls-client-ca"] != ""
	if tlsEnabled {
		scheme = "https"
		certificateFile := values["tls-cert"]
		if values["tls"] == "auto" {
			certificateFile = filepath.Join(profileDir, "tls", "server-cert.pem")
		}
		serverTLS := &tls.Config{MinVersion: tls.VersionTLS12}
		if values["tls-client-ca"] != "" {
			serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
		}
		tlsClient, err := gatewayLoopbackClient(
			serverTLS,
			certificateFile,
			values["tls-client-cert"],
			values["tls-client-key"],
		)
		if err != nil {
			return nil, nil, err
		}
		tlsClient.Timeout = time.Second
		client = tlsClient
	}
	return []daemonReadinessTarget{
		{Role: "ui", URL: scheme + "://" + net.JoinHostPort("127.0.0.1", values["ui-port"])},
		{Role: "api", URL: scheme + "://" + net.JoinHostPort(probeHost, values["port"])},
	}, client, nil
}

func probeDaemonReadiness(client *http.Client, target daemonReadinessTarget, controlToken string) error {
	nonceBytes := make([]byte, 32)
	if _, err := rand.Read(nonceBytes); err != nil {
		return err
	}
	nonce := hex.EncodeToString(nonceBytes)
	request, err := http.NewRequest(http.MethodGet, target.URL+"/_minisky/control/readiness", nil)
	if err != nil {
		return err
	}
	request.Header.Set("X-MiniSky-Readiness-Nonce", nonce)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("readiness returned HTTP %d", response.StatusCode)
	}
	return verifyDaemonReadinessProof(
		target.Role,
		controlToken,
		nonce,
		response.Header.Get("X-MiniSky-Readiness-Proof"),
	)
}

func verifyDaemonReadinessProof(role, controlToken, nonce, encodedProof string) error {
	proof, err := hex.DecodeString(encodedProof)
	if err != nil {
		return errors.New("readiness proof is malformed")
	}
	expected := hmac.New(sha256.New, []byte(controlToken))
	_, _ = expected.Write([]byte(role + ":" + nonce))
	if !hmac.Equal(proof, expected.Sum(nil)) {
		return errors.New("readiness proof did not authenticate replacement listener")
	}
	return nil
}

func waitForReplacementDaemon(
	ctx context.Context,
	expectedPID int,
	profile string,
	listeners []daemonReadinessTarget,
	childExit *childExitState,
	readIdentity func() (daemonIdentity, error),
	verifyIdentity func(daemonIdentity) error,
	probe func(daemonReadinessTarget, string) error,
	pollInterval time.Duration,
) error {
	if len(listeners) == 0 {
		return errors.New("replacement daemon has no configured listeners")
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-childExit.done:
			childErr := childExit.result()
			if childErr == nil {
				childErr = errors.New("process exited")
			}
			return fmt.Errorf("replacement process exited before becoming ready: %w", childErr)
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			identity, err := readIdentity()
			if err != nil || identity.PID != expectedPID || identity.Profile != profile {
				continue
			}
			if err := verifyIdentity(identity); err != nil {
				continue
			}
			ready := true
			for _, listener := range listeners {
				if err := probe(listener, identity.ControlToken); err != nil {
					ready = false
					break
				}
			}
			if ready {
				return nil
			}
		}
	}
}

type daemonRuntime struct {
	Args        []string          `json:"args"`
	Environment map[string]string `json:"environment,omitempty"`
	Directory   string            `json:"directory,omitempty"`
}

func daemonRuntimePath(profileDir string) string {
	return filepath.Join(profileDir, "runtime-config.json")
}

func writeDaemonRuntime(profileDir string, runtime daemonRuntime) error {
	if !containsStartCommand(runtime.Args) {
		return errors.New("daemon runtime must contain the start command")
	}
	payload, err := json.Marshal(runtime)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(profileDir, ".runtime-config-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, daemonRuntimePath(profileDir))
}

func readDaemonRuntime(profileDir string) (daemonRuntime, error) {
	var runtime daemonRuntime
	payload, err := os.ReadFile(daemonRuntimePath(profileDir))
	if err != nil {
		return runtime, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&runtime); err != nil {
		return runtime, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return runtime, errors.New("invalid trailing runtime configuration")
	}
	if !containsStartCommand(runtime.Args) {
		return runtime, errors.New("invalid saved daemon command")
	}
	return runtime, nil
}

func containsStartCommand(args []string) bool {
	for _, arg := range args {
		if arg == "start" {
			return true
		}
	}
	return false
}

func currentDaemonRuntime() daemonRuntime {
	environment := make(map[string]string)
	for _, value := range os.Environ() {
		name, contents, found := strings.Cut(value, "=")
		if found && strings.HasPrefix(name, "MINISKY_") {
			environment[name] = contents
		}
	}
	directory, _ := os.Getwd()
	return daemonRuntime{
		Args:        append([]string(nil), os.Args[1:]...),
		Environment: environment,
		Directory:   directory,
	}
}

func mergeDaemonEnvironment(current []string, saved map[string]string) []string {
	merged := make(map[string]string, len(current)+len(saved))
	for _, value := range current {
		name, contents, found := strings.Cut(value, "=")
		if found {
			merged[name] = contents
		}
	}
	for name, contents := range saved {
		merged[name] = contents
	}
	result := make([]string, 0, len(merged))
	for name, contents := range merged {
		result = append(result, name+"="+contents)
	}
	return result
}

func waitForProfileRelease(root, profile string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		store, err := state.New(root, profile)
		if err != nil {
			return err
		}
		ownership, err := store.AcquireOwnership()
		if err == nil {
			return ownership.Close()
		}
		if !errors.Is(err, state.ErrProfileInUse) {
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("timeout waiting for profile lock")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func init() {
	rootCmd.AddCommand(restartCmd)
}
