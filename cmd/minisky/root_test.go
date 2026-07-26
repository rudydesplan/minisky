package main

import (
	"bytes"
	"errors"
	"minisky/pkg/state"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestCLIHTTPClientHasTimeout(t *testing.T) {
	if cliHTTPClient.Timeout <= 0 {
		t.Fatal("CLI HTTP client has no request timeout")
	}
}

func TestDaemonRuntimeRoundTripPreservesSecurityConfiguration(t *testing.T) {
	profileDir := t.TempDir()
	want := daemonRuntime{
		Args: []string{"start", "--tls=files", "--tls-cert=/tmp/cert", "--audit-strict", "--services=compute"},
		Environment: map[string]string{
			"MINISKY_PROFILE":    "secure",
			"MINISKY_AUTH_TOKEN": "secret",
		},
	}
	if err := writeDaemonRuntime(profileDir, want); err != nil {
		t.Fatal(err)
	}
	got, err := readDaemonRuntime(profileDir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime = %#v, want %#v", got, want)
	}
	info, err := os.Stat(daemonRuntimePath(profileDir))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("runtime config mode = %04o, want 0600", info.Mode().Perm())
	}
}

func TestDaemonIdentityIsProfileScoped(t *testing.T) {
	first := daemonIdentityPath(filepath.Join("/tmp", "state", "profiles", "one"))
	second := daemonIdentityPath(filepath.Join("/tmp", "state", "profiles", "two"))
	if first == second || filepath.Dir(first) == filepath.Dir(second) {
		t.Fatalf("profile identity paths collide: %q and %q", first, second)
	}
}

func TestDaemonIdentityRoundTripAndProcessAuthentication(t *testing.T) {
	profileDir := t.TempDir()
	identity, err := newDaemonIdentity("secure")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeDaemonIdentity(profileDir, identity); err != nil {
		t.Fatal(err)
	}
	got, err := readDaemonIdentity(profileDir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, identity) {
		t.Fatalf("identity = %#v, want %#v", got, identity)
	}
	if err := verifyDaemonProcess(got); err != nil {
		t.Fatalf("self identity did not authenticate: %v", err)
	}
	reused := got
	reused.ProcessToken = "reused-process"
	if err := verifyDaemonProcess(reused); err == nil {
		t.Fatal("reused PID identity was accepted")
	}
	info, err := os.Stat(daemonIdentityPath(profileDir))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode = %04o, want 0600", info.Mode().Perm())
	}
}

func TestDaemonIdentityRejectsCorruptOrWrongProfile(t *testing.T) {
	profileDir := t.TempDir()
	if err := os.WriteFile(daemonIdentityPath(profileDir), []byte(`{"pid":123}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readDaemonIdentity(profileDir); err == nil {
		t.Fatal("corrupt identity was accepted")
	}
	identity, err := newDaemonIdentity("one")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateDaemonIdentity("two", identity); err == nil {
		t.Fatal("wrong-profile daemon identity was accepted")
	}
}

func TestRunningDaemonIdentityRejectsStaleUnlockedPID(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	store, err := state.New(root, "stale")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	if err := ownership.Close(); err != nil {
		t.Fatal(err)
	}
	pidPath := daemonIdentityPath(store.ProfileDir())
	if err := os.WriteFile(pidPath, []byte("424242"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runningDaemonIdentity(root, "stale"); err == nil {
		t.Fatal("stale unlocked PID was accepted")
	}
	if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale PID file still exists: %v", err)
	}
}

func TestRunningDaemonIdentityRejectsCorruptAndReusedActivePID(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	store, err := state.New(root, "active")
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := store.AcquireOwnership()
	if err != nil {
		t.Fatal(err)
	}
	defer ownership.Close()

	path := daemonIdentityPath(store.ProfileDir())
	if err := os.WriteFile(path, []byte(`{"pid":424242}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runningDaemonIdentity(root, "active"); err == nil {
		t.Fatal("corrupt active daemon identity was accepted")
	}

	identity, err := newDaemonIdentity("active")
	if err != nil {
		t.Fatal(err)
	}
	identity.ProcessToken = "reused-process"
	if err := writeDaemonIdentity(store.ProfileDir(), identity); err != nil {
		t.Fatal(err)
	}
	if _, err := runningDaemonIdentity(root, "active"); err == nil {
		t.Fatal("reused active PID was accepted")
	}
}

func TestStopAllProfileDaemonsCoversEveryInactiveProfile(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	for _, profile := range []string{"one", "two"} {
		store, err := state.New(root, profile)
		if err != nil {
			t.Fatal(err)
		}
		ownership, err := store.AcquireOwnership()
		if err != nil {
			t.Fatal(err)
		}
		if err := ownership.Close(); err != nil {
			t.Fatal(err)
		}
		if err := store.Save("test/value", profile); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(daemonIdentityPath(store.ProfileDir()), []byte("424242"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	profiles, err := stopAllProfileDaemons(root, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(profiles)
	if !reflect.DeepEqual(profiles, []string{"one", "two"}) {
		t.Fatalf("profiles = %#v", profiles)
	}
}

func TestRemoveSelectedStateDeletesOnlyManagedAndCustomRoots(t *testing.T) {
	parent := t.TempDir()
	custom := filepath.Join(parent, "custom-state")
	managed := filepath.Join(parent, ".minisky")
	sibling := filepath.Join(parent, "keep")
	for _, path := range []string{custom, managed, sibling} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeSelectedState(custom, managed); err != nil {
		t.Fatal(err)
	}
	for _, removed := range []string{custom, managed} {
		if _, err := os.Stat(removed); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("selected path %q still exists: %v", removed, err)
		}
	}
	if _, err := os.Stat(sibling); err != nil {
		t.Fatalf("unrelated sibling was removed: %v", err)
	}
	if err := removeSelectedState(string(os.PathSeparator), managed); err == nil {
		t.Fatal("broad state removal path was accepted")
	}
	if err := removeSelectedState(parent, managed); err == nil {
		t.Fatal("state root containing managed directory was accepted")
	}
	symlink := filepath.Join(parent, "linked-state")
	if err := os.Symlink(sibling, symlink); err == nil {
		if err := removeSelectedState(symlink, managed); err == nil {
			t.Fatal("symlinked custom state root was accepted")
		}
	}
}

func TestMiniSkyAPIURLUsesCanonicalRoute(t *testing.T) {
	t.Setenv("MINISKY_ENDPOINT", "http://127.0.0.1:9090/base/")
	miniskyEndpoint = ""

	got, err := miniskyAPIURL("cloudbuild", "/v1/projects/local-dev-project/builds")
	if err != nil {
		t.Fatalf("miniskyAPIURL returned an error: %v", err)
	}
	want := "http://127.0.0.1:9090/base/_minisky/cloudbuild/v1/projects/local-dev-project/builds"
	if got != want {
		t.Fatalf("URL = %q, want %q", got, want)
	}
}

func TestMiniSkyAPIURLFlagOverridesEnvironment(t *testing.T) {
	t.Setenv("MINISKY_ENDPOINT", "http://127.0.0.1:9090")
	miniskyEndpoint = "http://127.0.0.1:9191/"
	t.Cleanup(func() { miniskyEndpoint = "" })

	got, err := miniskyAPIURL("secretmanager", "v1/projects/test/secrets")
	if err != nil {
		t.Fatalf("miniskyAPIURL returned an error: %v", err)
	}
	if want := "http://127.0.0.1:9191/_minisky/secretmanager/v1/projects/test/secrets"; got != want {
		t.Fatalf("URL = %q, want %q", got, want)
	}
}

func TestMiniSkyAPIURLRejectsInvalidEndpoint(t *testing.T) {
	t.Setenv("MINISKY_ENDPOINT", "localhost:8080")
	miniskyEndpoint = ""

	if _, err := miniskyAPIURL("storage", "/storage/v1/b"); err == nil {
		t.Fatal("miniskyAPIURL accepted an endpoint without a scheme")
	}
}

func TestCanonicalURLAndStatusHandlingWithHTTPTestServer(t *testing.T) {
	var requestedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		http.Error(w, "gateway unavailable", http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)
	t.Setenv("MINISKY_ENDPOINT", server.URL)
	miniskyEndpoint = ""

	endpoint, err := miniskyAPIURL("artifactregistry", "/v1/projects/p/locations/l/repositories")
	if err != nil {
		t.Fatalf("miniskyAPIURL returned an error: %v", err)
	}
	err = getJSON(endpoint, &struct{}{})
	if err == nil || !strings.Contains(err.Error(), "502 Bad Gateway") {
		t.Fatalf("error = %v, want status error", err)
	}
	if want := "/_minisky/artifactregistry/v1/projects/p/locations/l/repositories"; requestedPath != want {
		t.Fatalf("request path = %q, want %q", requestedPath, want)
	}
}

func TestPostJSONUsesPOSTAndDecodesResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if contentType := r.Header.Get("Content-Type"); contentType != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", contentType)
		}
		w.Write([]byte(`{"entries":[{"insertId":"1"}]}`))
	}))
	t.Cleanup(server.Close)

	var response struct {
		Entries []struct {
			InsertID string `json:"insertId"`
		} `json:"entries"`
	}
	if err := postJSON(server.URL, map[string]interface{}{}, &response); err != nil {
		t.Fatalf("postJSON returned an error: %v", err)
	}
	if len(response.Entries) != 1 || response.Entries[0].InsertID != "1" {
		t.Fatalf("response = %#v, want one log entry", response)
	}
}

func TestUnsupportedHeadlessCommandMessage(t *testing.T) {
	var stderr bytes.Buffer
	command := &cobra.Command{}
	command.SetErr(&stderr)

	unsupportedHeadlessCommand(command, "Spanner")
	if got := stderr.String(); !strings.Contains(got, "does not expose a public MiniSky shim route") {
		t.Fatalf("message = %q, want unsupported shim explanation", got)
	}
}

func TestGetJSON(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"name":"resource-1"}`))
	}))
	t.Cleanup(server.Close)

	var response struct {
		Name string `json:"name"`
	}
	if err := getJSON(server.URL, &response); err != nil {
		t.Fatalf("getJSON returned an error: %v", err)
	}
	if response.Name != "resource-1" {
		t.Fatalf("name = %q, want resource-1", response.Name)
	}
}

func TestGetJSONRejectsErrorStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)

	err := getJSON(server.URL, &struct{}{})
	if err == nil || !strings.Contains(err.Error(), "503 Service Unavailable") {
		t.Fatalf("error = %v, want status error", err)
	}
}

func TestGetJSONRejectsInvalidResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`not-json`))
	}))
	t.Cleanup(server.Close)

	if err := getJSON(server.URL, &struct{}{}); err == nil {
		t.Fatal("getJSON accepted an invalid JSON response")
	}
}
