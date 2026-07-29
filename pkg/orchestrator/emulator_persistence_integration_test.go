package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
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
	"testing"
	"time"

	"minisky/pkg/config"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	pubsubapi "google.golang.org/api/pubsub/v1"
	storageapi "google.golang.org/api/storage/v1"
)

func TestStoragePersistenceAndPubSubSessionBoundaries(t *testing.T) {
	if os.Getenv("MINISKY_DOCKER_EMULATOR_BOUNDARY_INTEGRATION") != "1" {
		t.Skip("set MINISKY_DOCKER_EMULATOR_BOUNDARY_INTEGRATION=1 to run")
	}
	binary := os.Getenv("MINISKY_EMULATOR_BOUNDARY_BINARY")
	stateDir := os.Getenv("MINISKY_EMULATOR_BOUNDARY_STATE_DIR")
	profile := os.Getenv("MINISKY_EMULATOR_BOUNDARY_PROFILE")
	if binary == "" || stateDir == "" || profile == "" {
		t.Fatal("MINISKY_EMULATOR_BOUNDARY_BINARY, MINISKY_EMULATOR_BOUNDARY_STATE_DIR, and MINISKY_EMULATOR_BOUNDARY_PROFILE are required")
	}
	storageImage := os.Getenv("MINISKY_STORAGE_TEST_IMAGE")
	pubsubImage := os.Getenv("MINISKY_PUBSUB_TEST_IMAGE")
	if !isPinnedEmulatorImage(storageImage) || !isPinnedEmulatorImage(pubsubImage) {
		t.Fatal("MINISKY_STORAGE_TEST_IMAGE and MINISKY_PUBSUB_TEST_IMAGE must be image@sha256:<64 hex>")
	}

	registry := config.GetImageRegistry()
	if got := registry.Emulators["storage.googleapis.com"].Image; got != storageImage {
		t.Fatalf("embedded Storage image = %q, want required pin %q", got, storageImage)
	}
	if got := registry.Emulators["pubsub.googleapis.com"].Image; got != pubsubImage {
		t.Fatalf("embedded Pub/Sub image = %q, want required pin %q", got, pubsubImage)
	}

	t.Setenv("MINISKY_STATE_DIR", stateDir)
	t.Setenv("MINISKY_PROFILE", profile)
	storageConfig := config.EmulatorConfig{
		Name: "minisky-gcs", Image: storageImage, Port: "4443/tcp",
		Cmd: []string{"-scheme", "http"},
	}
	pubsubConfig := config.EmulatorConfig{
		Name: "minisky-pubsub", Image: pubsubImage, Port: "8085/tcp",
		Cmd: []string{"gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085"},
	}

	manager, err := NewServiceManager()
	if err != nil {
		t.Fatal(err)
	}
	ping, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost/_ping", nil)
	if err != nil {
		t.Fatal(err)
	}
	pingResponse, err := manager.doDocker(ping)
	if err != nil {
		t.Fatalf("Docker daemon unavailable after guarded integration opt-in: %v", err)
	}
	pingResponse.Body.Close()
	if pingResponse.StatusCode != http.StatusOK {
		t.Fatalf("Docker daemon ping returned %d after guarded integration opt-in", pingResponse.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	isolationProfile := profile + "-isolated"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, cleanupProfile := range []string{profile, isolationProfile} {
			if err := os.Setenv("MINISKY_PROFILE", cleanupProfile); err != nil {
				t.Errorf("select cleanup profile %s: %v", cleanupProfile, err)
				continue
			}
			for domain, emulator := range map[string]config.EmulatorConfig{
				"storage.googleapis.com": storageConfig,
				"pubsub.googleapis.com":  pubsubConfig,
			} {
				if err := manager.removeDurableEmulatorContainer(cleanupCtx, domain, emulator); err != nil {
					t.Errorf("cleanup %s emulator for %s: %v", domain, cleanupProfile, err)
				}
			}
		}
	})

	runHash := sha256.Sum256([]byte(profile))
	suffix := hex.EncodeToString(runHash[:6])
	project := "restart-project-" + suffix
	bucket := "restart-bucket-" + suffix
	object := "restart-object-" + suffix + ".txt"
	objectBody := "storage survives restart " + suffix
	topic := "restart-topic-" + suffix
	subscription := "restart-subscription-" + suffix
	messageBody := "pubsub survives restart " + suffix
	replacementBoundaryBody := "pubsub is lost with emulator replacement " + suffix

	first := startDurabilityDaemon(t, binary, stateDir, profile)
	storageClient := newDurabilityStorageClient(t, ctx, first.gateway)
	pubsubClient := newDurabilityPubSubClient(t, ctx, first.gateway)
	if _, err := storageClient.Buckets.Insert(project, &storageapi.Bucket{Name: bucket}).Context(ctx).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := storageClient.Objects.Insert(bucket, &storageapi.Object{Name: object}).
		Media(strings.NewReader(objectBody)).Context(ctx).Do(); err != nil {
		t.Fatal(err)
	}
	topicName := "projects/" + project + "/topics/" + topic
	subscriptionName := "projects/" + project + "/subscriptions/" + subscription
	encodedMessage := base64.StdEncoding.EncodeToString([]byte(messageBody))
	if _, err := pubsubClient.Projects.Topics.Create(topicName, &pubsubapi.Topic{}).Context(ctx).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := pubsubClient.Projects.Subscriptions.Create(subscriptionName,
		&pubsubapi.Subscription{Topic: topicName}).Context(ctx).Do(); err != nil {
		t.Fatal(err)
	}
	if _, err := pubsubClient.Projects.Topics.Publish(topicName, &pubsubapi.PublishRequest{
		Messages: []*pubsubapi.PubsubMessage{{Data: encodedMessage}},
	}).Context(ctx).Do(); err != nil {
		t.Fatal(err)
	}
	pubsubContainerID := durableEmulatorContainerID(t, manager, ctx, "pubsub.googleapis.com", pubsubConfig)

	if err := first.crash(); err != nil {
		t.Fatalf("abruptly stop first daemon while retaining emulator containers: %v", err)
	}
	if afterCrash := durableEmulatorContainerID(t, manager, ctx, "pubsub.googleapis.com", pubsubConfig); afterCrash != pubsubContainerID {
		t.Fatalf("Pub/Sub emulator container changed across daemon process stop: got %q, want %q", afterCrash, pubsubContainerID)
	}

	snapshot := filepath.Join(stateDir, "metadata-export.json")
	export := exec.CommandContext(ctx, binary, "state", "--profile", profile, "export", snapshot)
	export.Env = durabilityProcessEnvironment(stateDir, profile, filepath.Join(stateDir, "home-"+profile))
	if output, err := export.CombinedOutput(); err != nil {
		t.Fatalf("export profile metadata: %v\n%s", err, output)
	}
	exported, err := os.ReadFile(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, runtimePayload := range []string{objectBody, encodedMessage} {
		if strings.Contains(string(exported), runtimePayload) {
			t.Errorf("portable metadata export contains emulator runtime payload %q", runtimePayload)
		}
	}

	sessionRestart := startDurabilityDaemon(t, binary, stateDir, profile)
	if afterRestart := durableEmulatorContainerID(t, manager, ctx, "pubsub.googleapis.com", pubsubConfig); afterRestart != pubsubContainerID {
		t.Fatalf("Pub/Sub emulator container changed across daemon process restart: got %q, want %q", afterRestart, pubsubContainerID)
	}
	sessionStorage := newDurabilityStorageClient(t, ctx, sessionRestart.gateway)
	sessionPubSub := newDurabilityPubSubClient(t, ctx, sessionRestart.gateway)
	if _, err := sessionStorage.Buckets.Get(bucket).Context(ctx).Do(); err != nil {
		t.Fatalf("Storage metadata did not survive MiniSky daemon restart: %v", err)
	}
	if _, err := sessionPubSub.Projects.Topics.Get(topicName).Context(ctx).Do(); err != nil {
		t.Fatalf("Pub/Sub topic did not survive MiniSky daemon restart with emulator alive: %v", err)
	}
	if _, err := sessionPubSub.Projects.Subscriptions.Get(subscriptionName).Context(ctx).Do(); err != nil {
		t.Fatalf("Pub/Sub subscription did not survive MiniSky daemon restart with emulator alive: %v", err)
	}
	pulled, err := sessionPubSub.Projects.Subscriptions.Pull(subscriptionName,
		&pubsubapi.PullRequest{MaxMessages: 1}).Context(ctx).Do()
	if err != nil {
		t.Fatalf("pull Pub/Sub message after MiniSky daemon restart: %v", err)
	}
	if len(pulled.ReceivedMessages) != 1 ||
		pulled.ReceivedMessages[0].Message.Data != encodedMessage {
		t.Fatalf("Pub/Sub session-continuity response = %#v", pulled.ReceivedMessages)
	}
	replacementBoundaryMessage := base64.StdEncoding.EncodeToString([]byte(replacementBoundaryBody))
	if _, err := sessionPubSub.Projects.Topics.Publish(topicName, &pubsubapi.PublishRequest{
		Messages: []*pubsubapi.PubsubMessage{{Data: replacementBoundaryMessage}},
	}).Context(ctx).Do(); err != nil {
		t.Fatalf("publish replacement-boundary message: %v", err)
	}
	if err := sessionRestart.crash(); err != nil {
		t.Fatalf("abruptly stop session-continuity daemon while retaining emulator containers: %v", err)
	}
	t.Log("BOUNDARY: Pub/Sub topic, subscription, and message continuity survives MiniSky process restart only when the exact emulator container remains alive")

	if err := os.Setenv("MINISKY_PROFILE", profile); err != nil {
		t.Fatal(err)
	}
	runtimePaths := make(map[string]string)
	for domain, emulator := range map[string]config.EmulatorConfig{
		"storage.googleapis.com": storageConfig,
		"pubsub.googleapis.com":  pubsubConfig,
	} {
		container := mustDurableEmulatorConfig(t, domain, emulator)
		runtimePath := strings.TrimSuffix(container.Volume, ":/data")
		if info, err := os.Stat(runtimePath); err != nil || !info.IsDir() {
			t.Fatalf("%s runtime directory before container recreation: %v", domain, err)
		}
		if domain == "pubsub.googleapis.com" {
			entries, err := os.ReadDir(runtimePath)
			if err != nil {
				t.Fatalf("read Pub/Sub runtime directory: %v", err)
			}
			if len(entries) != 1 || entries[0].Name() != "env.yaml" {
				t.Fatalf("Pub/Sub --data-dir contents = %v, want configuration-only [env.yaml]", entryNames(entries))
			}
			t.Log("BOUNDARY: Pub/Sub --data-dir stores env.yaml launch configuration, not topics, subscriptions, or messages")
		}
		runtimePaths[domain] = runtimePath
		if err := manager.removeDurableEmulatorContainer(ctx, domain, emulator); err != nil {
			t.Fatalf("remove primary %s emulator: %v", domain, err)
		}
		if info, err := os.Stat(runtimePath); err != nil || !info.IsDir() {
			t.Fatalf("%s runtime directory was not retained: %v", domain, err)
		}
	}
	if err := manager.Teardown(ctx); err != nil {
		t.Fatalf("remove exact-owned primary Docker network after emulator replacement: %v", err)
	}

	if err := os.Setenv("MINISKY_PROFILE", isolationProfile); err != nil {
		t.Fatal(err)
	}
	isolated := startDurabilityDaemon(t, binary, stateDir, isolationProfile)
	isolatedStorage := newDurabilityStorageClient(t, ctx, isolated.gateway)
	isolatedPubSub := newDurabilityPubSubClient(t, ctx, isolated.gateway)
	_, err = isolatedStorage.Buckets.Get(bucket).Context(ctx).Do()
	assertGoogleAPINotFound(t, err)
	_, err = isolatedStorage.Objects.Get(bucket, object).Context(ctx).Do()
	assertGoogleAPINotFound(t, err)
	_, err = isolatedPubSub.Projects.Topics.Get(topicName).Context(ctx).Do()
	assertGoogleAPINotFound(t, err)
	_, err = isolatedPubSub.Projects.Subscriptions.Get(subscriptionName).Context(ctx).Do()
	assertGoogleAPINotFound(t, err)
	if err := isolated.stop(); err != nil {
		t.Fatalf("stop isolated daemon: %v", err)
	}

	if err := os.Setenv("MINISKY_PROFILE", profile); err != nil {
		t.Fatal(err)
	}
	restarted := startDurabilityDaemon(t, binary, stateDir, profile)
	storageClient = newDurabilityStorageClient(t, ctx, restarted.gateway)
	pubsubClient = newDurabilityPubSubClient(t, ctx, restarted.gateway)
	if _, err := storageClient.Buckets.Get(bucket).Context(ctx).Do(); err != nil {
		t.Fatal(err)
	}
	objectResponse, err := storageClient.Objects.Get(bucket, object).Context(ctx).Download()
	if err != nil {
		t.Fatal(err)
	}
	objectPayload, err := io.ReadAll(io.LimitReader(objectResponse.Body, 1<<20))
	closeErr := objectResponse.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if string(objectPayload) != objectBody {
		t.Fatalf("object after restart = %q, want %q", objectPayload, objectBody)
	}
	_, err = pubsubClient.Projects.Topics.Get(topicName).Context(ctx).Do()
	assertGoogleAPINotFound(t, err)
	_, err = pubsubClient.Projects.Subscriptions.Get(subscriptionName).Context(ctx).Do()
	assertGoogleAPINotFound(t, err)
	if _, err := pubsubClient.Projects.Topics.Create(topicName, &pubsubapi.Topic{}).Context(ctx).Do(); err != nil {
		t.Fatalf("recreate topic after explicit unsupported-boundary check: %v", err)
	}
	if _, err := pubsubClient.Projects.Subscriptions.Create(subscriptionName,
		&pubsubapi.Subscription{Topic: topicName}).Context(ctx).Do(); err != nil {
		t.Fatalf("recreate subscription after explicit unsupported-boundary check: %v", err)
	}
	pulled, err = pubsubClient.Projects.Subscriptions.Pull(subscriptionName,
		&pubsubapi.PullRequest{MaxMessages: 1, ReturnImmediately: true}).Context(ctx).Do()
	if err != nil {
		t.Fatalf("pull recreated subscription after emulator replacement: %v", err)
	}
	if len(pulled.ReceivedMessages) != 0 {
		t.Fatalf("Pub/Sub emulator replacement retained unsupported message state: %#v", pulled.ReceivedMessages)
	}
	t.Log("UNSUPPORTED BOUNDARY VERIFIED: exact-owned Pub/Sub emulator replacement loses topic, subscription, and queued message state")
	if err := restarted.stop(); err != nil {
		t.Fatalf("stop restarted daemon: %v", err)
	}

	for domain, runtimePath := range runtimePaths {
		if info, err := os.Stat(runtimePath); err != nil || !info.IsDir() {
			t.Errorf("%s runtime directory missing after verification: %v", domain, err)
		}
	}
}

type durabilityDaemon struct {
	command *exec.Cmd
	done    chan struct{}
	gateway string
	waitErr error
	mu      sync.Mutex
	stopped bool
}

func startDurabilityDaemon(t *testing.T, binary, stateDir, profile string) *durabilityDaemon {
	t.Helper()
	apiPort := reserveDurabilityPort(t)
	uiPort := reserveDurabilityPort(t)
	diagnosticsDir := os.Getenv("MINISKY_EMULATOR_BOUNDARY_DIAGNOSTICS_DIR")
	if diagnosticsDir == "" {
		diagnosticsDir = t.TempDir()
	}
	home := filepath.Join(stateDir, "home-"+profile)
	if err := os.MkdirAll(diagnosticsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	logFile, err := os.Create(filepath.Join(diagnosticsDir, "minisky-"+profile+".log"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "start", "--port", apiPort, "--ui-port", uiPort)
	command.Env = durabilityProcessEnvironment(stateDir, profile, home)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		logFile.Close()
		t.Fatal(err)
	}
	daemon := &durabilityDaemon{
		command: command,
		done:    make(chan struct{}),
		gateway: "http://127.0.0.1:" + apiPort,
	}
	go func() {
		err := command.Wait()
		daemon.mu.Lock()
		daemon.waitErr = err
		daemon.mu.Unlock()
		logFile.Close()
		close(daemon.done)
	}()
	t.Cleanup(func() {
		if err := daemon.stop(); err != nil {
			t.Errorf("cleanup daemon %s: %v", profile, err)
		}
	})

	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(60 * time.Second)
	for {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			daemon.gateway+"/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, requestErr := client.Do(request)
		if requestErr == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return daemon
			}
		}
		select {
		case <-daemon.done:
			daemon.mu.Lock()
			waitErr := daemon.waitErr
			daemon.mu.Unlock()
			t.Fatalf("MiniSky daemon for %s exited during startup: %v", profile, waitErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for MiniSky daemon for %s", profile)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (daemon *durabilityDaemon) stop() error {
	daemon.mu.Lock()
	if daemon.stopped {
		err := daemon.waitErr
		daemon.mu.Unlock()
		return err
	}
	daemon.stopped = true
	daemon.mu.Unlock()

	select {
	case <-daemon.done:
	default:
		if err := daemon.command.Process.Signal(os.Interrupt); err != nil {
			return err
		}
		select {
		case <-daemon.done:
		case <-time.After(30 * time.Second):
			if err := daemon.command.Process.Kill(); err != nil {
				return err
			}
			<-daemon.done
			return errors.New("MiniSky daemon did not stop within 30 seconds")
		}
	}
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	return daemon.waitErr
}

func (daemon *durabilityDaemon) crash() error {
	daemon.mu.Lock()
	if daemon.stopped {
		err := daemon.waitErr
		daemon.mu.Unlock()
		return err
	}
	daemon.stopped = true
	daemon.mu.Unlock()

	select {
	case <-daemon.done:
		return errors.New("MiniSky daemon exited before abrupt process-stop boundary")
	default:
	}
	if err := daemon.command.Process.Kill(); err != nil {
		return err
	}
	<-daemon.done
	daemon.mu.Lock()
	daemon.waitErr = nil
	daemon.mu.Unlock()
	return nil
}

func reserveDurabilityPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := fmt.Sprintf("%d", listener.Addr().(*net.TCPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func durabilityProcessEnvironment(stateDir, profile, home string) []string {
	environment := make([]string, 0, len(os.Environ())+3)
	for _, value := range os.Environ() {
		if strings.HasPrefix(value, "MINISKY_STATE_DIR=") ||
			strings.HasPrefix(value, "MINISKY_PROFILE=") ||
			strings.HasPrefix(value, "HOME=") {
			continue
		}
		environment = append(environment, value)
	}
	return append(environment,
		"MINISKY_STATE_DIR="+stateDir,
		"MINISKY_PROFILE="+profile,
		"HOME="+home,
	)
}

func newDurabilityStorageClient(t *testing.T, ctx context.Context, gateway string) *storageapi.Service {
	t.Helper()
	httpClient := &http.Client{Transport: canonicalGatewayTransport{
		prefix:         "/_minisky/storage/",
		rewriteStorage: true,
	}}
	client, err := storageapi.NewService(ctx,
		option.WithHTTPClient(httpClient),
	)
	if err != nil {
		t.Fatal(err)
	}
	client.BasePath = gateway + "/_minisky/storage/storage/v1/"
	return client
}

func newDurabilityPubSubClient(t *testing.T, ctx context.Context, gateway string) *pubsubapi.Service {
	t.Helper()
	httpClient := &http.Client{Transport: canonicalGatewayTransport{
		prefix: "/_minisky/pubsub/",
	}}
	client, err := pubsubapi.NewService(ctx,
		option.WithHTTPClient(httpClient),
	)
	if err != nil {
		t.Fatal(err)
	}
	client.BasePath = gateway + "/_minisky/pubsub/"
	return client
}

type canonicalGatewayTransport struct {
	prefix         string
	rewriteStorage bool
}

func (transport canonicalGatewayTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	urlCopy := *request.URL
	request.URL = &urlCopy
	if transport.rewriteStorage &&
		(strings.HasPrefix(request.URL.Path, "/upload/storage/v1/") ||
			strings.HasPrefix(request.URL.Path, "/download/storage/v1/")) {
		request.URL.Path = "/_minisky/storage" + request.URL.Path
	}
	if !strings.HasPrefix(request.URL.Path, transport.prefix) {
		return nil, fmt.Errorf("generated client bypassed canonical gateway prefix %q with %q",
			transport.prefix, request.URL.Path)
	}
	return http.DefaultTransport.RoundTrip(request)
}

func assertGoogleAPINotFound(t *testing.T, err error) {
	t.Helper()
	var apiError *googleapi.Error
	if !errors.As(err, &apiError) || apiError.Code != http.StatusNotFound {
		t.Fatalf("generated client error = %v, want HTTP 404", err)
	}
}

func entryNames(entries []os.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func durableEmulatorContainerID(
	t *testing.T,
	manager *ServiceManager,
	ctx context.Context,
	domain string,
	emulator config.EmulatorConfig,
) string {
	t.Helper()
	name := emulatorContainerName(domain, emulator.Name)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/containers/"+name+"/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := manager.doDocker(request)
	if err != nil {
		t.Fatalf("inspect exact-owned %s container: %v", domain, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("inspect exact-owned %s container returned %d", domain, response.StatusCode)
	}
	var inspect struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&inspect); err != nil {
		t.Fatalf("decode exact-owned %s container: %v", domain, err)
	}
	if inspect.ID == "" {
		t.Fatalf("exact-owned %s container has empty ID", domain)
	}
	return inspect.ID
}

func mustDurableEmulatorConfig(
	t *testing.T,
	domain string,
	emulator config.EmulatorConfig,
) ContainerConfig {
	t.Helper()
	container, _, err := durableEmulatorConfig(domain, emulator, nil)
	if err != nil {
		t.Fatal(err)
	}
	return container
}

func isPinnedEmulatorImage(image string) bool {
	parts := strings.Split(image, "@sha256:")
	if len(parts) != 2 || parts[0] == "" || len(parts[1]) != 64 {
		return false
	}
	_, err := hex.DecodeString(parts[1])
	return err == nil
}
