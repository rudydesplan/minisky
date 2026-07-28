package orchestrator

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"minisky/pkg/config"
)

func TestStorageAndPubSubDataSurviveContainerAndManagerRestart(t *testing.T) {
	if os.Getenv("MINISKY_DOCKER_EMULATOR_PERSISTENCE_INTEGRATION") != "1" {
		t.Skip("set MINISKY_DOCKER_EMULATOR_PERSISTENCE_INTEGRATION=1 to run")
	}
	storageImage := os.Getenv("MINISKY_STORAGE_TEST_IMAGE")
	pubsubImage := os.Getenv("MINISKY_PUBSUB_TEST_IMAGE")
	if !isPinnedEmulatorImage(storageImage) || !isPinnedEmulatorImage(pubsubImage) {
		t.Fatal("MINISKY_STORAGE_TEST_IMAGE and MINISKY_PUBSUB_TEST_IMAGE must be image@sha256:<64 hex>")
	}

	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", fmt.Sprintf("emulator-restart-%d", time.Now().UnixNano()))
	storageConfig := config.EmulatorConfig{
		Name: "ignored", Image: storageImage, Port: "4443/tcp",
		Cmd: []string{"-scheme", "http"},
	}
	pubsubConfig := config.EmulatorConfig{
		Name: "ignored", Image: pubsubImage, Port: "8085/tcp",
		Cmd: []string{"gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085"},
	}

	first, err := NewServiceManager()
	if err != nil {
		t.Fatal(err)
	}
	ping, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost/_ping", nil)
	if err != nil {
		t.Fatal(err)
	}
	pingResponse, err := first.doDocker(ping)
	if err != nil {
		t.Skipf("Docker daemon unavailable: %v", err)
	}
	pingResponse.Body.Close()
	if pingResponse.StatusCode != http.StatusOK {
		t.Skipf("Docker daemon ping returned %d", pingResponse.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for domain, emulator := range map[string]config.EmulatorConfig{
			"storage.googleapis.com": storageConfig,
			"pubsub.googleapis.com":  pubsubConfig,
		} {
			if err := first.removeDurableEmulatorContainer(cleanupCtx, domain, emulator); err != nil {
				t.Errorf("cleanup %s emulator: %v", domain, err)
			}
		}
	})

	storageURL, err := first.ensureDurableEmulatorRunning(ctx, "storage.googleapis.com", storageConfig)
	if err != nil {
		t.Fatal(err)
	}
	pubsubURL, err := first.ensureDurableEmulatorRunning(ctx, "pubsub.googleapis.com", pubsubConfig)
	if err != nil {
		t.Fatal(err)
	}

	const (
		project      = "restart-project"
		bucket       = "restart-bucket"
		object       = "restart-object.txt"
		objectBody   = "storage survives restart"
		topic        = "restart-topic"
		subscription = "restart-subscription"
		messageBody  = "cHVic3ViIHN1cnZpdmVzIHJlc3RhcnQ="
	)
	doEmulatorRequest(t, ctx, http.MethodPost, storageURL+"/storage/v1/b?project="+project,
		`{"name":"`+bucket+`"}`, http.StatusOK)
	doEmulatorRequest(t, ctx, http.MethodPost,
		storageURL+"/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name="+object,
		objectBody, http.StatusOK)
	doEmulatorRequest(t, ctx, http.MethodPut,
		pubsubURL+"/v1/projects/"+project+"/topics/"+topic, `{}`, http.StatusOK)
	doEmulatorRequest(t, ctx, http.MethodPut,
		pubsubURL+"/v1/projects/"+project+"/subscriptions/"+subscription,
		`{"topic":"projects/`+project+`/topics/`+topic+`"}`, http.StatusOK)
	doEmulatorRequest(t, ctx, http.MethodPost,
		pubsubURL+"/v1/projects/"+project+"/topics/"+topic+":publish",
		`{"messages":[{"data":"`+messageBody+`"}]}`, http.StatusOK)

	if err := first.removeDurableEmulatorContainer(ctx, "storage.googleapis.com", storageConfig); err != nil {
		t.Fatal(err)
	}
	if err := first.removeDurableEmulatorContainer(ctx, "pubsub.googleapis.com", pubsubConfig); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewServiceManager()
	if err != nil {
		t.Fatal(err)
	}
	storageURL, err = restarted.ensureDurableEmulatorRunning(ctx, "storage.googleapis.com", storageConfig)
	if err != nil {
		t.Fatal(err)
	}
	pubsubURL, err = restarted.ensureDurableEmulatorRunning(ctx, "pubsub.googleapis.com", pubsubConfig)
	if err != nil {
		t.Fatal(err)
	}

	doEmulatorRequest(t, ctx, http.MethodGet,
		storageURL+"/storage/v1/b/"+bucket, "", http.StatusOK)
	objectResponse := doEmulatorRequest(t, ctx, http.MethodGet,
		storageURL+"/download/storage/v1/b/"+bucket+"/o/"+object+"?alt=media", "", http.StatusOK)
	if string(objectResponse) != objectBody {
		t.Fatalf("object after restart = %q, want %q", objectResponse, objectBody)
	}
	doEmulatorRequest(t, ctx, http.MethodGet,
		pubsubURL+"/v1/projects/"+project+"/topics/"+topic, "", http.StatusOK)
	doEmulatorRequest(t, ctx, http.MethodGet,
		pubsubURL+"/v1/projects/"+project+"/subscriptions/"+subscription, "", http.StatusOK)
	pulled := doEmulatorRequest(t, ctx, http.MethodPost,
		pubsubURL+"/v1/projects/"+project+"/subscriptions/"+subscription+":pull",
		`{"maxMessages":1}`, http.StatusOK)
	var pullResponse struct {
		ReceivedMessages []struct {
			Message struct {
				Data string `json:"data"`
			} `json:"message"`
		} `json:"receivedMessages"`
	}
	if err := json.Unmarshal(pulled, &pullResponse); err != nil {
		t.Fatal(err)
	}
	if len(pullResponse.ReceivedMessages) != 1 ||
		pullResponse.ReceivedMessages[0].Message.Data != messageBody {
		t.Fatalf("persisted Pub/Sub message response = %s", pulled)
	}

	if err := restarted.removeDurableEmulatorContainer(ctx, "storage.googleapis.com", storageConfig); err != nil {
		t.Fatal(err)
	}
	storageRuntime := strings.TrimSuffix(
		mustDurableEmulatorConfig(t, "storage.googleapis.com", storageConfig).Volume, ":/data")
	if err := os.RemoveAll(storageRuntime); err != nil {
		t.Fatalf("remove host-owned Storage runtime after container cleanup: %v", err)
	}
}

func doEmulatorRequest(
	t *testing.T,
	ctx context.Context,
	method, endpoint, body string,
	wantStatus int,
) []byte {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" && strings.HasPrefix(strings.TrimSpace(body), "{") {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("%s %s returned %d, want %d: %s",
			method, endpoint, response.StatusCode, wantStatus, bytes.TrimSpace(payload))
	}
	return payload
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
