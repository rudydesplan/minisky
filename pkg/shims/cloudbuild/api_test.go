package cloudbuild

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"minisky/pkg/orchestrator"
)

func TestCloudBuildDockerIdentitySeparatesProfileProjectAndBuild(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "profile-a")
	first := cloudBuildDockerIdentity("projects/project-a/builds/build-1")
	second := cloudBuildDockerIdentity("projects/project-b/builds/build-1")
	third := cloudBuildDockerIdentity("projects/project-a/builds/build-2")
	if first == second || first == third || second == third {
		t.Fatalf("Cloud Build Docker identities collided: %q %q %q", first, second, third)
	}
	t.Setenv("MINISKY_PROFILE", "profile-b")
	if otherProfile := cloudBuildDockerIdentity("projects/project-a/builds/build-1"); otherProfile == first {
		t.Fatalf("Cloud Build Docker identity collided across profiles: %q", first)
	}
}

func TestConcurrentTriggerRunsReceiveDistinctOwnedDockerResources(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "trigger-race")
	api := newAPI(nil, orchestrator.NewOperationManager())
	api.runAsync = func(string, func() error) {}

	const requests = 256
	type result struct {
		buildID   string
		resource  string
		identity  string
		workspace string
	}
	results := make(chan result, requests)
	var wg sync.WaitGroup
	for index := 0; index < requests; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			request := httptest.NewRequest(http.MethodPost,
				fmt.Sprintf("/v1/projects/demo/triggers/trigger-%d:run", index), nil)
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Errorf("trigger %d status=%d body=%s", index, response.Code, response.Body.String())
				return
			}
			var operation orchestrator.Operation
			if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
				t.Errorf("trigger %d decode: %v", index, err)
				return
			}
			buildID := operation.TargetLink[strings.LastIndex(operation.TargetLink, "/")+1:]
			resource := "projects/demo/builds/" + buildID
			identity := cloudBuildDockerIdentity(resource)
			results <- result{buildID: buildID, resource: resource, identity: identity, workspace: identity + "-workspace"}
		}(index)
	}
	wg.Wait()
	close(results)

	ids := make(map[string]bool, requests)
	resources := make(map[string]bool, requests)
	identities := make(map[string]bool, requests)
	workspaces := make(map[string]bool, requests)
	for result := range results {
		if ids[result.buildID] || resources[result.resource] || identities[result.identity] || workspaces[result.workspace] {
			t.Fatalf("concurrent trigger collision: %#v", result)
		}
		ids[result.buildID] = true
		resources[result.resource] = true
		identities[result.identity] = true
		workspaces[result.workspace] = true
	}
	if len(ids) != requests {
		t.Fatalf("received %d unique trigger builds, want %d", len(ids), requests)
	}
}

func TestBuildIDAllocationAtomicallyRetriesCollision(t *testing.T) {
	api := newAPI(nil, orchestrator.NewOperationManager())
	var calls atomic.Int32
	api.randomID = func(target []byte) (int, error) {
		value := byte(1)
		if calls.Add(1) >= 3 {
			value = 2
		}
		for index := range target {
			target[index] = value
		}
		return len(target), nil
	}
	first, err := api.allocateBuildID("build-trigger-")
	if err != nil {
		t.Fatal(err)
	}
	second, err := api.allocateBuildID("build-trigger-")
	if err != nil {
		t.Fatal(err)
	}
	if first == second || calls.Load() != 3 {
		t.Fatalf("first=%q second=%q random calls=%d", first, second, calls.Load())
	}
}
