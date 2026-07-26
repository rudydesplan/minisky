package cloudbuild

import (
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

func TestCloudBuildTriggersReturnExplicitUnimplemented(t *testing.T) {
	api := newAPI(nil, orchestrator.NewOperationManager())
	api.runAsync = func(string, func() error) {}

	const requests = 32
	var wg sync.WaitGroup
	for index := 0; index < requests; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for _, path := range []string{
				"/v1/projects/demo/triggers",
				fmt.Sprintf("/v1/projects/demo/triggers/trigger-%d:run", index),
			} {
				response := httptest.NewRecorder()
				api.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
				if response.Code != http.StatusNotImplemented ||
					!strings.Contains(response.Body.String(), `"status":"UNIMPLEMENTED"`) {
					t.Errorf("%s status=%d body=%s", path, response.Code, response.Body.String())
				}
			}
		}(index)
	}
	wg.Wait()
	if operations := api.opMgr.List(); len(operations) != 0 {
		t.Fatalf("unsupported triggers created operations: %#v", operations)
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
