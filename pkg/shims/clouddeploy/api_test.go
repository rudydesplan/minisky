package clouddeploy

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"minisky/pkg/orchestrator"
)

func TestCreatePipeline(t *testing.T) {
	api := newTestAPI()
	body := `{"serialPipeline":{"stages":[{"targetId":"dev","profiles":["dev"]}]},"labels":{"env":"test"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines?deliveryPipelineId=mypipe", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != false {
		t.Fatal("expected LRO not done")
	}
	meta, _ := resp["metadata"].(map[string]any)
	if meta == nil || meta["verb"] != "create" {
		t.Fatalf("unexpected metadata: %v", meta)
	}
	if meta["target"] != "projects/test/locations/us-central1/deliveryPipelines/mypipe" {
		t.Fatalf("unexpected target: %v", meta["target"])
	}

	api.mu.RLock()
	stored := api.pipelines["projects/test/locations/us-central1/deliveryPipelines/mypipe"]
	api.mu.RUnlock()
	if stored == nil {
		t.Fatal("pipeline not stored")
	}
	if stored.UID == "" {
		t.Fatal("expected uid")
	}
	if stored.SerialPipeline == nil || len(stored.SerialPipeline.Stages) != 1 {
		t.Fatalf("unexpected serialPipeline: %+v", stored.SerialPipeline)
	}
}

func TestCreatePipelineMissingId(t *testing.T) {
	api := newTestAPI()
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCreatePipelineDuplicate(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.pipelines["projects/test/locations/us-central1/deliveryPipelines/dup"] = &DeliveryPipeline{Name: "projects/test/locations/us-central1/deliveryPipelines/dup"}
	api.mu.Unlock()

	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines?deliveryPipelineId=dup", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
}

func TestGetPipeline(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.pipelines["projects/test/locations/us-central1/deliveryPipelines/mypipe"] = &DeliveryPipeline{
		Name: "projects/test/locations/us-central1/deliveryPipelines/mypipe",
		UID:  "uid-1",
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/deliveryPipelines/mypipe", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var p DeliveryPipeline
	_ = json.Unmarshal(w.Body.Bytes(), &p)
	if p.UID != "uid-1" {
		t.Fatalf("unexpected uid: %s", p.UID)
	}
}

func TestGetPipelineNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/deliveryPipelines/nope", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestListPipelines(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.pipelines["projects/test/locations/us-central1/deliveryPipelines/a"] = &DeliveryPipeline{Name: "projects/test/locations/us-central1/deliveryPipelines/a"}
	api.pipelines["projects/test/locations/us-central1/deliveryPipelines/b"] = &DeliveryPipeline{Name: "projects/test/locations/us-central1/deliveryPipelines/b"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/deliveryPipelines", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	items, _ := resp["deliveryPipelines"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 pipelines, got %d", len(items))
	}
}

func TestDeletePipeline(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.pipelines["projects/test/locations/us-central1/deliveryPipelines/del"] = &DeliveryPipeline{Name: "projects/test/locations/us-central1/deliveryPipelines/del"}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/deliveryPipelines/del", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected done=true for delete LRO")
	}
}

func TestDeletePipelineNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodDelete, "/v1/projects/test/locations/us-central1/deliveryPipelines/nope", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestPatchPipeline(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.pipelines["projects/test/locations/us-central1/deliveryPipelines/mypipe"] = &DeliveryPipeline{
		Name:           "projects/test/locations/us-central1/deliveryPipelines/mypipe",
		SerialPipeline: &SerialPipeline{Stages: []Stage{{TargetID: "old"}}},
	}
	api.mu.Unlock()

	body := `{"serialPipeline":{"stages":[{"targetId":"new","profiles":["prod"]}]}}`
	req := httptest.NewRequest(http.MethodPatch, "/v1/projects/test/locations/us-central1/deliveryPipelines/mypipe", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != true {
		t.Fatal("expected done=true for patch LRO")
	}

	api.mu.RLock()
	updated := api.pipelines["projects/test/locations/us-central1/deliveryPipelines/mypipe"]
	api.mu.RUnlock()
	if updated.SerialPipeline.Stages[0].TargetID != "new" {
		t.Fatalf("expected updated targetId, got %s", updated.SerialPipeline.Stages[0].TargetID)
	}
}

func TestPatchPipelineRollsBackOnStateSaveFailure(t *testing.T) {
	api := newTestAPI()
	api.stateStore = failingDeployStore{}
	name := "projects/test/locations/us-central1/deliveryPipelines/mypipe"
	api.pipelines[name] = &DeliveryPipeline{
		Name:           name,
		SerialPipeline: &SerialPipeline{Stages: []Stage{{TargetID: "old"}}},
	}

	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/v1/"+name,
		bytes.NewBufferString(`{"serialPipeline":{"stages":[{"targetId":"new"}]}}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
	}
	if got := api.pipelines[name].SerialPipeline.Stages[0].TargetID; got != "old" {
		t.Fatalf("committed target after failed save = %q", got)
	}
	if operations := api.opMgr.List(); len(operations) != 0 {
		t.Fatalf("orphan operations = %+v", operations)
	}
}

func TestPatchPipelineRollsBackOnOperationRegistrationFailure(t *testing.T) {
	manager, err := orchestrator.NewOperationManagerWithStore(failingDeployStore{})
	if err != nil {
		t.Fatal(err)
	}
	api := NewAPI(manager)
	name := "projects/test/locations/us-central1/deliveryPipelines/mypipe"
	api.pipelines[name] = &DeliveryPipeline{
		Name:           name,
		SerialPipeline: &SerialPipeline{Stages: []Stage{{TargetID: "old"}}},
	}

	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/v1/"+name,
		bytes.NewBufferString(`{"serialPipeline":{"stages":[{"targetId":"new"}]}}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
	}
	if got := api.pipelines[name].SerialPipeline.Stages[0].TargetID; got != "old" {
		t.Fatalf("committed target after operation failure = %q", got)
	}
	if operations := manager.List(); len(operations) != 0 {
		t.Fatalf("orphan operations = %+v", operations)
	}
}

func TestCreateRelease(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.pipelines["projects/test/locations/us-central1/deliveryPipelines/mypipe"] = &DeliveryPipeline{Name: "projects/test/locations/us-central1/deliveryPipelines/mypipe"}
	api.mu.Unlock()

	body := `{"labels":{"version":"v1"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines/mypipe/releases?releaseId=rel1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != false {
		t.Fatal("expected LRO not done")
	}

	api.mu.RLock()
	stored := api.releases["projects/test/locations/us-central1/deliveryPipelines/mypipe/releases/rel1"]
	api.mu.RUnlock()
	if stored == nil {
		t.Fatal("release not stored")
	}
	if stored.UID == "" {
		t.Fatal("expected uid")
	}
}

func TestCreateReleaseParentNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines/nope/releases?releaseId=rel1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateRollout(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	api := newTestAPI()
	api.mu.Lock()
	api.pipelines["projects/test/locations/us-central1/deliveryPipelines/mypipe"] = &DeliveryPipeline{Name: "projects/test/locations/us-central1/deliveryPipelines/mypipe"}
	api.releases["projects/test/locations/us-central1/deliveryPipelines/mypipe/releases/rel1"] = &Release{Name: "projects/test/locations/us-central1/deliveryPipelines/mypipe/releases/rel1"}
	api.mu.Unlock()

	body := `{"targetId":"prod","image":"example.test/app@sha256:abc","localTarget":"` + target.URL + `"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines/mypipe/releases/rel1/rollouts?rolloutId=roll1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["done"] != false {
		t.Fatal("expected LRO not done")
	}

	api.mu.RLock()
	stored := api.rollouts["projects/test/locations/us-central1/deliveryPipelines/mypipe/releases/rel1/rollouts/roll1"]
	api.mu.RUnlock()
	if stored == nil {
		t.Fatal("rollout not stored")
	}
	if stored.TargetID != "prod" {
		t.Fatalf("expected targetId 'prod', got %s", stored.TargetID)
	}
}

func TestCreateRolloutParentNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines/mypipe/releases/nope/rollouts?rolloutId=roll1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	// Create a pipeline to generate an operation
	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines?deliveryPipelineId=optest", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create failed: %d", w.Code)
	}
	var createResp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &createResp)
	opFullName, _ := createResp["name"].(string)
	if opFullName == "" {
		t.Fatal("no operation name returned")
	}

	// Poll the operation
	req = httptest.NewRequest(http.MethodGet, "/v1/"+opFullName, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetOperationNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/operations/nonexistent", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestConcurrentPipelineCreation(t *testing.T) {
	api := newTestAPI()
	var wg sync.WaitGroup
	conflicts := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := `{}`
			req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines?deliveryPipelineId=race", bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code == http.StatusConflict {
				mu.Lock()
				conflicts++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if conflicts != 9 {
		t.Fatalf("expected 9 conflicts, got %d", conflicts)
	}
}

func TestFullHierarchy(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	api := newTestAPI()

	// Create pipeline
	body := `{"serialPipeline":{"stages":[{"targetId":"dev"}]}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines?deliveryPipelineId=pipe1", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create pipeline: %d", w.Code)
	}

	// Create release
	body = `{}`
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines/pipe1/releases?releaseId=rel1", bytes.NewBufferString(body))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create release: %d: %s", w.Code, w.Body.String())
	}

	// Create rollout
	body = `{"targetId":"dev","image":"example.test/app@sha256:abc","localTarget":"` + target.URL + `"}`
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us-central1/deliveryPipelines/pipe1/releases/rel1/rollouts?rolloutId=roll1", bytes.NewBufferString(body))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create rollout: %d: %s", w.Code, w.Body.String())
	}

	// Get rollout
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/deliveryPipelines/pipe1/releases/rel1/rollouts/roll1", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("get rollout: %d", w.Code)
	}

	// List rollouts
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us-central1/deliveryPipelines/pipe1/releases/rel1/rollouts", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("list rollouts: %d", w.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	items, _ := resp["rollouts"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 rollout, got %d", len(items))
	}
}
