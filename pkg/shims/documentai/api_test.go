package documentai

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
)

func TestMain(m *testing.M) {
	dir, _ := os.MkdirTemp("", "minisky-documentai-test-*")
	os.Setenv("MINISKY_STATE_DIR", dir)
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func newTestAPI() *API {
	return &API{
		processors: make(map[string]*Processor),
		operations: make(map[string]*lro),
		opMgr:      orchestrator.NewOperationManager(),
	}
}

func TestCreateProcessor(t *testing.T) {
	api := newTestAPI()
	body := `{"type":"FORM_PARSER_PROCESSOR","displayName":"My Parser"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us/processors", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	var created Processor
	json.NewDecoder(rec.Body).Decode(&created)
	if created.Name == "" || created.State != "ENABLED" {
		t.Fatalf("create response = %+v, want enabled Processor", created)
	}

	// Verify processor is ENABLED
	api.mu.RLock()
	var proc *Processor
	for _, p := range api.processors {
		proc = p
		break
	}
	api.mu.RUnlock()
	if proc == nil {
		t.Fatal("processor not created")
	}
	if proc.State != "ENABLED" {
		t.Fatalf("state = %q, want ENABLED", proc.State)
	}
	if proc.Type != "FORM_PARSER_PROCESSOR" {
		t.Fatalf("type = %q, want FORM_PARSER_PROCESSOR", proc.Type)
	}
	if proc.DisplayName != "My Parser" {
		t.Fatalf("displayName = %q, want My Parser", proc.DisplayName)
	}
	if proc.CreateTime == "" {
		t.Fatal("createTime is empty")
	}
}

func TestCreateProcessorMissingType(t *testing.T) {
	api := newTestAPI()
	body := `{"displayName":"No Type"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us/processors", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "type") {
		t.Error("error should mention type field")
	}
}

func TestGetProcessor(t *testing.T) {
	api := newTestAPI()
	name := "projects/test/locations/us/processors/proc-1"
	api.processors[name] = &Processor{
		Name:        name,
		Type:        "OCR_PROCESSOR",
		DisplayName: "OCR",
		State:       "ENABLED",
		CreateTime:  "2024-01-01T00:00:00Z",
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/"+name, nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	var proc Processor
	json.NewDecoder(rec.Body).Decode(&proc)
	if proc.Name != name {
		t.Fatalf("name = %q, want %q", proc.Name, name)
	}
	if proc.Type != "OCR_PROCESSOR" {
		t.Fatalf("type = %q, want OCR_PROCESSOR", proc.Type)
	}
}

func TestGetProcessorNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us/processors/nonexistent", nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestListProcessors(t *testing.T) {
	api := newTestAPI()
	api.processors["projects/test/locations/us/processors/proc-1"] = &Processor{
		Name: "projects/test/locations/us/processors/proc-1", Type: "OCR_PROCESSOR", State: "ENABLED",
	}
	api.processors["projects/test/locations/us/processors/proc-2"] = &Processor{
		Name: "projects/test/locations/us/processors/proc-2", Type: "FORM_PARSER_PROCESSOR", State: "ENABLED",
	}
	api.processors["projects/other/locations/eu/processors/proc-3"] = &Processor{
		Name: "projects/other/locations/eu/processors/proc-3", Type: "OCR_PROCESSOR", State: "ENABLED",
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/test/locations/us/processors", nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp map[string]any
	json.NewDecoder(rec.Body).Decode(&resp)
	processors := resp["processors"].([]any)
	if len(processors) != 2 {
		t.Fatalf("got %d processors, want 2 (filtered by parent)", len(processors))
	}
}

func TestDeleteProcessor(t *testing.T) {
	api := newTestAPI()
	name := "projects/test/locations/us/processors/proc-1"
	api.processors[name] = &Processor{
		Name: name, Type: "OCR_PROCESSOR", State: "ENABLED",
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/"+name, nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	var op map[string]any
	json.NewDecoder(rec.Body).Decode(&op)
	if op["done"] != true {
		t.Error("expected durable terminal delete operation")
	}

	// Wait for async deletion
	time.Sleep(100 * time.Millisecond)

	api.mu.RLock()
	_, exists := api.processors[name]
	api.mu.RUnlock()
	if exists {
		t.Fatal("processor should be deleted after LRO completes")
	}
}

func TestProcessDocument(t *testing.T) {
	api := newTestAPI()
	name := "projects/test/locations/us/processors/proc-1"
	api.processors[name] = &Processor{
		Name: name, Type: "FORM_PARSER_PROCESSOR", State: "ENABLED",
	}

	body := `{"rawDocument":{"content":"cGRmZGF0YQ==","mimeType":"application/pdf"},"skipHumanReview":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/"+name+":process", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body = %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-MiniSky-Simulated") != "true" {
		t.Error("missing X-MiniSky-Simulated header")
	}

	if strings.Contains(rec.Body.String(), `"confidence":`) {
		t.Fatal("unsupported processing fabricated confidence")
	}
}

func TestProcessDocumentProcessorNotFound(t *testing.T) {
	api := newTestAPI()
	body := `{"rawDocument":{"content":"cGRmZGF0YQ==","mimeType":"application/pdf"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us/processors/nonexistent:process", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestGetOperation(t *testing.T) {
	api := newTestAPI()
	registered, err := api.opMgr.RegisterScopedTargetDurable("documentai#operation", "delete",
		"projects/test/locations/us/processors/proc-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.opMgr.FinalizeScopedDurable(registered.Name, json.RawMessage(`{}`), 0, ""); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/"+registered.Name, nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var op map[string]any
	json.NewDecoder(rec.Body).Decode(&op)
	if op["done"] != true {
		t.Error("expected done=true")
	}
}

func TestPersistAndReload(t *testing.T) {
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "documentai-restart")
	// Use a real API with state dir
	api := NewAPI()

	// Create a processor
	body := `{"type":"OCR_PROCESSOR","displayName":"Persist Test"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/test/locations/us/processors", strings.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create status = %d", rec.Code)
	}

	// Wait for state transition
	time.Sleep(100 * time.Millisecond)

	// Create a new API instance (simulates restart)
	api2 := NewAPI()

	// Verify processor was rehydrated
	api2.mu.RLock()
	count := len(api2.processors)
	api2.mu.RUnlock()
	if count != 1 {
		t.Fatalf("rehydrated %d processors, want 1", count)
	}
}
