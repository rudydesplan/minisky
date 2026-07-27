package documentai

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/state"
)

type documentAIFakeStore struct {
	mu      sync.Mutex
	payload []byte
	loadErr error
	saveErr error
}

func (s *documentAIFakeStore) Load(_ string, target any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return s.loadErr
	}
	if s.payload == nil {
		return state.ErrNotFound
	}
	return json.Unmarshal(s.payload, target)
}

func (s *documentAIFakeStore) Save(_ string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	payload, err := json.Marshal(value)
	if err == nil {
		s.payload = payload
	}
	return err
}

func TestDocumentAIPersistenceFailureIsSticky(t *testing.T) {
	store := &documentAIFakeStore{saveErr: errors.New("injected save failure")}
	api := NewAPIWithStore(store)

	create := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost,
			"/v1/projects/test/locations/us/processors",
			strings.NewReader(`{"type":"OCR_PROCESSOR"}`))
		response := httptest.NewRecorder()
		api.ServeHTTP(response, request)
		return response
	}

	if response := create(); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("first create status = %d, body = %s", response.Code, response.Body.String())
	}
	store.mu.Lock()
	store.saveErr = nil
	store.mu.Unlock()
	if response := create(); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("second create status = %d, want sticky 503", response.Code)
	}
	api.mu.RLock()
	defer api.mu.RUnlock()
	if len(api.processors) != 0 {
		t.Fatalf("failed creates left processors: %#v", api.processors)
	}
}

func TestDocumentAICreatedProcessorSurvivesRestart(t *testing.T) {
	store := &documentAIFakeStore{}
	api := NewAPIWithStore(store)
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us/processors",
		strings.NewReader(`{"type":"OCR_PROCESSOR"}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}

	restarted := NewAPIWithStore(store)
	getRequest := httptest.NewRequest(http.MethodGet,
		"/v1/projects/test/locations/us/processors/proc-1", nil)
	getResponse := httptest.NewRecorder()
	restarted.ServeHTTP(getResponse, getRequest)
	if getResponse.Code != http.StatusOK {
		t.Fatalf("processor after restart status = %d, body = %s", getResponse.Code, getResponse.Body.String())
	}
	var processor Processor
	if err := json.NewDecoder(getResponse.Body).Decode(&processor); err != nil {
		t.Fatal(err)
	}
	if processor.Name == "" || processor.State != "ENABLED" {
		t.Fatalf("processor after restart = %#v", processor)
	}
}

func TestDocumentAIDeleteOperationSurvivesRestart(t *testing.T) {
	store := &documentAIFakeStore{}
	api := NewAPIWithStore(store)
	createRequest := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us/processors",
		strings.NewReader(`{"type":"OCR_PROCESSOR"}`))
	createResponse := httptest.NewRecorder()
	api.ServeHTTP(createResponse, createRequest)
	if createResponse.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", createResponse.Code, createResponse.Body.String())
	}

	deleteRequest := httptest.NewRequest(http.MethodDelete,
		"/v1/projects/test/locations/us/processors/proc-1", nil)
	deleteResponse := httptest.NewRecorder()
	api.ServeHTTP(deleteResponse, deleteRequest)
	if deleteResponse.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", deleteResponse.Code, deleteResponse.Body.String())
	}
	var operation lro
	if err := json.NewDecoder(deleteResponse.Body).Decode(&operation); err != nil {
		t.Fatal(err)
	}

	restarted := NewAPIWithStore(store)
	pollResponse := httptest.NewRecorder()
	restarted.ServeHTTP(pollResponse,
		httptest.NewRequest(http.MethodGet, "/v1/"+operation.Name, nil))
	if pollResponse.Code != http.StatusOK {
		t.Fatalf("poll after restart status = %d, body = %s", pollResponse.Code, pollResponse.Body.String())
	}
	var persisted lro
	if err := json.NewDecoder(pollResponse.Body).Decode(&persisted); err != nil {
		t.Fatal(err)
	}
	if !persisted.Done || persisted.Response == nil {
		t.Fatalf("persisted operation = %#v", persisted)
	}
}

func TestDocumentAIConcurrentCreatesRemainDurable(t *testing.T) {
	store := &documentAIFakeStore{}
	api := NewAPIWithStore(store)
	var wait sync.WaitGroup
	for range 16 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			request := httptest.NewRequest(http.MethodPost,
				"/v1/projects/test/locations/us/processors",
				strings.NewReader(`{"type":"OCR_PROCESSOR"}`))
			response := httptest.NewRecorder()
			api.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Errorf("create status = %d, body = %s", response.Code, response.Body.String())
			}
		}()
	}
	wait.Wait()
	restarted := NewAPIWithStore(store)
	restarted.mu.RLock()
	defer restarted.mu.RUnlock()
	if len(restarted.processors) != 16 {
		t.Fatalf("durable processors = %d, want 16", len(restarted.processors))
	}
}

func TestDocumentAIUnknownOperationIsNotFound(t *testing.T) {
	api := newTestAPI()
	request := httptest.NewRequest(http.MethodGet,
		"/v1/projects/test/locations/us/operations/missing", nil)
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestDocumentAICorruptLoadFailsClosed(t *testing.T) {
	store := &documentAIFakeStore{loadErr: errors.New("corrupt state")}
	api := NewAPIWithStore(store)
	request := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test/locations/us/processors",
		strings.NewReader(`{"type":"OCR_PROCESSOR"}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestDocumentAIImportRejectsMalformedSnapshotBeforeReplacement(t *testing.T) {
	store, err := state.New(t.TempDir(), "documentai-import")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(stateEntry, documentaiMetadata{
		Processors: map[string]*Processor{},
		Operations: map[string]*lro{},
	}); err != nil {
		t.Fatal(err)
	}
	snapshot := `{"format":"minisky-state","version":1,"entries":{"documentai/metadata":` +
		`{"processors":{"projects/p/locations/l/processors/proc-2":null},"operations":{},"seq":1}}}`
	if err := store.Import(bytes.NewBufferString(snapshot)); err == nil {
		t.Fatal("Import accepted malformed Document AI metadata")
	}
	var active documentaiMetadata
	if err := store.Load(stateEntry, &active); err != nil {
		t.Fatal(err)
	}
	if len(active.Processors) != 0 {
		t.Fatalf("failed import replaced active state: %#v", active.Processors)
	}
}
