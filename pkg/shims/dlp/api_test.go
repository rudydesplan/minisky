package dlp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"minisky/pkg/state"
)

func TestCreateTemplate(t *testing.T) {
	api := newAPI(nil)
	body := `{"inspectTemplate":{"displayName":"test","description":"desc","inspectConfig":{"infoTypes":[{"name":"EMAIL_ADDRESS"}],"minLikelihood":"LIKELY"}},"templateId":"my-tmpl"}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/inspectTemplates", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify X-MiniSky-Simulated header
	if w.Header().Get("X-MiniSky-Simulated") != "true" {
		t.Fatal("expected X-MiniSky-Simulated: true header")
	}

	var resp InspectTemplate
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Name != "projects/my-project/inspectTemplates/my-tmpl" {
		t.Fatalf("unexpected name: %s", resp.Name)
	}
	if resp.DisplayName != "test" {
		t.Fatalf("unexpected displayName: %s", resp.DisplayName)
	}
	if resp.Description != "desc" {
		t.Fatalf("unexpected description: %s", resp.Description)
	}
	if resp.CreateTime == "" {
		t.Fatal("expected createTime")
	}
	if resp.UpdateTime == "" {
		t.Fatal("expected updateTime")
	}
	if resp.InspectConfig == nil {
		t.Fatal("expected inspectConfig")
	}
}

func TestGetTemplate(t *testing.T) {
	api := newAPI(nil)
	// Create first
	body := `{"inspectTemplate":{"displayName":"get-test"},"templateId":"get-tmpl"}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/inspectTemplates", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d: %s", w.Code, w.Body.String())
	}

	// Get
	req = httptest.NewRequest(http.MethodGet, "/v2/projects/my-project/inspectTemplates/get-tmpl", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp InspectTemplate
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.DisplayName != "get-test" {
		t.Fatalf("unexpected displayName: %s", resp.DisplayName)
	}
	if resp.Name != "projects/my-project/inspectTemplates/get-tmpl" {
		t.Fatalf("unexpected name: %s", resp.Name)
	}
}

func TestGetTemplateNotFound(t *testing.T) {
	api := newAPI(nil)
	req := httptest.NewRequest(http.MethodGet, "/v2/projects/my-project/inspectTemplates/nonexistent", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	errObj := resp["error"].(map[string]any)
	if errObj["status"] != "NOT_FOUND" {
		t.Fatalf("expected NOT_FOUND status, got %v", errObj["status"])
	}
}

func TestListTemplates(t *testing.T) {
	api := newAPI(nil)
	// Create multiple templates
	for _, id := range []string{"alpha", "beta", "gamma"} {
		body := fmt.Sprintf(`{"inspectTemplate":{"displayName":"%s"},"templateId":"%s"}`, id, id)
		req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/inspectTemplates", strings.NewReader(body))
		w := httptest.NewRecorder()
		api.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("create %s: %d", id, w.Code)
		}
	}

	// List all
	req := httptest.NewRequest(http.MethodGet, "/v2/projects/my-project/inspectTemplates", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	templates := resp["inspectTemplates"].([]any)
	if len(templates) != 3 {
		t.Fatalf("expected 3 templates, got %d", len(templates))
	}

	// Verify sorted order
	first := templates[0].(map[string]any)["name"].(string)
	second := templates[1].(map[string]any)["name"].(string)
	if first >= second {
		t.Fatalf("expected sorted order, got %s >= %s", first, second)
	}

	// List with pagination
	req = httptest.NewRequest(http.MethodGet, "/v2/projects/my-project/inspectTemplates?pageSize=2", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	json.NewDecoder(w.Body).Decode(&resp)
	templates = resp["inspectTemplates"].([]any)
	if len(templates) != 2 {
		t.Fatalf("expected 2 templates on first page, got %d", len(templates))
	}
	nextToken, _ := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken for pagination")
	}
}

func TestDeleteTemplate(t *testing.T) {
	api := newAPI(nil)
	// Create
	body := `{"inspectTemplate":{"displayName":"del"},"templateId":"del-tmpl"}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/inspectTemplates", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d", w.Code)
	}

	// Delete
	req = httptest.NewRequest(http.MethodDelete, "/v2/projects/my-project/inspectTemplates/del-tmpl", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify gone
	req = httptest.NewRequest(http.MethodGet, "/v2/projects/my-project/inspectTemplates/del-tmpl", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", w.Code)
	}
}

func TestPatchTemplate(t *testing.T) {
	api := newAPI(nil)
	// Create
	body := `{"inspectTemplate":{"displayName":"original","description":"orig desc","inspectConfig":{"infoTypes":[{"name":"EMAIL_ADDRESS"}]}},"templateId":"patch-tmpl"}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/inspectTemplates", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d: %s", w.Code, w.Body.String())
	}
	var created InspectTemplate
	json.NewDecoder(w.Body).Decode(&created)

	// Patch
	patchBody := `{"inspectTemplate":{"displayName":"updated","inspectConfig":{"infoTypes":[{"name":"PHONE_NUMBER"}]}}}`
	req = httptest.NewRequest(http.MethodPatch, "/v2/projects/my-project/inspectTemplates/patch-tmpl", strings.NewReader(patchBody))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var patched InspectTemplate
	json.NewDecoder(w.Body).Decode(&patched)

	if patched.DisplayName != "updated" {
		t.Fatalf("expected updated displayName, got %s", patched.DisplayName)
	}
	if patched.Description != "orig desc" {
		t.Fatalf("description should be preserved, got %s", patched.Description)
	}
	if patched.CreateTime != created.CreateTime {
		t.Fatal("createTime should be preserved")
	}
	if patched.UpdateTime == created.UpdateTime {
		t.Fatal("updateTime should have changed")
	}
}

func TestInspectContent(t *testing.T) {
	api := newAPI(nil)
	body := `{"item":{"value":"contact user@example.com for info"}}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/content:inspect", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if w.Header().Get("X-MiniSky-Simulated") != "true" {
		t.Fatal("expected X-MiniSky-Simulated: true header")
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	result, ok := resp["result"].(map[string]any)
	if !ok {
		t.Fatal("expected result object")
	}
	findings, ok := result["findings"].([]any)
	if !ok || len(findings) == 0 {
		t.Fatal("expected findings")
	}

	// Verify finding structure
	finding := findings[0].(map[string]any)
	infoType := finding["infoType"].(map[string]any)
	if infoType["name"] != "EMAIL_ADDRESS" {
		t.Fatalf("expected EMAIL_ADDRESS infoType, got %v", infoType["name"])
	}
	if finding["likelihood"] != "LIKELY" {
		t.Fatalf("expected LIKELY likelihood, got %v", finding["likelihood"])
	}
	if finding["quote"] == nil {
		t.Fatal("expected quote field")
	}
	location := finding["location"].(map[string]any)
	byteRange := location["byteRange"].(map[string]any)
	if byteRange["start"] == nil || byteRange["end"] == nil {
		t.Fatal("expected byteRange with start and end")
	}
}

func TestInspectContentMissingItem(t *testing.T) {
	api := newAPI(nil)

	// Empty item value
	body := `{"item":{"value":""}}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/content:inspect", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}

	// Missing item entirely
	body = `{}`
	req = httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/content:inspect", strings.NewReader(body))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing item, got %d: %s", w.Code, w.Body.String())
	}
}

func TestDeidentifyContent(t *testing.T) {
	api := newAPI(nil)
	body := `{"item":{"value":"email: user@example.com"}}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/content:deidentify", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if w.Header().Get("X-MiniSky-Simulated") != "true" {
		t.Fatal("expected X-MiniSky-Simulated: true header")
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	item := resp["item"].(map[string]any)
	if item["value"] != "[REDACTED]" {
		t.Fatalf("expected [REDACTED], got %v", item["value"])
	}
	overview := resp["overview"].(map[string]any)
	if overview["transformedBytes"] == nil {
		t.Fatal("expected transformedBytes in overview")
	}
	summaries := overview["transformationSummaries"].([]any)
	if summaries == nil {
		t.Fatal("expected transformationSummaries array")
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := NewAPIWithStore(store)

	// Create a template
	body := `{"inspectTemplate":{"displayName":"persist-test","inspectConfig":{"infoTypes":[{"name":"EMAIL_ADDRESS"}]}},"templateId":"persist-tmpl"}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/inspectTemplates", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d: %s", w.Code, w.Body.String())
	}

	// Persist synchronously for test
	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	// Create a new API and reload
	api2 := NewAPIWithStore(store)

	// Verify template survived reload
	req = httptest.NewRequest(http.MethodGet, "/v2/projects/my-project/inspectTemplates/persist-tmpl", nil)
	w = httptest.NewRecorder()
	api2.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 after reload, got %d: %s", w.Code, w.Body.String())
	}
	var resp InspectTemplate
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.DisplayName != "persist-test" {
		t.Fatalf("expected persist-test, got %s", resp.DisplayName)
	}
	if resp.InspectConfig == nil {
		t.Fatal("inspectConfig lost after reload")
	}
}

func TestConcurrentTemplateOps(t *testing.T) {
	api := newAPI(nil)
	const n = 50
	var wg sync.WaitGroup

	// Concurrent creates
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"inspectTemplate":{"displayName":"tmpl-%d"},"templateId":"tmpl-%d"}`, idx, idx)
			req := httptest.NewRequest(http.MethodPost, "/v2/projects/my-project/inspectTemplates", strings.NewReader(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK && w.Code != http.StatusConflict {
				t.Errorf("unexpected status %d for create %d", w.Code, idx)
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/v2/projects/my-project/inspectTemplates", nil)
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d for list", w.Code)
			}
		}()
	}

	wg.Wait()
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

// mockStore is a simple in-memory state store for testing.
type mockStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (m *mockStore) Load(name string, target any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, ok := m.data[name]
	if !ok {
		return fmt.Errorf("not found: %w", state.ErrNotFound)
	}
	return json.Unmarshal(raw, target)
}

func (m *mockStore) Save(name string, value any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[name] = raw
	return nil
}
