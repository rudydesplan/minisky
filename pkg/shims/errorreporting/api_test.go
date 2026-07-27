package errorreporting

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestReportEvent(t *testing.T) {
	api := newTestAPI()
	body := `{"message":"NullPointerException at com.example.Main.run(Main.java:42)","serviceContext":{"service":"my-service","version":"1.0"},"context":{"reportLocation":{"filePath":"Main.java","lineNumber":42,"functionName":"run"}}}`
	req := httptest.NewRequest(http.MethodPost, "/v1beta1/projects/p1/events:report", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify empty response
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if len(resp) != 0 {
		t.Fatalf("expected empty response {}, got %v", resp)
	}

	// Verify group was created
	api.mu.RLock()
	if len(api.groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(api.groups))
	}
	for _, g := range api.groups {
		if g.Count != "1" {
			t.Fatalf("expected count '1', got %s", g.Count)
		}
		if g.Group == nil || g.Group.GroupId == "" {
			t.Fatal("groupId is empty")
		}
		if g.FirstSeenTime == "" {
			t.Fatal("firstSeenTime is empty")
		}
		if g.Representative == nil || g.Representative.Message == "" {
			t.Fatal("representative message is empty")
		}
	}
	api.mu.RUnlock()
}

func TestReportEventMissingMessage(t *testing.T) {
	api := newTestAPI()

	// Empty message
	body := `{"serviceContext":{"service":"svc"},"message":""}`
	req := httptest.NewRequest(http.MethodPost, "/v1beta1/projects/p1/events:report", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty message, got %d: %s", w.Code, w.Body.String())
	}

	// Missing message field
	body = `{"serviceContext":{"service":"svc"}}`
	req = httptest.NewRequest(http.MethodPost, "/v1beta1/projects/p1/events:report", bytes.NewBufferString(body))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing message, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListGroupStats(t *testing.T) {
	api := newTestAPI()

	// Report two different errors to create two groups
	body1 := `{"message":"Error A: something failed","serviceContext":{"service":"svc1"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1beta1/projects/p1/events:report", bytes.NewBufferString(body1))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("report 1 failed: %d", w.Code)
	}

	body2 := `{"message":"Error B: another failure","serviceContext":{"service":"svc2"}}`
	req = httptest.NewRequest(http.MethodPost, "/v1beta1/projects/p1/events:report", bytes.NewBufferString(body2))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("report 2 failed: %d", w.Code)
	}

	// Report same error again to increment count
	req = httptest.NewRequest(http.MethodPost, "/v1beta1/projects/p1/events:report", bytes.NewBufferString(body1))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("report 3 failed: %d", w.Code)
	}

	// List group stats
	req = httptest.NewRequest(http.MethodGet, "/v1beta1/projects/p1/groupStats", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	stats := resp["errorGroupStats"].([]any)
	if len(stats) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(stats))
	}

	// Find the group with count=2
	found := false
	for _, s := range stats {
		stat := s.(map[string]any)
		if stat["count"] == "2" {
			found = true
			group := stat["group"].(map[string]any)
			if group["groupId"] == "" {
				t.Fatal("groupId is empty")
			}
			if group["name"] == "" {
				t.Fatal("group name is empty")
			}
		}
	}
	if !found {
		t.Fatal("expected one group with count=2")
	}

	// Test pagination
	req = httptest.NewRequest(http.MethodGet, "/v1beta1/projects/p1/groupStats?pageSize=1", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	stats = resp["errorGroupStats"].([]any)
	if len(stats) != 1 {
		t.Fatalf("expected 1 group on first page, got %d", len(stats))
	}
	nextToken := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken")
	}

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/v1beta1/projects/p1/groupStats?pageSize=1&pageToken="+nextToken, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	stats = resp["errorGroupStats"].([]any)
	if len(stats) != 1 {
		t.Fatalf("expected 1 group on second page, got %d", len(stats))
	}
}

func TestListEvents(t *testing.T) {
	api := newTestAPI()

	// Report events for two different groups
	body1 := `{"message":"Error X","serviceContext":{"service":"svc1"},"eventTime":"2024-01-01T00:00:00Z"}`
	body2 := `{"message":"Error Y","serviceContext":{"service":"svc2"},"eventTime":"2024-01-01T00:01:00Z"}`

	req := httptest.NewRequest(http.MethodPost, "/v1beta1/projects/p1/events:report", bytes.NewBufferString(body1))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("report 1 failed: %d", w.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1beta1/projects/p1/events:report", bytes.NewBufferString(body2))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("report 2 failed: %d", w.Code)
	}

	// List all events (no groupId filter)
	req = httptest.NewRequest(http.MethodGet, "/v1beta1/projects/p1/events", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	events := resp["errorEvents"].([]any)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// Get the groupId for "Error X"
	groupIdX := generateGroupId("Error X")

	// List events filtered by groupId
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1beta1/projects/p1/events?groupId=%s", groupIdX), nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	events = resp["errorEvents"].([]any)
	if len(events) != 1 {
		t.Fatalf("expected 1 event filtered by groupId, got %d", len(events))
	}
	firstEvent := events[0].(map[string]any)
	if firstEvent["message"] != "Error X" {
		t.Fatalf("expected 'Error X', got %v", firstEvent["message"])
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := &API{
		stateStore: store,
		groups:     make(map[string]*ErrorGroupStats),
		events:     make(map[string][]ErrorEvent),
	}

	// Report an event
	api.mu.Lock()
	groupId := generateGroupId("test error")
	key := "p1:" + groupId
	api.groups[key] = &ErrorGroupStats{
		Group: &ErrorGroupInfo{
			GroupId: groupId,
			Name:    fmt.Sprintf("projects/p1/groups/%s", groupId),
		},
		Count:         "3",
		FirstSeenTime: "2024-01-01T00:00:00Z",
		LastSeenTime:  "2024-01-01T00:02:00Z",
		Representative: &ErrorEvent{
			Message:        "test error",
			ServiceContext: &ServiceContext{Service: "svc"},
		},
	}
	api.events[key] = []ErrorEvent{
		{EventTime: "2024-01-01T00:00:00Z", Message: "test error", ServiceContext: &ServiceContext{Service: "svc"}},
		{EventTime: "2024-01-01T00:01:00Z", Message: "test error", ServiceContext: &ServiceContext{Service: "svc"}},
		{EventTime: "2024-01-01T00:02:00Z", Message: "test error", ServiceContext: &ServiceContext{Service: "svc"}},
	}
	api.mu.Unlock()

	// Persist
	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	// Create a new API and reload
	api2 := &API{
		stateStore: store,
		groups:     make(map[string]*ErrorGroupStats),
		events:     make(map[string][]ErrorEvent),
	}
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	group, ok := api2.groups[key]
	api2.mu.RUnlock()
	if !ok {
		t.Fatal("group not found after reload")
	}
	if group.Count != "3" {
		t.Fatalf("expected count '3', got %s", group.Count)
	}
	if group.Group == nil || group.Group.GroupId != groupId {
		t.Fatal("groupId lost after reload")
	}

	api2.mu.RLock()
	events, ok := api2.events[key]
	api2.mu.RUnlock()
	if !ok {
		t.Fatal("events not found after reload")
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events after reload, got %d", len(events))
	}
}

func TestConcurrentReport(t *testing.T) {
	api := newTestAPI()
	const n = 50
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"message":"error %d","serviceContext":{"service":"svc-%d"}}`, idx, idx)
			req := httptest.NewRequest(http.MethodPost, "/v1beta1/projects/p1/events:report", bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d for report %d: %s", w.Code, idx, w.Body.String())
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/v1beta1/projects/p1/groupStats", nil)
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d for list", w.Code)
			}
		}()
	}

	wg.Wait()

	// Verify all events were stored
	api.mu.RLock()
	totalEvents := 0
	for _, events := range api.events {
		totalEvents += len(events)
	}
	api.mu.RUnlock()
	if totalEvents != n {
		t.Fatalf("expected %d total events, got %d", n, totalEvents)
	}
}
