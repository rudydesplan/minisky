package cloudtrace

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestBatchWrite(t *testing.T) {
	api := newTestAPI()
	body := `{"spans":[{"name":"projects/p1/traces/abcdef0123456789abcdef0123456789/spans/0123456789abcdef","spanId":"0123456789abcdef","displayName":{"value":"my-span","truncatedByteCount":0},"startTime":"2024-01-01T00:00:00Z","endTime":"2024-01-01T00:00:01Z","attributes":{"attributeMap":{"key":{"stringValue":{"value":"val"}}}},"status":{"code":0,"message":""}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/p1/traces:batchWrite", bytes.NewBufferString(body))
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

	// Verify trace was stored
	api.mu.RLock()
	trace := api.traces["p1:abcdef0123456789abcdef0123456789"]
	api.mu.RUnlock()
	if trace == nil {
		t.Fatal("trace not stored")
	}
	if len(trace.Spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(trace.Spans))
	}
	if trace.Spans[0].SpanId != "0123456789abcdef" {
		t.Fatalf("unexpected spanId: %s", trace.Spans[0].SpanId)
	}
	if trace.Spans[0].DisplayName == nil || trace.Spans[0].DisplayName.Value != "my-span" {
		t.Fatal("displayName not preserved")
	}
	if trace.Spans[0].Attributes == nil || trace.Spans[0].Attributes.AttributeMap == nil {
		t.Fatal("attributes not preserved")
	}
}

func TestBatchWriteMissingSpans(t *testing.T) {
	api := newTestAPI()

	// Empty spans array
	body := `{"spans":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v2/projects/p1/traces:batchWrite", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty spans, got %d: %s", w.Code, w.Body.String())
	}

	// Missing spans field entirely
	body = `{}`
	req = httptest.NewRequest(http.MethodPost, "/v2/projects/p1/traces:batchWrite", bytes.NewBufferString(body))
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing spans, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetTrace(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.traces["p1:abc123def456"] = &Trace{
		TraceId:   "abc123def456",
		ProjectId: "p1",
		Spans: []Span{
			{SpanId: "span1", DisplayName: &TruncatableString{Value: "root"}},
			{SpanId: "span2", ParentSpanId: "span1", DisplayName: &TruncatableString{Value: "child"}},
		},
	}
	api.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/v1/projects/p1/traces/abc123def456", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var trace Trace
	if err := json.Unmarshal(w.Body.Bytes(), &trace); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if trace.TraceId != "abc123def456" {
		t.Fatalf("unexpected traceId: %s", trace.TraceId)
	}
	if trace.ProjectId != "p1" {
		t.Fatalf("unexpected projectId: %s", trace.ProjectId)
	}
	if len(trace.Spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(trace.Spans))
	}
}

func TestGetTraceNotFound(t *testing.T) {
	api := newTestAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/p1/traces/nonexistent", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestListTraces(t *testing.T) {
	api := newTestAPI()
	api.mu.Lock()
	api.traces["p1:aaa"] = &Trace{TraceId: "aaa", ProjectId: "p1", Spans: []Span{{SpanId: "s1"}}}
	api.traces["p1:bbb"] = &Trace{TraceId: "bbb", ProjectId: "p1", Spans: []Span{{SpanId: "s2"}}}
	api.traces["p1:ccc"] = &Trace{TraceId: "ccc", ProjectId: "p1", Spans: []Span{{SpanId: "s3"}}}
	api.traces["p2:ddd"] = &Trace{TraceId: "ddd", ProjectId: "p2", Spans: []Span{{SpanId: "s4"}}}
	api.mu.Unlock()

	// First page: pageSize=2
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/p1/traces?pageSize=2", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	traces := resp["traces"].([]any)
	if len(traces) != 2 {
		t.Fatalf("expected 2 traces, got %d", len(traces))
	}
	// Verify sorted order
	first := traces[0].(map[string]any)["traceId"].(string)
	second := traces[1].(map[string]any)["traceId"].(string)
	if first >= second {
		t.Fatalf("expected sorted order, got %s >= %s", first, second)
	}

	nextToken := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected nextPageToken for pagination")
	}

	// Second page
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/p1/traces?pageSize=2&pageToken="+nextToken, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	traces = resp["traces"].([]any)
	if len(traces) != 1 {
		t.Fatalf("expected 1 trace on second page, got %d", len(traces))
	}
	// Should not include p2 traces
	third := traces[0].(map[string]any)["traceId"].(string)
	if third != "ccc" {
		t.Fatalf("expected trace 'ccc' on second page, got %s", third)
	}
}

func TestPersistAndReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	api := &API{
		stateStore: store,
		traces:     make(map[string]*Trace),
	}

	// Add a trace
	api.mu.Lock()
	api.traces["p1:trace1"] = &Trace{
		TraceId:   "trace1",
		ProjectId: "p1",
		Spans: []Span{
			{
				Name:        "projects/p1/traces/trace1/spans/span1",
				SpanId:      "span1",
				DisplayName: &TruncatableString{Value: "test-span"},
				StartTime:   "2024-01-01T00:00:00Z",
				EndTime:     "2024-01-01T00:00:01Z",
			},
		},
	}
	api.mu.Unlock()

	// Persist
	if err := api.persistState(); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	// Create a new API and reload
	api2 := &API{
		stateStore: store,
		traces:     make(map[string]*Trace),
	}
	if err := api2.loadState(); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	api2.mu.RLock()
	trace, ok := api2.traces["p1:trace1"]
	api2.mu.RUnlock()
	if !ok {
		t.Fatal("trace not found after reload")
	}
	if trace.TraceId != "trace1" {
		t.Fatalf("expected traceId 'trace1', got %s", trace.TraceId)
	}
	if len(trace.Spans) != 1 {
		t.Fatalf("expected 1 span after reload, got %d", len(trace.Spans))
	}
	if trace.Spans[0].DisplayName == nil || trace.Spans[0].DisplayName.Value != "test-span" {
		t.Fatal("span displayName lost after reload")
	}
}

func TestConcurrentBatchWrite(t *testing.T) {
	api := newTestAPI()
	const n = 50
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			traceId := fmt.Sprintf("%032x", idx)
			spanId := fmt.Sprintf("%016x", idx)
			body := fmt.Sprintf(`{"spans":[{"name":"projects/p1/traces/%s/spans/%s","spanId":"%s","displayName":{"value":"span-%d"},"startTime":"2024-01-01T00:00:00Z","endTime":"2024-01-01T00:00:01Z"}]}`, traceId, spanId, spanId, idx)
			req := httptest.NewRequest(http.MethodPost, "/v2/projects/p1/traces:batchWrite", bytes.NewBufferString(body))
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d for write %d: %s", w.Code, idx, w.Body.String())
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/v1/projects/p1/traces", nil)
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("unexpected status %d for list", w.Code)
			}
		}()
	}

	wg.Wait()

	// Verify all traces were written
	api.mu.RLock()
	count := len(api.traces)
	api.mu.RUnlock()
	if count != n {
		t.Fatalf("expected %d traces, got %d", n, count)
	}
}
