package cloudtrace

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"minisky/pkg/state"
)

func TestBatchWriteRejectsCrossProjectSpan(t *testing.T) {
	api := newTestAPI()
	body := `{"spans":[{"name":"projects/other/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/spans/bbbbbbbbbbbbbbbb","spanId":"bbbbbbbbbbbbbbbb"}]}`
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/projects/p1/traces:batchWrite", bytes.NewBufferString(body)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400: %s", rec.Code, rec.Body.String())
	}
	if len(api.traces) != 0 {
		t.Fatal("cross-project span mutated trace state")
	}
}

func TestTracePageTokenIsScopeBound(t *testing.T) {
	api := newTestAPI()
	api.traces["p1:a"] = &Trace{TraceId: "a", ProjectId: "p1"}
	api.traces["p1:b"] = &Trace{TraceId: "b", ProjectId: "p1"}
	api.traces["p2:a"] = &Trace{TraceId: "a", ProjectId: "p2"}
	api.traces["p2:b"] = &Trace{TraceId: "b", ProjectId: "p2"}

	first := httptest.NewRecorder()
	api.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v1/projects/p1/traces?pageSize=1", nil))
	token := decodeToken(t, first.Body.Bytes())

	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/projects/p2/traces?pageSize=1&pageToken="+token, nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400 for cross-scope token: %s", rec.Code, rec.Body.String())
	}
}

func TestBatchWriteRollsBackOnSaveFailure(t *testing.T) {
	api := &API{
		stateStore: failingTraceStore{},
		traces:     make(map[string]*Trace),
	}
	body := `{"spans":[{"name":"projects/p1/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/spans/bbbbbbbbbbbbbbbb","spanId":"bbbbbbbbbbbbbbbb"}]}`
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/projects/p1/traces:batchWrite", bytes.NewBufferString(body)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
	}
	if len(api.traces) != 0 {
		t.Fatal("failed durable write remained visible")
	}
}

func TestBatchWriteUpsertsSpanIdentity(t *testing.T) {
	api := newTestAPI()
	path := "/v2/projects/p1/traces:batchWrite"
	name := "projects/p1/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/spans/bbbbbbbbbbbbbbbb"
	for _, displayName := range []string{"first", "updated"} {
		body := `{"spans":[{"name":"` + name + `","spanId":"bbbbbbbbbbbbbbbb","displayName":{"value":"` + displayName + `"}}]}`
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body)))
		if rec.Code != http.StatusOK {
			t.Fatalf("batchWrite status=%d: %s", rec.Code, rec.Body.String())
		}
	}

	trace := api.traces["p1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
	if trace == nil || len(trace.Spans) != 1 {
		t.Fatalf("duplicate span identity was appended: %+v", trace)
	}
	if trace.Spans[0].DisplayName == nil || trace.Spans[0].DisplayName.Value != "updated" {
		t.Fatalf("span was not replaced by latest write: %+v", trace.Spans[0])
	}
}

func TestBatchWriteCollapsesLegacyDuplicateSpansAcrossReload(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	traceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	spanID := "bbbbbbbbbbbbbbbb"
	name := "projects/p1/traces/" + traceID + "/spans/" + spanID
	legacy := cloudtraceMetadata{Traces: map[string]*Trace{
		"p1:" + traceID: {
			TraceId:   traceID,
			ProjectId: "p1",
			Spans: []Span{
				{Name: name, SpanId: spanID, DisplayName: &TruncatableString{Value: "legacy-1"}},
				{Name: "projects/p1/traces/" + traceID + "/spans/cccccccccccccccc", SpanId: "cccccccccccccccc", DisplayName: &TruncatableString{Value: "other"}},
				{Name: name, SpanId: spanID, DisplayName: &TruncatableString{Value: "legacy-2"}},
			},
		},
	}}
	if err := store.Save(cloudtraceStateEntry, legacy); err != nil {
		t.Fatal(err)
	}
	api := &API{stateStore: store, traces: make(map[string]*Trace)}
	if err := api.loadState(); err != nil {
		t.Fatal(err)
	}

	body := `{"spans":[{"name":"` + name + `","spanId":"` + spanID + `","displayName":{"value":"canonical"}}]}`
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/projects/p1/traces:batchWrite", bytes.NewBufferString(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("batchWrite status=%d: %s", rec.Code, rec.Body.String())
	}

	restarted := &API{stateStore: store, traces: make(map[string]*Trace)}
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	trace := restarted.traces["p1:"+traceID]
	if trace == nil || len(trace.Spans) != 2 {
		t.Fatalf("reloaded trace retained duplicate identities: %+v", trace)
	}
	matches := 0
	for _, span := range trace.Spans {
		if span.SpanId == spanID {
			matches++
			if span.DisplayName == nil || span.DisplayName.Value != "canonical" {
				t.Fatalf("canonical span was not persisted: %+v", span)
			}
		}
	}
	if matches != 1 {
		t.Fatalf("reloaded canonical span count=%d, want 1", matches)
	}
}

func TestBatchWriteRejectsMismatchedSpanIdentity(t *testing.T) {
	api := newTestAPI()
	body := `{"spans":[{"name":"projects/p1/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/spans/cccccccccccccccc","spanId":"bbbbbbbbbbbbbbbb"}]}`
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/projects/p1/traces:batchWrite", bytes.NewBufferString(body)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400: %s", rec.Code, rec.Body.String())
	}
	if len(api.traces) != 0 {
		t.Fatal("mismatched span identity mutated trace state")
	}
}

func TestBatchWriteRejectsMalformedSpanNamesAndIDs(t *testing.T) {
	tests := []struct {
		name     string
		spanName string
		spanID   string
	}{
		{"extra segment", "projects/p1/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/spans/bbbbbbbbbbbbbbbb/extra", "bbbbbbbbbbbbbbbb"},
		{"wrong root", "folders/p1/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/spans/bbbbbbbbbbbbbbbb", "bbbbbbbbbbbbbbbb"},
		{"wrong trace segment", "projects/p1/trace/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/spans/bbbbbbbbbbbbbbbb", "bbbbbbbbbbbbbbbb"},
		{"short trace ID", "projects/p1/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/spans/bbbbbbbbbbbbbbbb", "bbbbbbbbbbbbbbbb"},
		{"non-hex trace ID", "projects/p1/traces/gggggggggggggggggggggggggggggggg/spans/bbbbbbbbbbbbbbbb", "bbbbbbbbbbbbbbbb"},
		{"short span ID", "projects/p1/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/spans/bbbbbbbbbbbbbbb", "bbbbbbbbbbbbbbb"},
		{"non-hex span ID", "projects/p1/traces/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/spans/zzzzzzzzzzzzzzzz", "zzzzzzzzzzzzzzzz"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newTestAPI()
			body, err := json.Marshal(map[string]any{"spans": []map[string]string{{
				"name": test.spanName, "spanId": test.spanID,
			}}})
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
				"/v2/projects/p1/traces:batchWrite", bytes.NewReader(body)))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d, want 400: %s", rec.Code, rec.Body.String())
			}
			if len(api.traces) != 0 {
				t.Fatal("malformed span name mutated trace state")
			}
		})
	}
}

func TestBatchWriteProjectClassification(t *testing.T) {
	const traceID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const spanID = "bbbbbbbbbbbbbbbb"
	tests := []struct {
		name    string
		project string
		want    int
	}{
		{"single letter project ID", "p", http.StatusOK},
		{"lowercase project ID", "project-1", http.StatusOK},
		{"numeric project number", "123456789012", http.StatusOK},
		{"mixed digit-leading letters", "1project", http.StatusBadRequest},
		{"mixed digit-leading hyphen", "1-234", http.StatusBadRequest},
		{"leading hyphen", "-project", http.StatusBadRequest},
		{"trailing hyphen", "project-", http.StatusBadRequest},
		{"uppercase", "Project", http.StatusBadRequest},
		{"encoded lowercase alias", "%70roject", http.StatusBadRequest},
		{"double encoded alias", "%2570roject", http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := newTestAPI()
			body := `{"spans":[{"name":"projects/` + test.project + `/traces/` + traceID +
				`/spans/` + spanID + `","spanId":"` + spanID + `"}]}`
			rec := httptest.NewRecorder()
			api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
				"/v2/projects/"+test.project+"/traces:batchWrite", bytes.NewBufferString(body)))
			if rec.Code != test.want {
				t.Fatalf("status=%d, want %d: %s", rec.Code, test.want, rec.Body.String())
			}
			if test.want != http.StatusOK && len(api.traces) != 0 {
				t.Fatal("invalid project mutated trace state")
			}
		})
	}
}

func TestBatchWriteRejectsEncodedAndAmbiguousProjects(t *testing.T) {
	const traceID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const spanID = "bbbbbbbbbbbbbbbb"
	validBody := `{"spans":[{"name":"projects/p1/traces/` + traceID + `/spans/` + spanID + `","spanId":"` + spanID + `"}]}`
	requestProjects := []string{
		"%70%31",
		"%2570%2531",
		"p1%2Falias",
		"p1%252Falias",
		"p1%3Aalias",
		"p1%5Calias",
		"p1%0Aalias",
		"%2E",
		".",
		"..",
		"-p1",
		"p1-",
		"P1",
	}
	for _, project := range requestProjects {
		t.Run("request_"+project, func(t *testing.T) {
			api := newTestAPI()
			rec := httptest.NewRecorder()
			api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
				"/v2/projects/"+project+"/traces:batchWrite", bytes.NewBufferString(validBody)))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d, want 400: %s", rec.Code, rec.Body.String())
			}
			if len(api.traces) != 0 {
				t.Fatal("ambiguous request project mutated trace state")
			}
		})
	}

	spanProjects := []string{
		"%70%31",
		"%2570%2531",
		"p1%2Falias",
		"p1%252Falias",
		"p1:alias",
		`p1\alias`,
		"p1\nalias",
		".",
		"..",
		"-p1",
		"p1-",
		"P1",
	}
	for _, project := range spanProjects {
		t.Run("span_"+project, func(t *testing.T) {
			api := newTestAPI()
			body, err := json.Marshal(map[string]any{"spans": []map[string]string{{
				"name":   "projects/" + project + "/traces/" + traceID + "/spans/" + spanID,
				"spanId": spanID,
			}}})
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
				"/v2/projects/p1/traces:batchWrite", bytes.NewReader(body)))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status=%d, want 400: %s", rec.Code, rec.Body.String())
			}
			if len(api.traces) != 0 {
				t.Fatal("ambiguous span project mutated trace state")
			}
		})
	}
}

type failingTraceStore struct{}

func (failingTraceStore) Load(string, any) error { return state.ErrNotFound }
func (failingTraceStore) Save(string, any) error { return errors.New("disk full") }

func decodeToken(t *testing.T, body []byte) string {
	t.Helper()
	var response struct {
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.NextPageToken == "" {
		t.Fatal("missing nextPageToken")
	}
	return response.NextPageToken
}
