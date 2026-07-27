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
