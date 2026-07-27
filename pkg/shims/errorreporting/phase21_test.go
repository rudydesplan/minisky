package errorreporting

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"minisky/pkg/state"
)

func TestGroupStatsPageTokenIsProjectBound(t *testing.T) {
	api := newTestAPI()
	for _, project := range []string{"p1", "p2"} {
		for _, message := range []string{"first", "second"} {
			rec := httptest.NewRecorder()
			path := "/v1beta1/projects/" + project + "/events:report"
			api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(`{"message":"`+message+`"}`)))
			if rec.Code != http.StatusOK {
				t.Fatalf("seed status=%d: %s", rec.Code, rec.Body.String())
			}
		}
	}
	first := httptest.NewRecorder()
	api.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v1beta1/projects/p1/groupStats?pageSize=1", nil))
	var page struct {
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil || page.NextPageToken == "" {
		t.Fatalf("decode page token: %v, body=%s", err, first.Body.String())
	}

	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1beta1/projects/p2/groupStats?pageSize=1&pageToken="+page.NextPageToken, nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400 for cross-project token: %s", rec.Code, rec.Body.String())
	}
}

func TestReportEventRollsBackGroupAndEventOnSaveFailure(t *testing.T) {
	api := &API{
		stateStore: failingErrorStore{},
		groups:     make(map[string]*ErrorGroupStats),
		events:     make(map[string][]ErrorEvent),
	}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1beta1/projects/p1/events:report", bytes.NewBufferString(`{"message":"secret-token-value"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
	}
	if len(api.groups) != 0 || len(api.events) != 0 {
		t.Fatalf("failed durable report remained visible: groups=%d events=%d", len(api.groups), len(api.events))
	}
	if bytes.Contains(rec.Body.Bytes(), []byte("secret-token-value")) {
		t.Fatal("error response leaked event payload")
	}
}

type failingErrorStore struct{}

func (failingErrorStore) Load(string, any) error { return state.ErrNotFound }
func (failingErrorStore) Save(string, any) error { return errors.New("disk full") }
