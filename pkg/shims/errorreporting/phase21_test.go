package errorreporting

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
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

func TestReportEventRetainsBoundedRecentEventsAndTotalCount(t *testing.T) {
	api := newTestAPI()
	const reports = 101
	for index := 0; index < reports; index++ {
		body := fmt.Sprintf(`{"message":"same error\noccurrence %d","eventTime":"2026-01-01T00:%02d:00Z"}`, index, index%60)
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1beta1/projects/p1/events:report", bytes.NewBufferString(body)))
		if rec.Code != http.StatusOK {
			t.Fatalf("report %d status=%d: %s", index, rec.Code, rec.Body.String())
		}
	}

	key := "p1:" + generateGroupId("same error")
	group := api.groups[key]
	if group == nil || group.Count != "101" {
		t.Fatalf("aggregate count = %+v, want 101", group)
	}
	if got := len(api.events[key]); got != 100 {
		t.Fatalf("retained events=%d, want 100", got)
	}
	if got := api.events[key][0].Message; got != "same error\noccurrence 1" {
		t.Fatalf("oldest retained event=%q, want occurrence 1", got)
	}
}

func TestLoadStateMigratesLegacyEventsToRetentionLimit(t *testing.T) {
	store := &mockStore{data: make(map[string][]byte)}
	groupID := generateGroupId("legacy error")
	key := "p1:" + groupID
	events := make([]ErrorEvent, 105)
	for index := range events {
		events[index] = ErrorEvent{Message: fmt.Sprintf("legacy error\noccurrence %d", index)}
	}
	legacy := errorreportingMetadata{
		Groups: map[string]*ErrorGroupStats{key: {
			Group: &ErrorGroupInfo{
				GroupId: groupID,
				Name:    "projects/p1/groups/" + groupID,
			},
			Count: "105",
		}},
		Events: map[string][]ErrorEvent{key: events},
	}
	if err := store.Save(errorreportingStateEntry, legacy); err != nil {
		t.Fatal(err)
	}

	api := &API{stateStore: store, groups: make(map[string]*ErrorGroupStats), events: make(map[string][]ErrorEvent)}
	if err := api.loadState(); err != nil {
		t.Fatal(err)
	}
	if api.groups[key].Count != "105" {
		t.Fatalf("aggregate count=%q, want 105", api.groups[key].Count)
	}
	if got := len(api.events[key]); got != maxRetainedEventsPerGroup {
		t.Fatalf("migrated events=%d, want %d", got, maxRetainedEventsPerGroup)
	}
	if got := api.events[key][0].Message; got != "legacy error\noccurrence 5" {
		t.Fatalf("oldest migrated event=%q, want occurrence 5", got)
	}

	restarted := &API{stateStore: store, groups: make(map[string]*ErrorGroupStats), events: make(map[string][]ErrorEvent)}
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if restarted.groups[key].Count != "105" || len(restarted.events[key]) != maxRetainedEventsPerGroup {
		t.Fatalf("migration did not survive restart: group=%+v events=%d",
			restarted.groups[key], len(restarted.events[key]))
	}
}

func TestMigrationSaveFailurePreservesReadsAndBlocksMutation(t *testing.T) {
	groupID := generateGroupId("legacy error")
	key := "p1:" + groupID
	events := make([]ErrorEvent, 105)
	for index := range events {
		events[index] = ErrorEvent{Message: fmt.Sprintf("legacy error\noccurrence %d", index)}
	}
	legacy := errorreportingMetadata{
		Groups: map[string]*ErrorGroupStats{key: {
			Group: &ErrorGroupInfo{
				GroupId: groupID,
				Name:    "projects/p1/groups/" + groupID,
			},
			Count: "105",
		}},
		Events: map[string][]ErrorEvent{key: events},
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	store := &oneShotMigrationFailStore{data: raw}
	api := &API{stateStore: store, groups: make(map[string]*ErrorGroupStats), events: make(map[string][]ErrorEvent)}
	if err := api.loadState(); err == nil {
		t.Fatal("migration save failure was not reported")
	}

	read := httptest.NewRecorder()
	api.ServeHTTP(read, httptest.NewRequest(http.MethodGet,
		"/v1beta1/projects/p1/events?groupId="+groupID, nil))
	if read.Code != http.StatusOK {
		t.Fatalf("safe read status=%d: %s", read.Code, read.Body.String())
	}
	var listed struct {
		Events []ErrorEvent `json:"errorEvents"`
	}
	if err := json.Unmarshal(read.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Events) != maxRetainedEventsPerGroup ||
		listed.Events[0].Message != "legacy error\noccurrence 5" {
		t.Fatalf("loaded migration view is not truthful: %+v", listed.Events)
	}

	report := httptest.NewRecorder()
	api.ServeHTTP(report, httptest.NewRequest(http.MethodPost,
		"/v1beta1/projects/p1/events:report", bytes.NewBufferString(`{"message":"new error"}`)))
	if report.Code != http.StatusServiceUnavailable {
		t.Fatalf("report status=%d, want 503: %s", report.Code, report.Body.String())
	}
	var envelope struct {
		Error struct {
			Code   int    `json:"code"`
			Status string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal(report.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != http.StatusServiceUnavailable || envelope.Error.Status != "UNAVAILABLE" {
		t.Fatalf("unexpected error envelope: %+v", envelope.Error)
	}
	if store.saves() != 1 {
		t.Fatalf("degraded report attempted durable overwrite: saves=%d", store.saves())
	}

	restarted := &API{stateStore: store, groups: make(map[string]*ErrorGroupStats), events: make(map[string][]ErrorEvent)}
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if restarted.groups[key].Count != "105" || len(restarted.events[key]) != maxRetainedEventsPerGroup {
		t.Fatalf("reload lost legacy aggregate: group=%+v events=%d",
			restarted.groups[key], len(restarted.events[key]))
	}
	if _, exists := restarted.groups["p1:"+generateGroupId("new error")]; exists {
		t.Fatal("rejected report overwrote durable legacy state")
	}
}

type oneShotMigrationFailStore struct {
	mu        sync.Mutex
	data      []byte
	saveCount int
}

func (store *oneShotMigrationFailStore) Load(_ string, target any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return json.Unmarshal(store.data, target)
}

func (store *oneShotMigrationFailStore) Save(_ string, value any) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.saveCount++
	if store.saveCount == 1 {
		return errors.New("transient migration write failure")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	store.data = raw
	return nil
}

func (store *oneShotMigrationFailStore) saves() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.saveCount
}

type failingErrorStore struct{}

func (failingErrorStore) Load(string, any) error { return state.ErrNotFound }
func (failingErrorStore) Save(string, any) error { return errors.New("disk full") }
