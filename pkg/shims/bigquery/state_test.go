package bigquery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"minisky/pkg/state"
)

func TestMetadataRehydratesAfterRestart(t *testing.T) {
	t.Parallel()

	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}

	datasetBody := `{"datasetReference":{"datasetId":"analytics"},"description":"restart test"}`
	request := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/demo/datasets", strings.NewReader(datasetBody))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("create dataset status = %d, body = %s", response.Code, response.Body.String())
	}

	tableBody := `{"tableReference":{"tableId":"events"},"schema":{"fields":[{"name":"id","type":"STRING","mode":"REQUIRED"}]}}`
	request = httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/demo/datasets/analytics/tables", strings.NewReader(tableBody))
	response = httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("create table status = %d, body = %s", response.Code, response.Body.String())
	}

	restarted, err := NewAPIWithStore(nil, store)
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/demo/datasets/analytics/tables/events", nil)
	response = httptest.NewRecorder()
	restarted.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("rehydrated table status = %d, body = %s", response.Code, response.Body.String())
	}
	var table Table
	if err := json.NewDecoder(response.Body).Decode(&table); err != nil {
		t.Fatal(err)
	}
	if table.TableReference.TableId != "events" || table.Schema == nil || len(table.Schema.Fields) != 1 {
		t.Fatalf("rehydrated table = %#v", table)
	}
}
