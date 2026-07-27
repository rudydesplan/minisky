package cloudasset

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestListAssets(t *testing.T) {
	api := NewAPI()
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/my-project/assets?contentType=RESOURCE", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify X-MiniSky-Simulated header
	if w.Header().Get("X-MiniSky-Simulated") != "true" {
		t.Fatal("expected X-MiniSky-Simulated: true header")
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	assets, ok := resp["assets"].([]any)
	if !ok || len(assets) == 0 {
		t.Fatal("expected non-empty assets list")
	}
	if _, ok := resp["nextPageToken"]; !ok {
		t.Fatal("expected nextPageToken field")
	}

	// Verify asset structure
	first := assets[0].(map[string]any)
	if _, ok := first["name"]; !ok {
		t.Fatal("expected name field on asset")
	}
	if _, ok := first["assetType"]; !ok {
		t.Fatal("expected assetType field on asset")
	}
	if _, ok := first["updateTime"]; !ok {
		t.Fatal("expected updateTime field on asset")
	}
}

func TestSearchResources(t *testing.T) {
	api := NewAPI()

	// Test with GET (standard)
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/my-project:searchAllResources?query=instance&pageSize=10", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	results, ok := resp["results"].([]any)
	if !ok || len(results) == 0 {
		t.Fatal("expected non-empty results for query 'instance'")
	}
	if _, ok := resp["nextPageToken"]; !ok {
		t.Fatal("expected nextPageToken field")
	}

	// Verify result structure
	first := results[0].(map[string]any)
	for _, field := range []string{"name", "assetType", "project", "displayName", "location", "createTime"} {
		if _, ok := first[field]; !ok {
			t.Fatalf("expected %s field on search result", field)
		}
	}

	// POST is not the official verb and must fail explicitly.
	req = httptest.NewRequest(http.MethodPost, "/v1/projects/my-project:searchAllResources?query=instance", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("POST searchAllResources: expected 501, got %d: %s", w.Code, w.Body.String())
	}

	// Test with query that matches nothing
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/my-project:searchAllResources?query=nonexistent-xyz", nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	json.NewDecoder(w.Body).Decode(&resp)
	results, ok = resp["results"].([]any)
	if !ok {
		t.Fatal("expected results array")
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for nonexistent query, got %d", len(results))
	}
}

func TestExportAssetsUnimplemented(t *testing.T) {
	api := NewAPI()
	body := `{"outputConfig":{"gcsDestination":{"uri":"gs://bucket/export"}},"assetTypes":["compute.googleapis.com/Instance"]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/my-project:exportAssets", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	errObj, ok := resp["error"].(map[string]any)
	if !ok {
		t.Fatal("expected error object in response")
	}
	if errObj["status"] != "UNIMPLEMENTED" {
		t.Fatalf("expected UNIMPLEMENTED status, got %v", errObj["status"])
	}
	code, _ := errObj["code"].(float64)
	if int(code) != 501 {
		t.Fatalf("expected error code 501, got %v", errObj["code"])
	}
}

func TestExportAssetsRejectsParentSwapToSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated Windows privileges")
	}
	root := t.TempDir()
	outside := t.TempDir()
	parent := filepath.Join(root, "safe")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	api := NewAPIWithInventory(assetInventory{}, root)
	api.beforeExportWrite = func() {
		if err := os.Rename(parent, parent+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, parent); err != nil {
			t.Fatal(err)
		}
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodPost,
		"/v1/projects/p:exportAssets",
		strings.NewReader(`{"outputConfig":{"localDestination":{"path":"safe/export.json"}}}`)))
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(filepath.Join(outside, "export.json")); !os.IsNotExist(err) {
		t.Fatalf("export escaped root: %v", err)
	}
}

func TestListAssetsPagination(t *testing.T) {
	api := NewAPI()

	// Request with pageSize=1 to force pagination
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/my-project/assets?pageSize=1", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	assets := resp["assets"].([]any)
	if len(assets) != 1 {
		t.Fatalf("expected 1 asset on first page, got %d", len(assets))
	}
	nextToken, _ := resp["nextPageToken"].(string)
	if nextToken == "" {
		t.Fatal("expected non-empty nextPageToken for pagination")
	}

	// Fetch second page
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/my-project/assets?pageSize=1&pageToken="+nextToken, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("page 2: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	json.NewDecoder(w.Body).Decode(&resp)
	assets = resp["assets"].([]any)
	if len(assets) != 1 {
		t.Fatalf("expected 1 asset on second page, got %d", len(assets))
	}
	nextToken2, _ := resp["nextPageToken"].(string)
	if nextToken2 == "" {
		t.Fatal("expected non-empty nextPageToken for third page")
	}

	// Fetch third (last) page
	req = httptest.NewRequest(http.MethodGet, "/v1/projects/my-project/assets?pageSize=1&pageToken="+nextToken2, nil)
	w = httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("page 3: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	json.NewDecoder(w.Body).Decode(&resp)
	assets = resp["assets"].([]any)
	if len(assets) != 1 {
		t.Fatalf("expected 1 asset on third page, got %d", len(assets))
	}
	finalToken, _ := resp["nextPageToken"].(string)
	if finalToken != "" {
		t.Fatalf("expected empty nextPageToken on last page, got %q", finalToken)
	}
}
