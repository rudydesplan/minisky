package cloudasset

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type staticInventory []Asset

func (inventory staticInventory) Assets(parent string) []Asset {
	if parent != "projects/demo" {
		return nil
	}
	return append([]Asset(nil), inventory...)
}

type unscopedInventory []Asset

func (inventory unscopedInventory) Assets(string) []Asset {
	return append([]Asset(nil), inventory...)
}

func TestListAssetsUsesInjectedRegistryInventory(t *testing.T) {
	api := NewAPIWithInventory(staticInventory{{
		Name:      "//storage.googleapis.com/projects/_/buckets/assets",
		AssetType: "storage.googleapis.com/Bucket",
		Project:   "projects/demo",
	}}, "")
	req := httptest.NewRequest(http.MethodGet, "/v1/projects/demo/assets", nil)
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)

	var response struct {
		Assets []Asset `json:"assets"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Assets) != 1 || response.Assets[0].Project != "projects/demo" {
		t.Fatalf("assets = %#v", response.Assets)
	}
}

func TestListAssetsEnforcesProjectScopeAgainstInventoryProvider(t *testing.T) {
	api := NewAPIWithInventory(unscopedInventory{
		{Name: "//example/demo", AssetType: "example/Item", Project: "projects/demo"},
		{Name: "//example/other", AssetType: "example/Item", Project: "projects/other"},
		{Name: "//example/unscoped", AssetType: "example/Item"},
	}, "")
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/projects/demo/assets", nil))

	var response struct {
		Assets []Asset `json:"assets"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Assets) != 1 || response.Assets[0].Name != "//example/demo" {
		t.Fatalf("assets = %#v", response.Assets)
	}
}

func TestProjectScopedMethodsRejectMalformedAndUnsupportedParents(t *testing.T) {
	api := NewAPIWithInventory(staticInventory{}, t.TempDir())
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		status int
		symbol string
	}{
		{
			name: "malformed list parent", method: http.MethodGet,
			path: "/v1/projects/demo/extra/assets", status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "traversal search parent", method: http.MethodGet,
			path: "/v1/projects/../:searchAllResources", status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "malformed export parent", method: http.MethodPost, path: "/v1/projects/demo/extra:exportAssets",
			body:   `{"outputConfig":{"localDestination":{"path":"assets.json"}}}`,
			status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "encoded separator", method: http.MethodGet,
			path: "/v1/projects/demo%2Fother/assets", status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "malformed organization extra segment", method: http.MethodGet,
			path: "/v1/organizations/123/extra/assets", status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "malformed organization identifier", method: http.MethodGet,
			path: "/v1/organizations/not-a-number/assets", status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "folder traversal", method: http.MethodGet,
			path: "/v1/folders/../assets", status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "organization inventory unsupported", method: http.MethodGet,
			path: "/v1/organizations/123/assets", status: http.StatusNotImplemented, symbol: "UNIMPLEMENTED",
		},
		{
			name: "folder inventory unsupported", method: http.MethodGet,
			path: "/v1/folders/456/assets", status: http.StatusNotImplemented, symbol: "UNIMPLEMENTED",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			api.ServeHTTP(rec, httptest.NewRequest(test.method, test.path, strings.NewReader(test.body)))
			if rec.Code != test.status {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, test.status, rec.Body.String())
			}
			if contentType := rec.Header().Get("Content-Type"); contentType != "application/json" {
				t.Fatalf("content-type=%q", contentType)
			}
			var envelope struct {
				Error struct {
					Code   int    `json:"code"`
					Status string `json:"status"`
				} `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Error.Code != test.status || envelope.Error.Status != test.symbol {
				t.Fatalf("error envelope = %#v", envelope.Error)
			}
		})
	}
}

func TestUnsupportedParentCanonicalDecimalRouteMatrix(t *testing.T) {
	api := NewAPIWithInventory(staticInventory{}, t.TempDir())
	tests := []struct {
		name   string
		method string
		path   string
		status int
		symbol string
	}{
		{
			name: "organization list", method: http.MethodGet,
			path: "/v1/organizations/123/assets", status: http.StatusNotImplemented, symbol: "UNIMPLEMENTED",
		},
		{
			name: "folder search", method: http.MethodGet,
			path: "/v1/folders/456:searchAllResources", status: http.StatusNotImplemented, symbol: "UNIMPLEMENTED",
		},
		{
			name: "organization export", method: http.MethodPost,
			path: "/v1/organizations/789:exportAssets", status: http.StatusNotImplemented, symbol: "UNIMPLEMENTED",
		},
		{
			name: "maximum uint64 organization", method: http.MethodGet,
			path:   "/v1/organizations/18446744073709551615/assets",
			status: http.StatusNotImplemented, symbol: "UNIMPLEMENTED",
		},
		{
			name: "leading zero organization list", method: http.MethodGet,
			path: "/v1/organizations/0123/assets", status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "leading zero folder search", method: http.MethodGet,
			path: "/v1/folders/0456:searchAllResources", status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "leading zero organization export", method: http.MethodPost,
			path: "/v1/organizations/0789:exportAssets", status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "zero identifier", method: http.MethodGet,
			path: "/v1/folders/0/assets", status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "encoded decimal alias", method: http.MethodGet,
			path: "/v1/organizations/%31%32%33/assets", status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
		{
			name: "decimal overflow", method: http.MethodGet,
			path:   "/v1/organizations/18446744073709551616/assets",
			status: http.StatusBadRequest, symbol: "INVALID_ARGUMENT",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			api.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
			if response.Code != test.status {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.status, response.Body.String())
			}
			var envelope struct {
				Error struct {
					Code   int    `json:"code"`
					Status string `json:"status"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Error.Code != test.status || envelope.Error.Status != test.symbol {
				t.Fatalf("error envelope = %#v", envelope.Error)
			}
		})
	}
}

func TestExportAssetsAllowsOnlyRelativeLocalDestination(t *testing.T) {
	root := t.TempDir()
	api := NewAPIWithInventory(staticInventory{{Name: "//example/item", AssetType: "example/Item", Project: "projects/demo"}}, root)

	req := httptest.NewRequest(http.MethodPost, "/v1/projects/demo:exportAssets",
		strings.NewReader(`{"outputConfig":{"localDestination":{"path":"exports/assets.json"}}}`))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	payload, err := os.ReadFile(filepath.Join(root, "exports", "assets.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"assetType":"example/Item"`) {
		t.Fatalf("export = %s", payload)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/projects/demo:exportAssets",
		strings.NewReader(`{"outputConfig":{"localDestination":{"path":"../escape.json"}}}`))
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("traversal status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(root, "..", "escape.json")); !os.IsNotExist(err) {
		t.Fatalf("escape destination exists: %v", err)
	}
}

func TestExportWithoutConfiguredLocalRootIsExplicitlyUnsupported(t *testing.T) {
	api := NewAPIWithInventory(staticInventory{}, "")
	req := httptest.NewRequest(http.MethodPost, "/v1/projects/demo:exportAssets",
		strings.NewReader(`{"outputConfig":{"localDestination":{"path":"assets.json"}}}`))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestExportRejectsParentSymlinkSwap(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "exports"), 0o700); err != nil {
		t.Fatal(err)
	}
	api := NewAPIWithInventory(staticInventory{{Name: "//example/item", AssetType: "example/Item"}}, root)
	api.beforeExportWrite = func() {
		if err := os.Remove(filepath.Join(root, "exports")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "exports")); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/projects/demo:exportAssets",
		strings.NewReader(`{"outputConfig":{"localDestination":{"path":"exports/assets.json"}}}`))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(outside, "assets.json")); !os.IsNotExist(err) {
		t.Fatalf("outside destination exists: %v", err)
	}
}
