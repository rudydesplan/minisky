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
