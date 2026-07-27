package cloudasset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"minisky/pkg/pagination"
	"minisky/pkg/registry"
)

func init() {
	registry.Register("cloudasset.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPIWithInventory(registryInventory{context: ctx}, "")
	})
}

// Asset is a registry-derived metadata record. Cloud Asset does not invent
// resources when no backing shim exports inventory.
type Asset struct {
	Name        string         `json:"name"`
	AssetType   string         `json:"assetType"`
	Project     string         `json:"project,omitempty"`
	DisplayName string         `json:"displayName,omitempty"`
	Location    string         `json:"location,omitempty"`
	CreateTime  string         `json:"createTime,omitempty"`
	UpdateTime  string         `json:"updateTime,omitempty"`
	Resource    map[string]any `json:"resource,omitempty"`
}

// Inventory supplies snapshots from local registered shims.
type Inventory interface {
	Assets(parent string) []Asset
}

type assetInventory []Asset

func (inventory assetInventory) Assets(parent string) []Asset {
	result := make([]Asset, 0, len(inventory))
	for _, asset := range inventory {
		if asset.Project == "" || asset.Project == parent {
			result = append(result, asset)
		}
	}
	return result
}

type registryInventory struct {
	context *registry.Context
}

var inventoryDomains = []string{
	"bigquery.googleapis.com",
	"cloudresourcemanager.googleapis.com",
	"compute.googleapis.com",
	"pubsub.googleapis.com",
	"storage.googleapis.com",
}

func (inventory registryInventory) Assets(parent string) []Asset {
	if inventory.context == nil {
		return nil
	}
	seen := make(map[string]bool)
	var assets []Asset
	for _, domain := range inventoryDomains {
		discoverer, ok := inventory.context.GetShim(domain).(registry.ProjectDiscoverer)
		if !ok {
			continue
		}
		for _, project := range discoverer.ListProjects() {
			projectName := "projects/" + project
			if seen[projectName] || (parent != "" && parent != projectName) {
				continue
			}
			seen[projectName] = true
			now := time.Now().UTC().Format(time.RFC3339Nano)
			assets = append(assets, Asset{
				Name: "//cloudresourcemanager.googleapis.com/" + projectName, AssetType: "cloudresourcemanager.googleapis.com/Project",
				Project: projectName, DisplayName: project, Location: "global", CreateTime: now, UpdateTime: now,
			})
		}
	}
	return assets
}

func standaloneInventory() Inventory {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return assetInventory{
		{
			Name:      "//compute.googleapis.com/projects/my-project/zones/us-central1-a/instances/instance-1",
			AssetType: "compute.googleapis.com/Instance", Project: "projects/my-project",
			DisplayName: "instance-1", Location: "us-central1-a", CreateTime: now, UpdateTime: now,
		},
		{
			Name:      "//storage.googleapis.com/projects/_/buckets/my-bucket",
			AssetType: "storage.googleapis.com/Bucket", Project: "projects/my-project",
			DisplayName: "my-bucket", Location: "us", CreateTime: now, UpdateTime: now,
		},
		{
			Name:      "//pubsub.googleapis.com/projects/my-project/topics/my-topic",
			AssetType: "pubsub.googleapis.com/Topic", Project: "projects/my-project",
			DisplayName: "my-topic", Location: "global", CreateTime: now, UpdateTime: now,
		},
	}
}

// API is a read-only Cloud Asset Inventory v1 shim.
type API struct {
	inventory         Inventory
	exportRoot        string
	beforeExportWrite func()
}

func NewAPI() *API {
	return NewAPIWithInventory(standaloneInventory(), "")
}

// NewAPIWithInventory injects registry-derived inventory and an optional local
// export root. Empty exportRoot keeps exportAssets explicitly unsupported.
func NewAPIWithInventory(inventory Inventory, exportRoot string) *API {
	return &API{inventory: inventory, exportRoot: exportRoot}
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-MiniSky-Simulated", "true")
	switch {
	case strings.HasSuffix(r.URL.Path, ":exportAssets") && r.Method == http.MethodPost:
		api.exportAssets(w, r)
	case strings.HasSuffix(r.URL.Path, ":searchAllResources") && r.Method == http.MethodGet:
		api.searchAllResources(w, r)
	case strings.HasSuffix(r.URL.Path, ":searchAllResources"):
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "POST searchAllResources is not supported")
	case strings.HasSuffix(r.URL.Path, "/assets") && r.Method == http.MethodGet:
		api.listAssets(w, r)
	default:
		gcpError(w, http.StatusNotFound, "NOT_FOUND", "Route not found")
	}
}

func (api *API) listAssets(w http.ResponseWriter, r *http.Request) {
	parent := parentFromPath(r.URL.Path, "/assets")
	assets := api.assets(parent)
	page, next, err := paginateAssets(assets, r.URL.Query().Get("pageSize"), r.URL.Query().Get("pageToken"),
		pagination.Scope{Service: "cloudasset.assets", Parent: parent})
	if err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"assets": page, "nextPageToken": next})
}

func (api *API) searchAllResources(w http.ResponseWriter, r *http.Request) {
	parent := parentFromPath(r.URL.Path, ":searchAllResources")
	query := r.URL.Query().Get("query")
	assets := api.assets(parent)
	results := make([]Asset, 0, len(assets))
	for _, asset := range assets {
		if query == "" || strings.Contains(asset.Name, query) ||
			strings.Contains(asset.AssetType, query) {
			results = append(results, asset)
		}
	}
	page, next, err := paginateAssets(results, r.URL.Query().Get("pageSize"), r.URL.Query().Get("pageToken"),
		pagination.Scope{Service: "cloudasset.searchAllResources", Parent: parent, Filter: query})
	if err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"results": page, "nextPageToken": next})
}

func (api *API) exportAssets(w http.ResponseWriter, r *http.Request) {
	if api.exportRoot == "" {
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"exportAssets requires an explicitly configured local export root")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var request struct {
		OutputConfig struct {
			LocalDestination struct {
				Path string `json:"path"`
			} `json:"localDestination"`
		} `json:"outputConfig"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid export request")
		return
	}
	destination, err := safeDestination(api.exportRoot, request.OutputConfig.LocalDestination.Path)
	if err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	parent := parentFromPath(r.URL.Path, ":exportAssets")
	var payload bytes.Buffer
	if err := json.NewEncoder(&payload).Encode(map[string]any{"assets": api.assets(parent)}); err != nil {
		gcpError(w, http.StatusInternalServerError, "INTERNAL", "encode local asset export failed")
		return
	}
	if api.beforeExportWrite != nil {
		api.beforeExportWrite()
	}
	if err := secureWriteAssetExport(api.exportRoot, destination, payload.Bytes()); err != nil {
		gcpError(w, http.StatusInternalServerError, "INTERNAL", "write local asset export failed")
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"readTime":     "local",
		"outputConfig": request.OutputConfig,
		"outputResult": map[string]string{"localPath": request.OutputConfig.LocalDestination.Path},
	})
}

func (api *API) assets(parent string) []Asset {
	if api.inventory == nil {
		return []Asset{}
	}
	assets := append([]Asset(nil), api.inventory.Assets(parent)...)
	sort.Slice(assets, func(i, j int) bool {
		if assets[i].AssetType == assets[j].AssetType {
			return assets[i].Name < assets[j].Name
		}
		return assets[i].AssetType < assets[j].AssetType
	})
	return assets
}

func parentFromPath(path, suffix string) string {
	trimmed := strings.TrimPrefix(path, "/v1/")
	return strings.TrimSuffix(trimmed, suffix)
}

func paginateAssets(assets []Asset, rawSize, rawToken string, scope pagination.Scope) ([]Asset, string, error) {
	pageSize := 100
	if rawSize != "" {
		value, err := strconv.Atoi(rawSize)
		if err != nil || value <= 0 {
			return nil, "", fmt.Errorf("pageSize must be a positive integer")
		}
		pageSize = value
	}
	if pageSize > 500 {
		pageSize = 500
	}
	return pagination.Page(assets, pageSize, rawToken, scope, func(asset Asset) string {
		return asset.AssetType + "\x00" + asset.Name
	})
}

func safeDestination(_ string, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return "", fmt.Errorf("local destination must be a relative path")
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("local destination escapes the export root")
	}
	return clean, nil
}

func gcpError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": message, "status": status, "details": []any{}},
	})
}
