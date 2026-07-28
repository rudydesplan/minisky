package dlp

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"minisky/pkg/config"
	"minisky/pkg/pagination"
	"minisky/pkg/registry"
	"minisky/pkg/state"
)

func init() {
	registry.Register("dlp.googleapis.com", func(ctx *registry.Context) http.Handler {
		return NewAPI()
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Resource types (DLP v2 contract)
// ─────────────────────────────────────────────────────────────────────────────

// InspectTemplate represents a google.privacy.dlp.v2.InspectTemplate resource.
type InspectTemplate struct {
	Name          string         `json:"name"`
	DisplayName   string         `json:"displayName,omitempty"`
	Description   string         `json:"description,omitempty"`
	CreateTime    string         `json:"createTime,omitempty"`
	UpdateTime    string         `json:"updateTime,omitempty"`
	InspectConfig map[string]any `json:"inspectConfig,omitempty"`
}

// ─────────────────────────────────────────────────────────────────────────────
// API
// ─────────────────────────────────────────────────────────────────────────────

// API implements the DLP v2 shim with template CRUD and stateless inspect/deidentify.
type API struct {
	mu         sync.RWMutex
	persistMu  sync.Mutex
	templates  map[string]*InspectTemplate // key: full resource name
	stateStore dlpStateStore
	counter    int
}

// NewAPI creates a DLP API with persistence from config defaults.
func NewAPI() *API {
	store, err := state.New(config.GetStateDir(), config.GetProfile())
	api := NewAPIWithStore(state.NewGuardedEntryStore(store, err))
	if err != nil {
		log.Printf("[Shim: DLP] persistence degraded: %v", err)
		return api
	}
	return api
}

func newAPI(store dlpStateStore) *API {
	return &API{
		templates:  make(map[string]*InspectTemplate),
		stateStore: store,
	}
}

// ServeHTTP routes DLP v2 requests.
func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Shim: DLP] %s %s", r.Method, r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-MiniSky-Simulated", "true")

	path := strings.TrimPrefix(r.URL.Path, "/v2")

	switch {
	case strings.HasSuffix(path, "/content:inspect") && r.Method == http.MethodPost:
		api.inspectContent(w, r)
	case strings.HasSuffix(path, "/content:deidentify") && r.Method == http.MethodPost:
		api.deidentifyContent(w, r)
	case strings.Contains(path, "/inspectTemplates"):
		api.routeTemplates(w, r, path)
	default:
		gcpError(w, http.StatusNotFound, "NOT_FOUND", "Route not found")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Template CRUD
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) routeTemplates(w http.ResponseWriter, r *http.Request, path string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	// Expected: projects/{p}/inspectTemplates[/{id}]
	templateIdx := -1
	for i, p := range parts {
		if p == "inspectTemplates" {
			templateIdx = i
			break
		}
	}
	if templateIdx < 0 {
		gcpError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}

	hasID := templateIdx+1 < len(parts)

	switch r.Method {
	case http.MethodPost:
		if !hasID {
			api.createTemplate(w, r, parts, templateIdx)
		} else {
			gcpError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	case http.MethodGet:
		if hasID {
			api.getTemplate(w, parts, templateIdx)
		} else {
			api.listTemplates(w, r, parts, templateIdx)
		}
	case http.MethodPatch:
		if hasID {
			api.patchTemplate(w, r, parts, templateIdx)
		} else {
			gcpError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	case http.MethodDelete:
		if hasID {
			api.deleteTemplate(w, parts, templateIdx)
		} else {
			gcpError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		}
	default:
		gcpError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
	}
}

func (api *API) createTemplate(w http.ResponseWriter, r *http.Request, parts []string, templateIdx int) {
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	var body struct {
		InspectTemplate struct {
			DisplayName   string         `json:"displayName"`
			Description   string         `json:"description"`
			InspectConfig map[string]any `json:"inspectConfig"`
		} `json:"inspectTemplate"`
		TemplateId string `json:"templateId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	api.mu.Lock()

	api.counter++
	id := body.TemplateId
	if id == "" {
		id = fmt.Sprintf("tmpl-%d", api.counter)
	}

	// Build parent from path parts before "inspectTemplates"
	parent := strings.Join(parts[:templateIdx], "/")
	name := fmt.Sprintf("%s/inspectTemplates/%s", parent, id)

	if _, exists := api.templates[name]; exists {
		api.mu.Unlock()
		gcpError(w, http.StatusConflict, "ALREADY_EXISTS", "template already exists: "+name)
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	tmpl := &InspectTemplate{
		Name:          name,
		DisplayName:   body.InspectTemplate.DisplayName,
		Description:   body.InspectTemplate.Description,
		CreateTime:    now,
		UpdateTime:    now,
		InspectConfig: body.InspectTemplate.InspectConfig,
	}
	api.templates[name] = tmpl
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Roll back
		api.mu.Lock()
		delete(api.templates, name)
		api.mu.Unlock()
		gcpError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(tmpl)
}

func (api *API) getTemplate(w http.ResponseWriter, parts []string, templateIdx int) {
	name := strings.Join(parts[:templateIdx+2], "/")
	api.mu.RLock()
	tmpl, ok := api.templates[name]
	api.mu.RUnlock()
	if !ok {
		gcpError(w, http.StatusNotFound, "NOT_FOUND", "template not found: "+name)
		return
	}
	json.NewEncoder(w).Encode(tmpl)
}

func (api *API) listTemplates(w http.ResponseWriter, r *http.Request, parts []string, templateIdx int) {
	api.mu.RLock()
	defer api.mu.RUnlock()

	parent := strings.Join(parts[:templateIdx], "/")

	pageSize := 100
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if n, err := strconv.Atoi(ps); err == nil && n > 0 {
			pageSize = n
		}
	}
	if pageSize > 500 {
		pageSize = 500
	}
	pageToken := r.URL.Query().Get("pageToken")

	var items []*InspectTemplate
	for _, tmpl := range api.templates {
		if strings.HasPrefix(tmpl.Name, parent+"/inspectTemplates/") {
			items = append(items, tmpl)
		}
	}

	page, nextToken, err := pagination.Page(items, pageSize, pageToken, pagination.Scope{
		Service: "dlp.inspectTemplates",
		Parent:  parent,
		Filter:  r.URL.Query().Get("filter"),
		OrderBy: r.URL.Query().Get("orderBy"),
	}, func(template *InspectTemplate) string { return template.Name })
	if err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid pageToken")
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"inspectTemplates": page,
		"nextPageToken":    nextToken,
	})
}

func (api *API) patchTemplate(w http.ResponseWriter, r *http.Request, parts []string, templateIdx int) {
	name := strings.Join(parts[:templateIdx+2], "/")

	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	var body struct {
		InspectTemplate struct {
			DisplayName   string         `json:"displayName"`
			Description   string         `json:"description"`
			InspectConfig map[string]any `json:"inspectConfig"`
		} `json:"inspectTemplate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}

	api.mu.Lock()

	tmpl, ok := api.templates[name]
	if !ok {
		api.mu.Unlock()
		gcpError(w, http.StatusNotFound, "NOT_FOUND", "template not found: "+name)
		return
	}

	// Apply updates (simple merge — non-empty fields overwrite)
	if body.InspectTemplate.DisplayName != "" {
		tmpl.DisplayName = body.InspectTemplate.DisplayName
	}
	if body.InspectTemplate.Description != "" {
		tmpl.Description = body.InspectTemplate.Description
	}
	if body.InspectTemplate.InspectConfig != nil {
		tmpl.InspectConfig = body.InspectTemplate.InspectConfig
	}
	tmpl.UpdateTime = time.Now().UTC().Format(time.RFC3339Nano)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		gcpError(w, 503, "UNAVAILABLE", "Service temporarily unavailable: state persistence failed")
		return
	}

	json.NewEncoder(w).Encode(tmpl)
}

func (api *API) deleteTemplate(w http.ResponseWriter, parts []string, templateIdx int) {
	name := strings.Join(parts[:templateIdx+2], "/")

	api.mu.Lock()

	tmpl, ok := api.templates[name]
	if !ok {
		api.mu.Unlock()
		gcpError(w, http.StatusNotFound, "NOT_FOUND", "template not found: "+name)
		return
	}
	delete(api.templates, name)
	api.mu.Unlock()

	if err := api.persistState(); err != nil {
		// Re-add the resource since persist failed
		api.mu.Lock()
		api.templates[name] = tmpl
		api.mu.Unlock()
		gcpError(w, 503, "UNAVAILABLE", "State persistence failed")
		return
	}

	// DLP delete returns empty response
	json.NewEncoder(w).Encode(map[string]any{})
}

// ─────────────────────────────────────────────────────────────────────────────
// Stateless operations
// ─────────────────────────────────────────────────────────────────────────────

func (api *API) inspectContent(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	var body struct {
		Item struct {
			Value string `json:"value"`
		} `json:"item"`
		InspectConfig inspectConfig `json:"inspectConfig"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	if body.Item.Value == "" {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "item.value is required")
		return
	}
	if len(body.InspectConfig.InfoTypes) == 0 {
		body.InspectConfig = defaultInspectConfig()
	}
	if unsupported := unsupportedInfoType(body.InspectConfig); unsupported != "" {
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"info type is not supported by the bounded local detector: "+unsupported)
		return
	}

	findings := inspectValue(body.Item.Value, body.InspectConfig)

	json.NewEncoder(w).Encode(map[string]any{
		"result": map[string]any{
			"findings": findings,
		},
	})
}

func (api *API) deidentifyContent(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	var body struct {
		Item struct {
			Value string `json:"value"`
		} `json:"item"`
		InspectConfig    inspectConfig `json:"inspectConfig"`
		DeidentifyConfig struct {
			InfoTypeTransformations struct {
				Transformations []struct {
					PrimitiveTransformation struct {
						ReplaceConfig *struct {
							NewValue struct {
								StringValue string `json:"stringValue"`
							} `json:"newValue"`
						} `json:"replaceConfig"`
					} `json:"primitiveTransformation"`
				} `json:"transformations"`
			} `json:"infoTypeTransformations"`
		} `json:"deidentifyConfig"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid JSON: "+err.Error())
		return
	}
	if body.Item.Value == "" {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "item.value is required")
		return
	}
	if len(body.InspectConfig.InfoTypes) == 0 {
		body.InspectConfig = defaultInspectConfig()
	}
	if unsupported := unsupportedInfoType(body.InspectConfig); unsupported != "" {
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"info type is not supported by the bounded local detector: "+unsupported)
		return
	}

	replacement := "[REDACTED]"
	transformations := body.DeidentifyConfig.InfoTypeTransformations.Transformations
	if len(transformations) > 0 {
		if len(transformations) != 1 || transformations[0].PrimitiveTransformation.ReplaceConfig == nil {
			gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
				"only one replaceConfig transformation is supported")
			return
		}
		replacement = transformations[0].PrimitiveTransformation.ReplaceConfig.NewValue.StringValue
	}
	value, transformedBytes := transformValue(body.Item.Value, body.InspectConfig, replacement)

	json.NewEncoder(w).Encode(map[string]any{
		"item": map[string]any{
			"value": value,
		},
		"overview": map[string]any{
			"transformedBytes":        strconv.Itoa(transformedBytes),
			"transformationSummaries": []any{},
		},
	})
}

type inspectConfig struct {
	InfoTypes []struct {
		Name string `json:"name"`
	} `json:"infoTypes"`
	IncludeQuote bool `json:"includeQuote"`
}

func defaultInspectConfig() inspectConfig {
	var config inspectConfig
	config.InfoTypes = append(config.InfoTypes, struct {
		Name string `json:"name"`
	}{Name: "EMAIL_ADDRESS"})
	config.IncludeQuote = true
	return config
}

func unsupportedInfoType(config inspectConfig) string {
	for _, infoType := range config.InfoTypes {
		if _, supported := boundedDetectors[infoType.Name]; !supported {
			return infoType.Name
		}
	}
	return ""
}

type detectedValue struct {
	name       string
	start, end int
	quote      string
}

var boundedDetectors = map[string]*regexp.Regexp{
	"EMAIL_ADDRESS":      regexp.MustCompile(`[A-Za-z0-9.!#$%&'*+/=?^_` + "`" + `{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+`),
	"CREDIT_CARD_NUMBER": regexp.MustCompile(`(?:[0-9][ -]?){13,19}`),
	"IP_ADDRESS":         regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`),
}

func detectValues(value string, config inspectConfig) []detectedValue {
	names := make([]string, 0, len(config.InfoTypes))
	for _, infoType := range config.InfoTypes {
		if _, supported := boundedDetectors[infoType.Name]; supported {
			names = append(names, infoType.Name)
		}
	}
	sort.Strings(names)
	var detected []detectedValue
	for _, name := range names {
		for _, location := range boundedDetectors[name].FindAllStringIndex(value, -1) {
			quote := value[location[0]:location[1]]
			if name == "CREDIT_CARD_NUMBER" && !validLuhn(quote) {
				continue
			}
			detected = append(detected, detectedValue{name: name, start: location[0], end: location[1], quote: quote})
		}
	}
	sort.Slice(detected, func(i, j int) bool {
		if detected[i].start == detected[j].start {
			return detected[i].name < detected[j].name
		}
		return detected[i].start < detected[j].start
	})
	return detected
}

func inspectValue(value string, config inspectConfig) []map[string]any {
	detected := detectValues(value, config)
	findings := make([]map[string]any, 0, len(detected))
	for _, item := range detected {
		finding := map[string]any{
			"infoType":   map[string]string{"name": item.name},
			"likelihood": "LIKELY",
			"location": map[string]any{
				"byteRange": map[string]string{"start": strconv.Itoa(item.start), "end": strconv.Itoa(item.end)},
			},
		}
		if config.IncludeQuote {
			finding["quote"] = item.quote
		}
		findings = append(findings, finding)
	}
	return findings
}

func transformValue(value string, config inspectConfig, replacement string) (string, int) {
	detected := detectValues(value, config)
	if len(detected) == 0 {
		return value, 0
	}
	var output strings.Builder
	cursor, transformed := 0, 0
	for _, item := range detected {
		if item.start < cursor {
			continue
		}
		output.WriteString(value[cursor:item.start])
		output.WriteString(replacement)
		cursor = item.end
		transformed += item.end - item.start
	}
	output.WriteString(value[cursor:])
	return output.String(), transformed
}

func validLuhn(value string) bool {
	digits := strings.NewReplacer(" ", "", "-", "").Replace(value)
	if len(digits) < 13 || len(digits) > 19 {
		return false
	}
	sum, parity := 0, len(digits)%2
	for i := range digits {
		digit := int(digits[i] - '0')
		if digit < 0 || digit > 9 {
			return false
		}
		if i%2 == parity {
			digit *= 2
			if digit > 9 {
				digit -= 9
			}
		}
		sum += digit
	}
	return sum%10 == 0
}

func gcpError(w http.ResponseWriter, code int, status, msg string) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": msg,
			"status":  status,
			"details": []any{},
		},
	})
}
