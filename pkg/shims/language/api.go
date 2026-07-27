// Package language implements a bounded Cloud Natural Language v1 surface.
package language

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"minisky/pkg/registry"
)

func init() {
	registry.Register("language.googleapis.com", func(_ *registry.Context) http.Handler { return NewAPI() })
}

type API struct{}

func NewAPI() *API { return &API{} }

type analyzeRequest struct {
	Document *struct {
		Type     string `json:"type"`
		Content  string `json:"content,omitempty"`
		GCSURI   string `json:"gcsContentUri,omitempty"`
		Language string `json:"language,omitempty"`
	} `json:"document"`
	EncodingType string `json:"encodingType,omitempty"`
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-MiniSky-Simulated", "true")
	if r.Method != http.MethodPost {
		writeError(w, 404, "NOT_FOUND", "Natural Language resource not found")
		return
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/documents:analyzeSentiment"),
		strings.HasSuffix(r.URL.Path, "/documents:analyzeEntities"),
		strings.HasSuffix(r.URL.Path, "/documents:analyzeSyntax"),
		strings.HasSuffix(r.URL.Path, "/documents:annotateText"),
		strings.HasSuffix(r.URL.Path, "/documents:classifyText"),
		strings.HasSuffix(r.URL.Path, "/documents:moderateText"):
		api.analyze(w, r)
	default:
		writeError(w, 404, "NOT_FOUND", "Natural Language resource not found")
	}
}

func (api *API) analyze(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var request analyzeRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid request body")
		return
	}
	if request.Document == nil {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'document' is required")
		return
	}
	if request.Document.Type != "PLAIN_TEXT" {
		if request.Document.Type == "HTML" {
			writeError(w, 501, "UNIMPLEMENTED", "field 'document.type' supports only PLAIN_TEXT locally")
		} else {
			writeError(w, 400, "INVALID_ARGUMENT", "field 'document.type' must be PLAIN_TEXT or HTML")
		}
		return
	}
	if (request.Document.Content == "") == (request.Document.GCSURI == "") {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'document' must set exactly one of 'content' or 'gcsContentUri'")
		return
	}
	if request.Document.GCSURI != "" {
		uri, err := url.Parse(request.Document.GCSURI)
		if err != nil || uri.Scheme != "gs" || uri.Host == "" || uri.User != nil {
			writeError(w, 400, "INVALID_ARGUMENT", "field 'document.gcsContentUri' must use a credential-free gs:// URI")
			return
		}
		writeError(w, 501, "UNIMPLEMENTED", "GCS document input is not implemented")
		return
	}
	if len(request.Document.Content) > 100_000 {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'document.content' exceeds the 100000-byte local limit")
		return
	}
	writeError(w, 501, "UNIMPLEMENTED", "semantic language analysis is not implemented; no scores or labels were generated")
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "message": message, "status": status, "details": []any{},
	}})
}
