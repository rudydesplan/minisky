package translate

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"minisky/pkg/registry"
)

func init() {
	registry.Register("translate.googleapis.com", func(_ *registry.Context) http.Handler {
		return NewAPI()
	})
}

// API implements a stateless mock of Cloud Translation API v3.
type API struct{}

func NewAPI() *API { return &API{} }

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-MiniSky-Simulated", "true")

	switch {
	case strings.HasSuffix(r.URL.Path, ":translateText") && r.Method == http.MethodPost:
		api.translateText(w, r)
	case strings.HasSuffix(r.URL.Path, "/supportedLanguages") && r.Method == http.MethodGet:
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"supported language catalog parity is not implemented")
	case r.Method == http.MethodPost && (strings.HasSuffix(r.URL.Path, ":detectLanguage") ||
		strings.HasSuffix(r.URL.Path, ":batchTranslateText") ||
		strings.HasSuffix(r.URL.Path, ":romanizeText") ||
		strings.HasSuffix(r.URL.Path, ":transliterateText")):
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"language detection, batch translation, romanization, and transliteration are not implemented")
	default:
		gcpError(w, http.StatusNotFound, "NOT_FOUND", "Translate resource not found")
	}
}

type translateRequest struct {
	Contents              []string `json:"contents"`
	TargetLanguageCode    string   `json:"targetLanguageCode"`
	SourceLanguageCode    string   `json:"sourceLanguageCode"`
	MimeType              string   `json:"mimeType"`
	Model                 string   `json:"model,omitempty"`
	GlossaryConfig        any      `json:"glossaryConfig,omitempty"`
	TransliterationConfig any      `json:"transliterationConfig,omitempty"`
}

type translationEntry struct {
	TranslatedText string `json:"translatedText"`
	Model          string `json:"model,omitempty"`
}

func (api *API) translateText(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req translateRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid request body")
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid request body")
		return
	}
	if len(req.Contents) == 0 {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "contents field is required")
		return
	}
	if req.TargetLanguageCode == "" {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'targetLanguageCode' is required")
		return
	}
	if len(req.Contents) > 128 {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'contents' must contain at most 128 items")
		return
	}
	total := 0
	for _, content := range req.Contents {
		total += len(content)
	}
	if total > 30_000 {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'contents' exceeds the 30000-byte local limit")
		return
	}
	if req.MimeType != "" && req.MimeType != "text/plain" {
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "field 'mimeType' supports only text/plain locally")
		return
	}
	if req.Model != "" || req.GlossaryConfig != nil || req.TransliterationConfig != nil {
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
			"fields 'model', 'glossaryConfig', and 'transliterationConfig' are not implemented")
		return
	}
	if req.SourceLanguageCode == "" {
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "automatic source language detection is not implemented")
		return
	}
	if req.SourceLanguageCode != req.TargetLanguageCode {
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "semantic translation between different languages is not implemented")
		return
	}

	translations := make([]translationEntry, 0, len(req.Contents))
	for _, content := range req.Contents {
		translations = append(translations, translationEntry{
			TranslatedText: content,
		})
	}

	json.NewEncoder(w).Encode(map[string]any{
		"translations":         translations,
		"glossaryTranslations": []any{},
	})
}

func gcpError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
			"status":  status,
			"details": []any{},
		},
	})
}
