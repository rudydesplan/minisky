// Package speech implements the bounded Cloud Speech-to-Text v1 HTTP surface.
package speech

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"minisky/pkg/registry"
)

const maxBody = 1 << 20

func init() {
	registry.Register("speech.googleapis.com", func(_ *registry.Context) http.Handler { return NewAPI() })
}

type API struct{}

func NewAPI() *API { return &API{} }

type recognizeRequest struct {
	Config *struct {
		Encoding        string `json:"encoding,omitempty"`
		SampleRateHertz int    `json:"sampleRateHertz,omitempty"`
		LanguageCode    string `json:"languageCode"`
		Model           string `json:"model,omitempty"`
	} `json:"config"`
	Audio *struct {
		Content string `json:"content,omitempty"`
		URI     string `json:"uri,omitempty"`
	} `json:"audio"`
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-MiniSky-Simulated", "true")
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/speech:recognize"):
		api.recognize(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/speech:longrunningrecognize"):
		writeError(w, 501, "UNIMPLEMENTED", "long-running speech recognition is not implemented")
	default:
		writeError(w, 404, "NOT_FOUND", "Speech-to-Text resource not found")
	}
}

func (api *API) recognize(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	var request recognizeRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid request body")
		return
	}
	if request.Config == nil || request.Config.LanguageCode == "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'config.languageCode' is required")
		return
	}
	if request.Audio == nil {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'audio' is required")
		return
	}
	if (request.Audio.Content == "") == (request.Audio.URI == "") {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'audio' must set exactly one of 'content' or 'uri'")
		return
	}
	if request.Audio.URI != "" {
		parsed, err := url.Parse(request.Audio.URI)
		if err != nil || parsed.Scheme != "gs" || parsed.Host == "" || parsed.User != nil {
			writeError(w, 400, "INVALID_ARGUMENT", "field 'audio.uri' must use a gs:// URI; external URLs and credentials are rejected")
			return
		}
		writeError(w, 501, "UNIMPLEMENTED", "GCS audio input is not implemented")
		return
	}
	audio, err := base64.StdEncoding.DecodeString(request.Audio.Content)
	if err != nil {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'audio.content' must be valid base64")
		return
	}
	if len(audio) > 512<<10 {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'audio.content' exceeds the 512 KiB local limit")
		return
	}
	writeError(w, 501, "UNIMPLEMENTED", "speech recognition is not implemented; no transcript or confidence was generated")
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "message": message, "status": status, "details": []any{},
	}})
}
