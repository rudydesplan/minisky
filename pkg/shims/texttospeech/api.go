// Package texttospeech implements the bounded Cloud Text-to-Speech v1 surface.
package texttospeech

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"minisky/pkg/registry"
)

func init() {
	registry.Register("texttospeech.googleapis.com", func(_ *registry.Context) http.Handler { return NewAPI() })
}

type API struct{}

func NewAPI() *API { return &API{} }

type synthesizeRequest struct {
	Input *struct {
		Text   string          `json:"text,omitempty"`
		SSML   string          `json:"ssml,omitempty"`
		Prompt json.RawMessage `json:"multiSpeakerMarkup,omitempty"`
	} `json:"input"`
	Voice *struct {
		LanguageCode string `json:"languageCode"`
		Name         string `json:"name,omitempty"`
	} `json:"voice"`
	AudioConfig *struct {
		AudioEncoding string  `json:"audioEncoding"`
		SpeakingRate  float64 `json:"speakingRate,omitempty"`
		Pitch         float64 `json:"pitch,omitempty"`
	} `json:"audioConfig"`
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-MiniSky-Simulated", "true")
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/text:synthesize"):
		api.synthesize(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/voices"):
		writeError(w, 501, "UNIMPLEMENTED", "voice catalog parity is not implemented")
	default:
		writeError(w, 404, "NOT_FOUND", "Text-to-Speech resource not found")
	}
}

func (api *API) synthesize(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var request synthesizeRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid request body")
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(w, 400, "INVALID_ARGUMENT", "invalid request body")
		return
	}
	if request.Input == nil {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'input' is required")
		return
	}
	set := 0
	if request.Input.Text != "" {
		set++
	}
	if request.Input.SSML != "" {
		set++
	}
	if len(request.Input.Prompt) != 0 {
		set++
	}
	if set != 1 {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'input' must set exactly one input modality")
		return
	}
	if request.Input.SSML != "" || len(request.Input.Prompt) != 0 {
		writeError(w, 501, "UNIMPLEMENTED", "only plain text input is recognized by the local boundary")
		return
	}
	if len(request.Input.Text) > 5000 {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'input.text' exceeds the 5000-byte local limit")
		return
	}
	if request.Voice == nil || request.Voice.LanguageCode == "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'voice.languageCode' is required")
		return
	}
	if request.Voice.Name != "" {
		writeError(w, 501, "UNIMPLEMENTED", "field 'voice.name' is not implemented")
		return
	}
	if request.AudioConfig == nil || request.AudioConfig.AudioEncoding == "" {
		writeError(w, 400, "INVALID_ARGUMENT", "field 'audioConfig.audioEncoding' is required")
		return
	}
	if request.AudioConfig.SpeakingRate != 0 || request.AudioConfig.Pitch != 0 {
		writeError(w, 501, "UNIMPLEMENTED",
			"fields 'audioConfig.speakingRate' and 'audioConfig.pitch' are not implemented")
		return
	}
	writeError(w, 501, "UNIMPLEMENTED", "speech synthesis is not implemented; no audio content was generated")
}

func writeError(w http.ResponseWriter, code int, status, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
		"code": code, "message": message, "status": status, "details": []any{},
	}})
}
