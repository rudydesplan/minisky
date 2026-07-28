package translate

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTranslateText(t *testing.T) {
	api := NewAPI()

	body := `{"contents":["Hello"],"targetLanguageCode":"en","sourceLanguageCode":"en","mimeType":"text/plain"}`
	req := httptest.NewRequest(http.MethodPost, "/v3/projects/test-project/locations/global:translateText", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", w.Code, w.Body.String())
	}
	if w.Header().Get("X-MiniSky-Simulated") != "true" {
		t.Error("missing X-MiniSky-Simulated header")
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	translations := resp["translations"].([]any)
	if len(translations) != 1 {
		t.Fatalf("expected 1 translation, got %d", len(translations))
	}
	first := translations[0].(map[string]any)
	if first["translatedText"] != "Hello" {
		t.Errorf("translatedText = %v, want Hello", first["translatedText"])
	}
	if first["detectedLanguageCode"] != nil {
		t.Errorf("explicit source language must not claim detection: %v", first["detectedLanguageCode"])
	}
	if first["model"] != nil {
		t.Errorf("identity translation must not claim a model: %v", first["model"])
	}
	if _, ok := resp["glossaryTranslations"]; !ok {
		t.Error("missing glossaryTranslations field")
	}
}

func TestTranslateTextMissingContents(t *testing.T) {
	api := NewAPI()

	body := `{"targetLanguageCode":"fr"}`
	req := httptest.NewRequest(http.MethodPost, "/v3/projects/test-project/locations/global:translateText", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
}

func TestTranslateTextMissingTarget(t *testing.T) {
	api := NewAPI()

	body := `{"contents":["Hello"]}`
	req := httptest.NewRequest(http.MethodPost, "/v3/projects/test-project/locations/global:translateText", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
}

func TestSupportedLanguages(t *testing.T) {
	api := NewAPI()

	req := httptest.NewRequest(http.MethodGet, "/v3/projects/test-project/locations/global/supportedLanguages", nil)
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body = %s", w.Code, w.Body.String())
	}
	if w.Header().Get("X-MiniSky-Simulated") != "true" {
		t.Error("missing X-MiniSky-Simulated header")
	}

	if strings.Contains(w.Body.String(), `"languages"`) {
		t.Fatal("unsupported language catalog returned fabricated capabilities")
	}
}

func TestTranslateMultipleContents(t *testing.T) {
	api := NewAPI()

	body := `{"contents":["Hello","World","Goodbye"],"targetLanguageCode":"es"}`
	req := httptest.NewRequest(http.MethodPost, "/v3/projects/test-project/locations/global:translateText", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body = %s", w.Code, w.Body.String())
	}
}
