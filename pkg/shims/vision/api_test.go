package vision

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAnnotateLabels(t *testing.T) {
	api := NewAPI()
	body := `{"requests":[{"image":{"content":"aGVsbG8="},"features":[{"type":"LABEL_DETECTION","maxResults":10}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/images:annotate", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body = %s", w.Code, w.Body.String())
	}
	if w.Header().Get("X-MiniSky-Simulated") != "true" {
		t.Error("missing X-MiniSky-Simulated header")
	}
	if bytes.Contains(w.Body.Bytes(), []byte(`"score"`)) {
		t.Fatal("unsupported semantic response fabricated a score")
	}
}

func TestAnnotateText(t *testing.T) {
	api := NewAPI()

	body := `{"requests":[{"image":{"source":{"imageUri":"https://example.com/img.png"}},"features":[{"type":"TEXT_DETECTION"}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/images:annotate", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
}

func TestAnnotateMultipleFeatures(t *testing.T) {
	api := NewAPI()

	body := `{"requests":[{"image":{"content":"aGVsbG8="},"features":[{"type":"LABEL_DETECTION"},{"type":"FACE_DETECTION"},{"type":"SAFE_SEARCH_DETECTION"},{"type":"IMAGE_PROPERTIES"}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/images:annotate", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body = %s", w.Code, w.Body.String())
	}
}

func TestAnnotateMissingRequests(t *testing.T) {
	api := NewAPI()

	body := `{}`
	req := httptest.NewRequest(http.MethodPost, "/v1/images:annotate", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
}

func TestAnnotateMissingFeatures(t *testing.T) {
	api := NewAPI()

	body := `{"requests":[{"image":{"content":"base64"},"features":[]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/images:annotate", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
}

func TestAnnotateEmptyRequests(t *testing.T) {
	api := NewAPI()

	body := `{"requests":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/images:annotate", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	api.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
	}
}
