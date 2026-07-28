package vision

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"minisky/pkg/registry"
)

func init() {
	registry.Register("vision.googleapis.com", func(_ *registry.Context) http.Handler {
		return NewAPI()
	})
}

// API implements a stateless mock of Cloud Vision API v1.
type API struct{}

func NewAPI() *API { return &API{} }

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-MiniSky-Simulated", "true")

	if strings.HasSuffix(r.URL.Path, "/images:annotate") && r.Method == http.MethodPost {
		api.annotate(w, r)
		return
	}
	if r.Method == http.MethodPost && (strings.HasSuffix(r.URL.Path, "/images:asyncBatchAnnotate") ||
		strings.HasSuffix(r.URL.Path, "/files:annotate") ||
		strings.HasSuffix(r.URL.Path, "/files:asyncBatchAnnotate")) {
		gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED", "batch and file annotation are not implemented")
		return
	}
	gcpError(w, http.StatusNotFound, "NOT_FOUND", "Vision resource not found")
}

// Request/response types matching Cloud Vision API v1.

type annotateRequest struct {
	Requests []annotateImageRequest `json:"requests"`
}

type annotateImageRequest struct {
	Image        *imageSource   `json:"image"`
	Features     []feature      `json:"features"`
	ImageContext map[string]any `json:"imageContext"`
}

type imageSource struct {
	Content string    `json:"content"`
	Source  *imageURI `json:"source"`
}

type imageURI struct {
	ImageUri    string `json:"imageUri"`
	GcsImageUri string `json:"gcsImageUri"`
}

type feature struct {
	Type       string `json:"type"`
	MaxResults int    `json:"maxResults"`
}

func (api *API) annotate(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req annotateRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid request body")
		return
	}
	if err := ensureEOF(decoder); err != nil {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid request body")
		return
	}
	if len(req.Requests) == 0 {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'requests' is required")
		return
	}
	if len(req.Requests) > 16 {
		gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'requests' must contain at most 16 items")
		return
	}
	for requestIndex, imgReq := range req.Requests {
		if imgReq.Image == nil {
			gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'requests.image' is required")
			return
		}
		if err := validateImage(imgReq.Image); err != nil {
			gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		if len(imgReq.ImageContext) != 0 {
			gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
				"field 'requests.imageContext' is not implemented")
			return
		}
		if len(imgReq.Features) == 0 {
			gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "field 'requests.features' is required")
			return
		}
		for _, feat := range imgReq.Features {
			switch feat.Type {
			case "LABEL_DETECTION", "TEXT_DETECTION", "DOCUMENT_TEXT_DETECTION",
				"FACE_DETECTION", "LANDMARK_DETECTION", "LOGO_DETECTION",
				"SAFE_SEARCH_DETECTION", "IMAGE_PROPERTIES", "OBJECT_LOCALIZATION",
				"WEB_DETECTION", "PRODUCT_SEARCH":
				// All recognized Vision features require semantic inference. Returning
				// deterministic labels or confidence values would fabricate parity.
			default:
				gcpError(w, http.StatusBadRequest, "INVALID_ARGUMENT",
					"field 'requests.features.type' contains an unsupported value")
				return
			}
		}
		_ = requestIndex
	}
	gcpError(w, http.StatusNotImplemented, "UNIMPLEMENTED",
		"Cloud Vision semantic image annotation is not implemented")
}

func validateImage(image *imageSource) error {
	hasContent := image.Content != ""
	hasSource := image.Source != nil && (image.Source.ImageUri != "" || image.Source.GcsImageUri != "")
	if hasContent == hasSource {
		return errors.New("field 'requests.image' must set exactly one of 'content' or 'source'")
	}
	if hasContent {
		decoded, err := base64.StdEncoding.DecodeString(image.Content)
		if err != nil {
			return errors.New("field 'requests.image.content' must be valid base64")
		}
		if len(decoded) > 4<<20 {
			return errors.New("field 'requests.image.content' exceeds the 4 MiB local limit")
		}
		return nil
	}
	raw := image.Source.ImageUri
	if raw == "" {
		raw = image.Source.GcsImageUri
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil {
		return errors.New("field 'requests.image.source' contains an unsafe URI")
	}
	switch parsed.Scheme {
	case "gs":
		if parsed.Host == "" {
			return errors.New("field 'requests.image.source' must name a GCS bucket")
		}
		return nil
	case "http", "https":
		return errors.New("field 'requests.image.source' does not allow external HTTP URLs")
	default:
		return errors.New("field 'requests.image.source' must use a gs:// URI")
	}
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
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
