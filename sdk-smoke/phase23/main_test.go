package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRequireGoogleAPIVersion(t *testing.T) {
	if err := requireGoogleAPIVersion(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateLoopbackEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"http://127.0.0.1:8080",
		"http://[::1]:8080",
		"http://localhost:8080",
	} {
		if err := validateLoopbackEndpoint(endpoint); err != nil {
			t.Errorf("validateLoopbackEndpoint(%q): %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{
		"", "https://127.0.0.1:8080", "http://192.0.2.1:8080",
		"http://localhost", "http://user@localhost:8080",
		"http://localhost:8080/path", "http://localhost:8080?query=1",
	} {
		if err := validateLoopbackEndpoint(endpoint); err == nil {
			t.Errorf("validateLoopbackEndpoint(%q) unexpectedly succeeded", endpoint)
		}
	}
}

func TestConfigRequiresOptInSafeIdentifiersAndAbsoluteEvidence(t *testing.T) {
	setValidEnv(t)
	t.Setenv("MINISKY_PHASE23_MODE", "create")
	if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), optInEnv) {
		t.Fatalf("missing opt-in error=%v", err)
	}
	t.Setenv(optInEnv, "1")
	if _, err := configFromEnv(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	for _, test := range []struct {
		name, key, value, want string
	}{
		{"external endpoint", "MINISKY_ENDPOINT", "http://example.com:8080", "loopback"},
		{"unsafe project", "MINISKY_PROJECT_ID", "../project", "project"},
		{"relative evidence", "MINISKY_PHASE23_EVIDENCE", "evidence.json", "absolute"},
		{"short sentinel", "MINISKY_PHASE23_SENSITIVE_SENTINEL", "short", "16-128"},
	} {
		t.Run(test.name, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("MINISKY_PHASE23_MODE", "create")
			t.Setenv(optInEnv, "1")
			t.Setenv(test.key, test.value)
			if _, err := configFromEnv(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want substring %q", err, test.want)
			}
		})
	}
}

func TestGeneratedClientsUseCanonicalFullDomainPaths(t *testing.T) {
	responses := map[string]string{
		"/_minisky/speech.googleapis.com/v1/speech:recognize":                            `{}`,
		"/_minisky/texttospeech.googleapis.com/v1/text:synthesize":                       `{}`,
		"/_minisky/translate.googleapis.com/v3/projects/demo/locations/us:translateText": `{"translations":[]}`,
		"/_minisky/vision.googleapis.com/v1/images:annotate":                             `{"responses":[]}`,
		"/_minisky/language.googleapis.com/v1/documents:analyzeSentiment":                `{}`,
		"/_minisky/documentai.googleapis.com/v1/projects/demo/locations/us/processors":   `{"processors":[]}`,
		"/_minisky/dialogflow.googleapis.com/v3/projects/demo/locations/us/agents":       `{"agents":[]}`,
		"/_minisky/aiplatform.googleapis.com/v1/projects/demo/locations/us/indexes":      `{"indexes":[]}`,
	}
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := responses[r.URL.Path]
		if !ok {
			http.Error(w, fmt.Sprintf("unexpected path %q", r.URL.Path), http.StatusNotFound)
			return
		}
		if strings.Contains(r.URL.Path, "vision.googleapis.com") &&
			r.Header.Get("X-Goog-User-Project") != "demo" {
			http.Error(w, "missing Vision quota project", http.StatusBadRequest)
			return
		}
		seen[r.URL.Path] = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)
	c, err := newClients(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	parent := "projects/demo/locations/us"
	calls := []func() error{
		func() error { _, err := c.speech.Speech.Recognize(speechRequest("x")).Do(); return err },
		func() error { _, err := c.tts.Text.Synthesize(ttsRequest("x")).Do(); return err },
		func() error {
			_, err := c.translate.Projects.Locations.TranslateText(parent, translationRequest("x", "en", "en")).Do()
			return err
		},
		func() error {
			_, err := annotateVision(context.Background(), c.vision, visionRequest("x"), "demo")
			return err
		},
		func() error { _, err := c.language.Documents.AnalyzeSentiment(languageRequest("x")).Do(); return err },
		func() error { _, err := c.document.Projects.Locations.Processors.List(parent).Do(); return err },
		func() error { _, err := c.dialog.Projects.Locations.Agents.List(parent).Do(); return err },
		func() error { _, err := c.vertex.Projects.Locations.Indexes.List(parent).Do(); return err },
	}
	for _, call := range calls {
		if err := call(); err != nil {
			t.Fatal(err)
		}
	}
	for path := range responses {
		if !seen[path] {
			t.Errorf("generated client did not request %s", path)
		}
	}
}

func TestGeneratedClientErrorsRetainGCPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = fmt.Fprint(w, `{"error":{"code":501,"message":"not implemented","status":"UNIMPLEMENTED"}}`)
	}))
	t.Cleanup(server.Close)
	c, err := newClients(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, callErr := c.speech.Speech.Recognize(speechRequest("x")).Do()
	if err := expectGoogleStatus(callErr, 501, "UNIMPLEMENTED"); err != nil {
		t.Fatal(err)
	}
}

func TestEvidenceIsBoundedStrictProjectIsolatedAndSecretFree(t *testing.T) {
	cfg := testConfig(t)
	record := validEvidence(cfg)
	if err := writeEvidence(cfg.evidencePath, record, cfg.sensitiveSentinel); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(cfg.evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("evidence mode=%o want=600", info.Mode().Perm())
	}
	got, err := readEvidence(cfg.evidencePath, cfg, cfg.sensitiveSentinel)
	if err != nil {
		t.Fatal(err)
	}
	if got != record {
		t.Fatalf("evidence=%+v want=%+v", got, record)
	}

	t.Run("project mismatch", func(t *testing.T) {
		other := cfg
		other.project = "other"
		if _, err := readEvidence(cfg.evidencePath, other, cfg.sensitiveSentinel); err == nil {
			t.Fatal("project-mismatched evidence unexpectedly accepted")
		}
	})
	t.Run("unknown field", func(t *testing.T) {
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		data = []byte(strings.Replace(string(data), `"version":1`, `"version":1,"unknown":true`, 1))
		path := filepath.Join(t.TempDir(), "evidence.json")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readEvidence(path, cfg, cfg.sensitiveSentinel); err == nil ||
			!strings.Contains(err.Error(), "unknown field") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("oversized", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		if err := os.WriteFile(path, []byte(strings.Repeat("x", maxEvidenceBytes+1)), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readEvidence(path, cfg, cfg.sensitiveSentinel); err == nil ||
			!strings.Contains(err.Error(), "size") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("sensitive content", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		if err := os.WriteFile(path, []byte(cfg.sensitiveSentinel), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readEvidence(path, cfg, cfg.sensitiveSentinel); err == nil ||
			!strings.Contains(err.Error(), "sensitive") {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestOperationTarget(t *testing.T) {
	got, err := operationTarget(map[string]any{"verb": "create", "target": "projects/p/locations/us/indexes/i"})
	if err != nil || got != "projects/p/locations/us/indexes/i" {
		t.Fatalf("target=%q err=%v", got, err)
	}
	if _, err := operationTarget(map[string]any{"verb": "create"}); err == nil {
		t.Fatal("missing target unexpectedly accepted")
	}
}

func setValidEnv(t *testing.T) {
	t.Helper()
	t.Setenv("MINISKY_ENDPOINT", "http://127.0.0.1:8080")
	t.Setenv("MINISKY_PROJECT_ID", "demo")
	t.Setenv("MINISKY_PHASE23_LOCATION", "us")
	t.Setenv("MINISKY_PHASE23_EVIDENCE", filepath.Join(t.TempDir(), "evidence.json"))
	t.Setenv("MINISKY_PHASE23_SENSITIVE_SENTINEL", "phase23-sensitive-unit-probe")
}

func testConfig(t *testing.T) config {
	t.Helper()
	setValidEnv(t)
	t.Setenv("MINISKY_PHASE23_MODE", "create")
	t.Setenv(optInEnv, "1")
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func validEvidence(cfg config) evidence {
	parent := locationParent(cfg)
	return evidence{
		Version: evidenceVersion, GoogleAPIVersion: googleAPIVersion,
		Project: cfg.project, Location: cfg.location,
		DocumentProcessor:    parent + "/processors/proc-1",
		DialogflowAgent:      parent + "/agents/agent-1",
		VertexIndex:          parent + "/indexes/index-1",
		VertexModel:          parent + "/models/model-1",
		VertexIndexOperation: parent + "/operations/operation-2",
		VertexModelOperation: parent + "/operations/operation-4",
		IdentityTranslation:  true, SemanticBoundaries: "EXPLICIT_UNIMPLEMENTED",
		PayloadBounds: "VERIFIED", UnsupportedOptions: "EXPLICIT_UNIMPLEMENTED",
		ProjectIsolation: true, LocalExtensions: "NONE",
	}
}
