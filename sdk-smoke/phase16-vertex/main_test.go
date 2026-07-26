package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
)

func TestInvokePredictionUsesGeneratedClientAndCanonicalPath(t *testing.T) {
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method=%s", r.Method)
		}
		if r.URL.Path != "/_minisky/aiplatform/v1/projects/sdk-project/locations/us-central1/endpoints/sdk-endpoint:predict" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(`{
			"predictions":[
				{"instance":{"feature":2,"name":"alpha"},"score":0.4920141316233236},
				{"instance":[1,2,3],"score":0.9634117816955344}
			],
			"deployedModelId":"minisky-deterministic",
			"model":"projects/sdk-project/locations/us-central1/models/minisky-deterministic",
			"modelDisplayName":"MiniSky deterministic predictor",
			"modelVersionId":"1",
			"metadata":{"simulation":"deterministic-local"}
		}`))
	}))
	defer server.Close()

	got, err := invokePrediction(context.Background(), server.URL, "sdk-project", "us-central1", "sdk-endpoint")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateExpectedEvidence(got, "sdk-project", "us-central1"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotBody["labels"], map[string]any{"phase": "phase16", "purpose": "restart-determinism"}) {
		t.Fatalf("labels=%#v", gotBody["labels"])
	}
	if !reflect.DeepEqual(gotBody["parameters"], map[string]any{"scale": float64(2), "temperature": float64(0)}) {
		t.Fatalf("parameters=%#v", gotBody["parameters"])
	}
	if instances, ok := gotBody["instances"].([]any); !ok || len(instances) != 2 {
		t.Fatalf("instances=%#v", gotBody["instances"])
	}
}

func TestEvidenceRoundTripAndExactComparison(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "vertex-response.json")
	want := expectedEvidence("sdk-project", "us-central1")
	if err := writeEvidence(path, root, want); err != nil {
		t.Fatal(err)
	}
	got, err := readEvidence(path, root)
	if err != nil {
		t.Fatal(err)
	}
	if err := compareEvidence(got, want); err != nil {
		t.Fatal(err)
	}
	got.Predictions[0].Score++
	if err := compareEvidence(got, want); err == nil {
		t.Fatal("expected score mismatch")
	}
}

func TestEvidencePathMustStayInsideOwnedRoot(t *testing.T) {
	root := t.TempDir()
	if err := validateEvidencePath(filepath.Join(root, "response.json"), root, false); err != nil {
		t.Fatal(err)
	}
	if err := validateEvidencePath(filepath.Join(root, "..", "outside.json"), root, false); err == nil {
		t.Fatal("expected outside path rejection")
	}
	if err := validateEvidencePath(filepath.Join(root, "missing", "response.json"), root, false); err == nil {
		t.Fatal("expected missing parent rejection")
	}
}
