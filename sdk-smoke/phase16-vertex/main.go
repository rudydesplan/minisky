package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	aiplatform "google.golang.org/api/aiplatform/v1"
	"google.golang.org/api/option"
)

type predictionEvidence struct {
	Instance json.RawMessage `json:"instance"`
	Score    float64         `json:"score"`
}

type responseEvidence struct {
	Predictions      []predictionEvidence `json:"predictions"`
	DeployedModelID  string               `json:"deployedModelId"`
	Model            string               `json:"model"`
	ModelDisplayName string               `json:"modelDisplayName"`
	ModelVersionID   string               `json:"modelVersionId"`
	Simulation       string               `json:"simulation"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "phase 16 Vertex AI smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	mode := strings.TrimSpace(os.Getenv("MINISKY_PHASE16_VERTEX_MODE"))
	gateway := strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/")
	evidencePath := strings.TrimSpace(os.Getenv("MINISKY_PHASE16_VERTEX_EVIDENCE"))
	evidenceRoot := strings.TrimSpace(os.Getenv("MINISKY_PHASE16_VERTEX_EVIDENCE_ROOT"))
	if mode != "record" && mode != "verify" {
		return fmt.Errorf("MINISKY_PHASE16_VERTEX_MODE must be record or verify")
	}
	if gateway == "" || evidencePath == "" || evidenceRoot == "" {
		return fmt.Errorf("MINISKY_ENDPOINT, MINISKY_PHASE16_VERTEX_EVIDENCE, and MINISKY_PHASE16_VERTEX_EVIDENCE_ROOT are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	project := env("MINISKY_PROJECT_ID", "phase16-project")
	location := env("MINISKY_PHASE16_VERTEX_LOCATION", "us-central1")
	current, err := invokePrediction(
		ctx,
		gateway,
		project,
		location,
		env("MINISKY_PHASE16_VERTEX_ENDPOINT", "deterministic-endpoint"),
	)
	if err != nil {
		return err
	}
	if err := validateExpectedEvidence(current, project, location); err != nil {
		return fmt.Errorf("validate live response: %w", err)
	}

	switch mode {
	case "record":
		if err := writeEvidence(evidencePath, evidenceRoot, current); err != nil {
			return err
		}
		fmt.Printf("recorded Vertex AI restart determinism evidence predictions=%d\n", len(current.Predictions))
		return nil
	case "verify":
		recorded, err := readEvidence(evidencePath, evidenceRoot)
		if err != nil {
			return err
		}
		if err := compareEvidence(current, recorded); err != nil {
			return fmt.Errorf("restart determinism verification: %w", err)
		}
		fmt.Printf("verified Vertex AI restart determinism predictions=%d deployedModelId=%s\n",
			len(current.Predictions), current.DeployedModelID)
		return nil
	default:
		return errors.New("unreachable Vertex AI smoke mode")
	}
}

func vertexService(ctx context.Context, gateway string) (*aiplatform.Service, error) {
	return aiplatform.NewService(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(strings.TrimRight(gateway, "/")+"/_minisky/aiplatform/"),
	)
}

func invokePrediction(ctx context.Context, gateway, project, location, endpointID string) (responseEvidence, error) {
	for name, value := range map[string]string{
		"project": project, "location": location, "endpoint": endpointID,
	} {
		if value == "" || strings.Contains(value, "/") {
			return responseEvidence{}, fmt.Errorf("%s must be a nonempty path segment", name)
		}
	}
	service, err := vertexService(ctx, gateway)
	if err != nil {
		return responseEvidence{}, fmt.Errorf("create Vertex AI client: %w", err)
	}
	request := &aiplatform.GoogleCloudAiplatformV1PredictRequest{
		Instances: []any{
			map[string]any{"feature": float64(2), "name": "alpha"},
			[]any{float64(1), float64(2), float64(3)},
		},
		Parameters: map[string]any{"scale": float64(2), "temperature": float64(0)},
		Labels: map[string]string{
			"phase":   "phase16",
			"purpose": "restart-determinism",
		},
	}
	name := fmt.Sprintf("projects/%s/locations/%s/endpoints/%s", project, location, endpointID)
	response, err := service.Projects.Locations.Endpoints.Predict(name, request).Context(ctx).Do()
	if err != nil {
		return responseEvidence{}, fmt.Errorf("predict with generated Vertex AI client: %w", err)
	}
	return evidenceFromResponse(response)
}

func evidenceFromResponse(response *aiplatform.GoogleCloudAiplatformV1PredictResponse) (responseEvidence, error) {
	evidence := responseEvidence{
		Predictions:      make([]predictionEvidence, len(response.Predictions)),
		DeployedModelID:  response.DeployedModelId,
		Model:            response.Model,
		ModelDisplayName: response.ModelDisplayName,
		ModelVersionID:   response.ModelVersionId,
	}
	for i, value := range response.Predictions {
		encoded, err := json.Marshal(value)
		if err != nil {
			return responseEvidence{}, fmt.Errorf("encode prediction %d: %w", i, err)
		}
		var prediction predictionEvidence
		if err := json.Unmarshal(encoded, &prediction); err != nil {
			return responseEvidence{}, fmt.Errorf("decode prediction %d: %w", i, err)
		}
		evidence.Predictions[i] = prediction
	}
	metadata, ok := response.Metadata.(map[string]any)
	if !ok {
		return responseEvidence{}, fmt.Errorf("metadata has type %T, want object", response.Metadata)
	}
	evidence.Simulation, ok = metadata["simulation"].(string)
	if !ok {
		return responseEvidence{}, fmt.Errorf("metadata.simulation is missing or not a string")
	}
	return evidence, nil
}

func expectedEvidence(project, location string) responseEvidence {
	return responseEvidence{
		Predictions: []predictionEvidence{
			{Instance: json.RawMessage(`{"feature":2,"name":"alpha"}`), Score: 0.4920141316233236},
			{Instance: json.RawMessage(`[1,2,3]`), Score: 0.9634117816955344},
		},
		DeployedModelID:  "minisky-deterministic",
		Model:            "projects/" + project + "/locations/" + location + "/models/minisky-deterministic",
		ModelDisplayName: "MiniSky deterministic predictor",
		ModelVersionID:   "1",
		Simulation:       "deterministic-local",
	}
}

func validateExpectedEvidence(got responseEvidence, project, location string) error {
	return compareEvidence(got, expectedEvidence(project, location))
}

func compareEvidence(got, want responseEvidence) error {
	if !reflect.DeepEqual(got, want) {
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		return fmt.Errorf("response=%s want=%s", gotJSON, wantJSON)
	}
	return nil
}

func writeEvidence(path, root string, evidence responseEvidence) error {
	if err := validateEvidencePath(path, root, false); err != nil {
		return err
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return fmt.Errorf("encode evidence: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create evidence: %w", err)
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		_ = file.Close()
		return fmt.Errorf("write evidence: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close evidence: %w", err)
	}
	return nil
}

func readEvidence(path, root string) (responseEvidence, error) {
	if err := validateEvidencePath(path, root, true); err != nil {
		return responseEvidence{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return responseEvidence{}, fmt.Errorf("open evidence: %w", err)
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return responseEvidence{}, fmt.Errorf("read evidence: %w", err)
	}
	if len(body) > 1<<20 {
		return responseEvidence{}, errors.New("evidence exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var evidence responseEvidence
	if err := decoder.Decode(&evidence); err != nil {
		return responseEvidence{}, fmt.Errorf("decode evidence: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return responseEvidence{}, errors.New("evidence contains trailing JSON")
	}
	return evidence, nil
}

func validateEvidencePath(path, root string, mustExist bool) error {
	rootPath, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve evidence root: %w", err)
	}
	rootPath, err = filepath.Abs(rootPath)
	if err != nil {
		return fmt.Errorf("absolute evidence root: %w", err)
	}
	parentPath, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("resolve evidence parent: %w", err)
	}
	parentPath, err = filepath.Abs(parentPath)
	if err != nil {
		return fmt.Errorf("absolute evidence parent: %w", err)
	}
	relative, err := filepath.Rel(rootPath, parentPath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("evidence path must remain inside the integration-owned root")
	}
	if mustExist {
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("inspect evidence: %w", err)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("evidence must be a regular file")
		}
	}
	return nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
