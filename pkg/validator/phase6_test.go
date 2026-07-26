package validator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPhase6CreateContracts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		domain       string
		method       string
		path         string
		validBody    string
		invalidBody  string
		invalidQuery bool
		wantMessage  string
	}{
		{
			name:        "storage bucket",
			domain:      "storage.googleapis.com",
			path:        "/storage/v1/b?project=test-project",
			validBody:   `{"name":"test-bucket","location":"US"}`,
			invalidBody: `{}`,
			wantMessage: "name",
		},
		{
			name:        "pubsub subscription",
			domain:      "pubsub.googleapis.com",
			method:      http.MethodPut,
			path:        "/v1/projects/test-project/subscriptions/test-subscription",
			validBody:   `{"topic":"projects/test-project/topics/test-topic","ackDeadlineSeconds":20}`,
			invalidBody: `{"ackDeadlineSeconds":20}`,
			wantMessage: "topic",
		},
		{
			name:         "secret manager secret",
			domain:       "secretmanager.googleapis.com",
			path:         "/v1/projects/test-project/secrets?secretId=test-secret",
			validBody:    `{"replication":{"automatic":{}},"labels":{"env":"test"}}`,
			invalidBody:  `{"replication":{"automatic":{}}}`,
			invalidQuery: true,
			wantMessage:  "secretId",
		},
		{
			name:        "secret manager replication",
			domain:      "secretmanager.googleapis.com",
			path:        "/v1/projects/test-project/secrets?secretId=test-secret",
			validBody:   `{"replication":{"automatic":{}}}`,
			invalidBody: `{}`,
			wantMessage: "replication",
		},
		{
			name:        "secret manager version",
			domain:      "secretmanager.googleapis.com",
			path:        "/v1/projects/test-project/secrets/test-secret:addVersion",
			validBody:   `{"payload":{"data":"c2VjcmV0"}}`,
			invalidBody: `{"payload":{}}`,
			wantMessage: "payload.data",
		},
		{
			name:         "kms key ring",
			domain:       "cloudkms.googleapis.com",
			path:         "/v1/projects/test-project/locations/us-central1/keyRings?keyRingId=test-ring",
			validBody:    `{}`,
			invalidBody:  `{}`,
			invalidQuery: true,
			wantMessage:  "keyRingId",
		},
		{
			name:         "kms crypto key",
			domain:       "cloudkms.googleapis.com",
			path:         "/v1/projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys?cryptoKeyId=test-key",
			validBody:    `{}`,
			invalidBody:  `{"purpose":"ENCRYPT_DECRYPT"}`,
			invalidQuery: true,
			wantMessage:  "cryptoKeyId",
		},
		{
			name:        "kms malformed resource",
			domain:      "cloudkms.googleapis.com",
			path:        "/v1/projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys?cryptoKeyId=test-key",
			validBody:   `{"purpose":"ENCRYPT_DECRYPT"}`,
			invalidBody: `{"purpose":`,
			wantMessage: "valid JSON",
		},
		{
			name:        "scheduler job",
			domain:      "cloudscheduler.googleapis.com",
			path:        "/v1/projects/test-project/locations/us-central1/jobs",
			validBody:   `{"name":"test-job","schedule":"* * * * *"}`,
			invalidBody: `{"name":"test-job","httpTarget":{"uri":"https://example.test"}}`,
			wantMessage: "schedule",
		},
		{
			name:        "tasks queue",
			domain:      "cloudtasks.googleapis.com",
			path:        "/v2/projects/test-project/locations/us-central1/queues",
			validBody:   `{"name":"projects/test-project/locations/us-central1/queues/test-queue","retryConfig":{"maxAttempts":3}}`,
			invalidBody: `{"retryConfig":{"maxAttempts":3}}`,
			wantMessage: "name",
		},
		{
			name:        "tasks task",
			domain:      "cloudtasks.googleapis.com",
			path:        "/v2/projects/test-project/locations/us-central1/queues/test-queue/tasks",
			validBody:   `{"task":{}}`,
			invalidBody: `{"task":null}`,
			wantMessage: "task",
		},
		{
			name:        "cloud build",
			domain:      "cloudbuild.googleapis.com",
			path:        "/v1/projects/test-project/builds",
			validBody:   `{"steps":[{"name":"ubuntu","args":["echo","hello"]}],"timeout":"600s"}`,
			invalidBody: `{"source":{"repoSource":{"repoName":"example/repo"}}}`,
			wantMessage: "steps",
		},
		{
			name:        "compute subnetwork name",
			domain:      "compute.googleapis.com",
			path:        "/compute/v1/projects/test-project/regions/us-central1/subnetworks",
			validBody:   `{"name":"subnet","ipCidrRange":"10.0.0.0/24","network":"custom"}`,
			invalidBody: `{"ipCidrRange":"10.0.0.0/24","network":"custom"}`,
			wantMessage: "name",
		},
		{
			name:        "compute subnetwork CIDR",
			domain:      "compute.googleapis.com",
			path:        "/compute/v1/projects/test-project/regions/us-central1/subnetworks",
			validBody:   `{"name":"subnet","ipCidrRange":"10.0.0.0/24","network":"custom"}`,
			invalidBody: `{"name":"subnet","network":"custom"}`,
			wantMessage: "ipCidrRange",
		},
		{
			name:        "compute subnetwork network",
			domain:      "compute.googleapis.com",
			path:        "/compute/v1/projects/test-project/regions/us-central1/subnetworks",
			validBody:   `{"name":"subnet","ipCidrRange":"10.0.0.0/24","network":"custom"}`,
			invalidBody: `{"name":"subnet","ipCidrRange":"10.0.0.0/24"}`,
			wantMessage: "network",
		},
		{
			name:        "regional cloud build",
			domain:      "cloudbuild.googleapis.com",
			path:        "/v1/projects/test-project/locations/us-central1/builds",
			validBody:   `{"steps":[{"name":"ubuntu"}]}`,
			invalidBody: `{}`,
			wantMessage: "steps",
		},
		{
			name:         "artifact registry repository",
			domain:       "artifactregistry.googleapis.com",
			path:         "/v1/projects/test-project/locations/us-central1/repositories?repositoryId=test-repository",
			validBody:    `{"format":"DOCKER","description":"test repository"}`,
			invalidBody:  `{"format":"DOCKER"}`,
			invalidQuery: true,
			wantMessage:  "repositoryId",
		},
		{
			name:        "artifact registry repository format",
			domain:      "artifactregistry.googleapis.com",
			path:        "/v1/projects/test-project/locations/us-central1/repositories?repositoryId=test-repository",
			validBody:   `{"format":"DOCKER","description":"test repository"}`,
			invalidBody: `{"description":"test repository"}`,
			wantMessage: "format",
		},
		{
			name:         "memorystore redis instance",
			domain:       "redis.googleapis.com",
			path:         "/v1/projects/test-project/locations/us-central1/instances?instanceId=cache",
			validBody:    `{"tier":"BASIC","memorySizeGb":1}`,
			invalidBody:  `{"tier":"BASIC","memorySizeGb":1}`,
			invalidQuery: true,
			wantMessage:  "instanceId",
		},
		{
			name:        "monitoring metric descriptor",
			domain:      "monitoring.googleapis.com",
			path:        "/v3/projects/test-project/metricDescriptors",
			validBody:   `{"type":"custom.googleapis.com/requests","metricKind":"GAUGE","valueType":"DOUBLE"}`,
			invalidBody: `{"metricKind":"GAUGE","valueType":"DOUBLE"}`,
			wantMessage: "type",
		},
		{
			name:        "monitoring time series",
			domain:      "monitoring.googleapis.com",
			path:        "/v3/projects/test-project/timeSeries",
			validBody:   `{"timeSeries":[{"metric":{"type":"custom.googleapis.com/requests"},"resource":{"type":"global"},"points":[{}]}]}`,
			invalidBody: `{}`,
			wantMessage: "timeSeries",
		},
		{
			name:        "logging entries write",
			domain:      "logging.googleapis.com",
			path:        "/v2/entries:write",
			validBody:   `{"entries":[{"logName":"projects/test-project/logs/app"}]}`,
			invalidBody: `{}`,
			wantMessage: "entries",
		},
		{
			name:        "logging sink",
			domain:      "logging.googleapis.com",
			path:        "/v2/projects/test-project/sinks",
			validBody:   `{"name":"errors","destination":"file://errors"}`,
			invalidBody: `{"name":"errors"}`,
			wantMessage: "destination",
		},
		{
			name:        "vertex generate content",
			domain:      "aiplatform.googleapis.com",
			path:        "/v1/projects/test-project/locations/us-central1/publishers/google/models/gemini:generateContent",
			validBody:   `{"contents":[{"parts":[{"text":"hello"}]}]}`,
			invalidBody: `{}`,
			wantMessage: "contents",
		},
		{
			name:        "vertex predict",
			domain:      "aiplatform.googleapis.com",
			path:        "/v1/projects/test-project/locations/us-central1/endpoints/test:predict",
			validBody:   `{"instances":[{"value":1}]}`,
			invalidBody: `{}`,
			wantMessage: "instances",
		},
	}

	validator := NewValidator()
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			method := tt.method
			if method == "" {
				method = http.MethodPost
			}
			validReq := newJSONRequest(t, method, tt.path, tt.validBody)
			validRec := httptest.NewRecorder()
			if ok := validator.ValidateRequestForDomain(validRec, validReq, tt.domain); !ok {
				t.Fatalf("valid request rejected with %d: %s", validRec.Code, validRec.Body.String())
			}

			invalidPath := tt.path
			if tt.invalidQuery {
				invalidPath = strings.SplitN(invalidPath, "?", 2)[0]
			}
			invalidReq := newJSONRequest(t, method, invalidPath, tt.invalidBody)
			invalidRec := httptest.NewRecorder()
			if ok := validator.ValidateRequestForDomain(invalidRec, invalidReq, tt.domain); ok {
				t.Fatal("invalid request passed validation")
			}
			assertInvalidArgument(t, invalidRec, tt.wantMessage)
		})
	}
}

func newJSONRequest(t *testing.T, method, path, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	return req
}

func assertInvalidArgument(t *testing.T, rec *httptest.ResponseRecorder, wantMessage string) {
	t.Helper()
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}

	var response struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
			Details []struct {
				Type    string `json:"@type"`
				Message string `json:"message"`
			} `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	if response.Error.Code != http.StatusBadRequest || response.Error.Status != "INVALID_ARGUMENT" {
		t.Fatalf("unexpected GCP error envelope: %+v", response.Error)
	}
	if !strings.Contains(response.Error.Message, wantMessage) {
		t.Fatalf("message = %q, want it to contain %q", response.Error.Message, wantMessage)
	}
	if len(response.Error.Details) != 1 ||
		response.Error.Details[0].Type != "type.googleapis.com/google.rpc.BadRequest" ||
		response.Error.Details[0].Message != response.Error.Message {
		t.Fatalf("unexpected BadRequest details: %+v", response.Error.Details)
	}
}
