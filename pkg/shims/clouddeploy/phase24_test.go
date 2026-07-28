package clouddeploy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"minisky/pkg/orchestrator"
	"minisky/pkg/registry"
	"minisky/pkg/shims/binaryauthorization"
	"minisky/pkg/state"
)

const phase24Image = "us-docker.pkg.dev/demo/releases/app@sha256:abc"

type phase24ErrorEvaluator struct {
	err error
}

func (e phase24ErrorEvaluator) EvaluateImage(string, string) error { return e.err }

func TestBootInjectsBinaryAuthorizationEvaluator(t *testing.T) {
	t.Setenv(registry.ExperimentalServicesEnv, "1")
	t.Setenv("MINISKY_STATE_DIR", t.TempDir())
	t.Setenv("MINISKY_PROFILE", "phase24-injection")
	handlers, _ := registry.BootAll(orchestrator.NewOperationManager(), nil)
	api, ok := handlers["clouddeploy.googleapis.com"].(*API)
	if !ok {
		t.Fatalf("Cloud Deploy handler = %T", handlers["clouddeploy.googleapis.com"])
	}
	if api.policyEvaluator == nil {
		t.Fatal("Binary Authorization evaluator was not injected")
	}
}

func TestRolloutBinaryAuthorizationDecision(t *testing.T) {
	tests := []struct {
		name         string
		project      string
		policy       *binaryauthorization.Policy
		wantState    string
		wantCalled   int32
		wantError    bool
		wantCode     int
		wantMessage  string
		unsupported  bool
		unavailable  bool
		evaluatorErr error
	}{
		{
			name:    "allow",
			project: "allowed",
			policy: &binaryauthorization.Policy{
				Name:                 "projects/allowed/policy",
				DefaultAdmissionRule: binaryauthorization.AdmissionRule{EvaluationMode: "ALWAYS_ALLOW"},
			},
			wantState:  "SUCCEEDED",
			wantCalled: 1,
		},
		{
			name:    "deny",
			project: "denied",
			policy: &binaryauthorization.Policy{
				Name:                 "projects/denied/policy",
				DefaultAdmissionRule: binaryauthorization.AdmissionRule{EvaluationMode: "DISALLOWED"},
			},
			wantState: "FAILED",
			wantError: true,
			wantCode:  7,
		},
		{
			name:    "dry-run denial does not block",
			project: "dry-run",
			policy: &binaryauthorization.Policy{
				Name: "projects/dry-run/policy",
				DefaultAdmissionRule: binaryauthorization.AdmissionRule{
					EvaluationMode:  "ALWAYS_DENY",
					EnforcementMode: "DRYRUN_AUDIT_LOG_ONLY",
				},
			},
			wantState:  "SUCCEEDED",
			wantCalled: 1,
		},
		{
			name:    "attestation remains explicitly unsupported",
			project: "attestation",
			policy: &binaryauthorization.Policy{
				Name: "projects/attestation/policy",
				DefaultAdmissionRule: binaryauthorization.AdmissionRule{
					EvaluationMode:        "REQUIRE_ATTESTATION",
					EnforcementMode:       "ENFORCED_BLOCK_AND_AUDIT_LOG",
					RequireAttestationsBy: []string{"projects/security/attestors/provenance"},
				},
			},
			wantState:   "FAILED",
			wantError:   true,
			wantCode:    12,
			wantMessage: "evaluation unsupported",
			unsupported: true,
		},
		{
			name:    "dry-run attestation does not block",
			project: "dry-run-attestation",
			policy: &binaryauthorization.Policy{
				Name: "projects/dry-run-attestation/policy",
				DefaultAdmissionRule: binaryauthorization.AdmissionRule{
					EvaluationMode:        "REQUIRE_ATTESTATION",
					EnforcementMode:       "DRYRUN_AUDIT_LOG_ONLY",
					RequireAttestationsBy: []string{"projects/security/attestors/provenance"},
				},
			},
			wantState:  "SUCCEEDED",
			wantCalled: 1,
		},
		{
			name:    "global policy remains explicitly unsupported",
			project: "global-policy",
			policy: &binaryauthorization.Policy{
				Name:                       "projects/global-policy/policy",
				GlobalPolicyEvaluationMode: "ENABLE",
				DefaultAdmissionRule: binaryauthorization.AdmissionRule{
					EvaluationMode:  "ALWAYS_ALLOW",
					EnforcementMode: "ENFORCED_BLOCK_AND_AUDIT_LOG",
				},
			},
			wantState:   "FAILED",
			wantError:   true,
			wantCode:    12,
			wantMessage: "evaluation unsupported",
			unsupported: true,
		},
		{
			name:      "missing policy defaults to deny",
			project:   "missing",
			wantState: "FAILED",
			wantError: true,
			wantCode:  7,
		},
		{
			name:         "persistence outage is unavailable",
			project:      "outage",
			wantState:    "FAILED",
			wantError:    true,
			wantCode:     14,
			wantMessage:  "unavailable",
			unavailable:  true,
			evaluatorErr: fmt.Errorf("sticky initialization: %w", binaryauthorization.ErrPersistence),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var called atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(target.Close)

			evaluator, err := binaryauthorization.NewAPIWithStore(nil, binaryauthorization.AllowAllAuthorizer{})
			if err != nil {
				t.Fatal(err)
			}
			if test.policy != nil {
				if err := evaluator.SetPolicy("projects/"+test.project, *test.policy); err != nil {
					t.Fatal(err)
				}
			}
			var policyEvaluator imagePolicyEvaluator = evaluator
			if test.evaluatorErr != nil {
				policyEvaluator = phase24ErrorEvaluator{err: test.evaluatorErr}
			}
			api, release := phase24API(test.project, policyEvaluator)
			opName := createPhase24Rollout(t, api, release, target.URL)
			op := waitPhase24Operation(t, api.opMgr, opName)

			api.mu.RLock()
			rollout := deepCopyRollout(api.rollouts[release+"/rollouts/roll1"])
			api.mu.RUnlock()
			if rollout.State != test.wantState {
				t.Fatalf("rollout state = %q, want %q", rollout.State, test.wantState)
			}
			if called.Load() != test.wantCalled {
				t.Fatalf("local target calls = %d, want %d", called.Load(), test.wantCalled)
			}
			if (op.Error != nil) != test.wantError {
				t.Fatalf("operation error = %#v, want error %t", op.Error, test.wantError)
			}
			if test.wantError && !strings.Contains(op.Error.Message, "Binary Authorization") {
				t.Fatalf("operation error = %q, want Binary Authorization reason", op.Error.Message)
			}
			if test.wantError && op.Error.Code != test.wantCode {
				t.Fatalf("operation error code = %d, want %d", op.Error.Code, test.wantCode)
			}
			if test.wantMessage != "" && !strings.Contains(op.Error.Message, test.wantMessage) {
				t.Fatalf("operation error = %q, want %q", op.Error.Message, test.wantMessage)
			}
			if test.unsupported && strings.Contains(op.Error.Message, "denied image") {
				t.Fatalf("unsupported evaluation reported as denial: %q", op.Error.Message)
			}
			if test.unavailable && strings.Contains(op.Error.Message, "denied image") {
				t.Fatalf("unavailable evaluation reported as denial: %q", op.Error.Message)
			}
		})
	}
}

func TestRolloutBinaryAuthorizationUsesRequestProject(t *testing.T) {
	evaluator, err := binaryauthorization.NewAPIWithStore(nil, binaryauthorization.AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := evaluator.SetPolicy("projects/project-a", binaryauthorization.Policy{
		Name:                 "projects/project-a/policy",
		DefaultAdmissionRule: binaryauthorization.AdmissionRule{EvaluationMode: "ALWAYS_ALLOW"},
	}); err != nil {
		t.Fatal(err)
	}
	var called atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	api, release := phase24API("project-b", evaluator)
	op := waitPhase24Operation(t, api.opMgr, createPhase24Rollout(t, api, release, target.URL))
	if op.Error == nil || called.Load() != 0 {
		t.Fatalf("cross-project policy decision: error=%#v calls=%d", op.Error, called.Load())
	}
}

func TestExecutableRolloutRequiresImageBeforeMutation(t *testing.T) {
	var called atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	api, release := phase24API("missing-image", allowImagePolicyEvaluator{})
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
		"/v1/"+release+"/rollouts?rolloutId=roll1",
		bytes.NewBufferString(`{"targetId":"local","localTarget":"`+target.URL+`"}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400: %s", rec.Code, rec.Body.String())
	}
	if called.Load() != 0 || len(api.rollouts) != 0 || len(api.opMgr.List()) != 0 {
		t.Fatalf("invalid rollout side effects: calls=%d rollouts=%d operations=%d",
			called.Load(), len(api.rollouts), len(api.opMgr.List()))
	}
}

func TestRolloutBinaryAuthorizationPolicySurvivesRestart(t *testing.T) {
	store := &phase24PolicyStore{}
	evaluator, err := binaryauthorization.NewAPIWithStore(store, binaryauthorization.AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := evaluator.SetPolicy("projects/restarted", binaryauthorization.Policy{
		Name:                 "projects/restarted/policy",
		DefaultAdmissionRule: binaryauthorization.AdmissionRule{EvaluationMode: "DISALLOWED"},
	}); err != nil {
		t.Fatal(err)
	}
	restarted, err := binaryauthorization.NewAPIWithStore(store, binaryauthorization.AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	var called atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	api, release := phase24API("restarted", restarted)
	op := waitPhase24Operation(t, api.opMgr, createPhase24Rollout(t, api, release, target.URL))
	if op.Error == nil || called.Load() != 0 {
		t.Fatalf("restarted policy decision: error=%#v calls=%d", op.Error, called.Load())
	}
}

func TestDeniedRolloutAndOperationSurviveRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "phase24-deploy-restart")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	evaluator, err := binaryauthorization.NewAPIWithStore(nil, binaryauthorization.AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := evaluator.SetPolicy("projects/deploy-restart", binaryauthorization.Policy{
		Name:                 "projects/deploy-restart/policy",
		DefaultAdmissionRule: binaryauthorization.AdmissionRule{EvaluationMode: "DISALLOWED"},
	}); err != nil {
		t.Fatal(err)
	}
	var called atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	api, release := phase24API("deploy-restart", evaluator)
	api.opMgr = manager
	api.stateStore = store
	operationName := createPhase24Rollout(t, api, release, target.URL)
	waitPhase24Operation(t, manager, operationName)

	restartedManager, err := orchestrator.NewOperationManagerWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	restarted := newTestAPI()
	restarted.opMgr = restartedManager
	restarted.stateStore = store
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if rollout := restarted.rollouts[release+"/rollouts/roll1"]; rollout == nil || rollout.State != "FAILED" {
		t.Fatalf("restarted rollout = %#v, want FAILED", rollout)
	}
	operation := restartedManager.Get(operationName)
	if operation == nil || !operation.Done || operation.Error == nil || operation.Error.Code != 7 {
		t.Fatalf("restarted operation = %#v, want terminal permission denial", operation)
	}
	if called.Load() != 0 {
		t.Fatalf("local target calls = %d, want 0", called.Load())
	}
}

func TestDeniedRolloutSaveFailureStillHasNoSideEffect(t *testing.T) {
	var called atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	evaluator, err := binaryauthorization.NewAPIWithStore(nil, binaryauthorization.AllowAllAuthorizer{})
	if err != nil {
		t.Fatal(err)
	}
	if err := evaluator.SetPolicy("projects/save-failure", binaryauthorization.Policy{
		Name:                 "projects/save-failure/policy",
		DefaultAdmissionRule: binaryauthorization.AdmissionRule{EvaluationMode: "DISALLOWED"},
	}); err != nil {
		t.Fatal(err)
	}
	api, release := phase24API("save-failure", evaluator)
	api.stateStore = &failSecondDeploySave{}
	op := waitPhase24Operation(t, api.opMgr, createPhase24Rollout(t, api, release, target.URL))
	if called.Load() != 0 {
		t.Fatalf("local target calls = %d, want 0", called.Load())
	}
	if op.Error == nil || !strings.Contains(op.Error.Message, "persistence") {
		t.Fatalf("operation error = %#v, want persistence failure", op.Error)
	}
	if op.Error.Code != 14 {
		t.Fatalf("operation error code = %d, want 14", op.Error.Code)
	}
}

func phase24API(project string, evaluator imagePolicyEvaluator) (*API, string) {
	api := newTestAPI()
	api.policyEvaluator = evaluator
	pipeline := "projects/" + project + "/locations/us-central1/deliveryPipelines/pipe1"
	release := pipeline + "/releases/r1"
	api.pipelines[pipeline] = &DeliveryPipeline{Name: pipeline}
	api.releases[release] = &Release{Name: release}
	return api, release
}

func createPhase24Rollout(t *testing.T, api *API, release, target string) string {
	t.Helper()
	body := `{"targetId":"local","image":"` + phase24Image + `","localTarget":"` + target + `"}`
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+release+"/rollouts?rolloutId=roll1", bytes.NewBufferString(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("create status = %d: %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	return response.Name
}

func waitPhase24Operation(t *testing.T, manager *orchestrator.OperationManager, name string) *orchestrator.Operation {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if operation := manager.Get(name); operation != nil && operation.Done {
			return operation
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("operation %q did not finish", name)
	return nil
}

type phase24PolicyStore struct {
	payload []byte
}

func (store *phase24PolicyStore) Load(_ string, target any) error {
	if store.payload == nil {
		return state.ErrNotFound
	}
	return json.Unmarshal(store.payload, target)
}

func (store *phase24PolicyStore) Save(_ string, value any) error {
	payload, err := json.Marshal(value)
	if err == nil {
		store.payload = payload
	}
	return err
}

type failSecondDeploySave struct {
	saves atomic.Int32
}

func (*failSecondDeploySave) Load(string, any) error { return state.ErrNotFound }

func (store *failSecondDeploySave) Save(string, any) error {
	if store.saves.Add(1) == 2 {
		return errors.New("disk full")
	}
	return nil
}
