package clouddeploy

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"minisky/pkg/state"
)

func TestDeletePipelineRequiresNoReleases(t *testing.T) {
	api := newTestAPI()
	pipeline := "projects/p1/locations/us-central1/deliveryPipelines/pipe1"
	api.pipelines[pipeline] = &DeliveryPipeline{Name: pipeline}
	api.releases[pipeline+"/releases/r1"] = &Release{Name: pipeline + "/releases/r1"}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/"+pipeline, nil))
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status=%d, want 412: %s", rec.Code, rec.Body.String())
	}
}

func TestCloudDeployCleanupRequiresChildFirst(t *testing.T) {
	api := newTestAPI()
	pipeline := "projects/p1/locations/us-central1/deliveryPipelines/pipe1"
	release := pipeline + "/releases/r1"
	rollout := release + "/rollouts/ro1"
	api.pipelines[pipeline] = &DeliveryPipeline{Name: pipeline}
	api.releases[release] = &Release{Name: release}
	api.rollouts[rollout] = &Rollout{Name: rollout}

	for _, path := range []string{rollout, release, pipeline} {
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/"+path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("delete %s status=%d: %s", path, rec.Code, rec.Body.String())
		}
	}
	if len(api.pipelines)+len(api.releases)+len(api.rollouts) != 0 {
		t.Fatal("hierarchy cleanup left resources")
	}
}

func TestRolloutExecutesLoopbackTarget(t *testing.T) {
	delivered := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s, want POST", r.Method)
		}
		delivered <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	api := newTestAPI()
	release := "projects/p1/locations/us-central1/deliveryPipelines/pipe1/releases/r1"
	api.pipelines["projects/p1/locations/us-central1/deliveryPipelines/pipe1"] = &DeliveryPipeline{Name: "projects/p1/locations/us-central1/deliveryPipelines/pipe1"}
	api.releases[release] = &Release{Name: release}
	body := `{"targetId":"local","localTarget":"` + target.URL + `"}`
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+release+"/rollouts?rolloutId=roll1", bytes.NewBufferString(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d: %s", rec.Code, rec.Body.String())
	}
	select {
	case <-delivered:
	case <-time.After(4 * time.Second):
		t.Fatal("local rollout was not delivered")
	}
}

func TestRolloutRejectsSSRFAndUnsupportedStrategy(t *testing.T) {
	api := newTestAPI()
	release := "projects/p1/locations/us-central1/deliveryPipelines/pipe1/releases/r1"
	api.pipelines["projects/p1/locations/us-central1/deliveryPipelines/pipe1"] = &DeliveryPipeline{Name: "projects/p1/locations/us-central1/deliveryPipelines/pipe1"}
	api.releases[release] = &Release{Name: release}
	tests := []struct {
		name string
		body string
		code int
	}{
		{"ssrf", `{"targetId":"local","localTarget":"http://169.254.169.254/latest"}`, http.StatusBadRequest},
		{"strategy", `{"targetId":"prod","strategy":{"canary":{"percentages":[10,100]}}}`, http.StatusNotImplemented},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+release+"/rollouts?rolloutId="+test.name, bytes.NewBufferString(test.body)))
			if rec.Code != test.code {
				t.Fatalf("status=%d, want %d: %s", rec.Code, test.code, rec.Body.String())
			}
		})
	}
}

func TestCloudDeployHierarchySurvivesRestart(t *testing.T) {
	store, err := state.New(t.TempDir(), "restart")
	if err != nil {
		t.Fatal(err)
	}
	api := newTestAPI()
	api.stateStore = store
	pipeline := "projects/p1/locations/us-central1/deliveryPipelines/pipe1"
	release := pipeline + "/releases/r1"
	rollout := release + "/rollouts/ro1"
	api.pipelines[pipeline] = &DeliveryPipeline{Name: pipeline}
	api.releases[release] = &Release{Name: release}
	api.rollouts[rollout] = &Rollout{Name: rollout, State: "SUCCEEDED"}
	if err := api.persistState(); err != nil {
		t.Fatal(err)
	}
	restarted := newTestAPI()
	restarted.stateStore = store
	if err := restarted.loadState(); err != nil {
		t.Fatal(err)
	}
	if restarted.pipelines[pipeline] == nil || restarted.releases[release] == nil || restarted.rollouts[rollout] == nil {
		t.Fatal("deploy hierarchy was not restored")
	}
}

func TestRolloutSaveFailureRollsBack(t *testing.T) {
	api := newTestAPI()
	api.stateStore = failingDeployStore{}
	pipeline := "projects/p1/locations/us-central1/deliveryPipelines/pipe1"
	release := pipeline + "/releases/r1"
	api.pipelines[pipeline] = &DeliveryPipeline{Name: pipeline}
	api.releases[release] = &Release{Name: release}
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
		"/v1/"+release+"/rollouts?rolloutId=ro1", bytes.NewBufferString(`{"targetId":"local"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503: %s", rec.Code, rec.Body.String())
	}
	if len(api.rollouts) != 0 {
		t.Fatal("failed rollout save remained visible")
	}
}

type failingDeployStore struct{}

func (failingDeployStore) Load(string, any) error { return state.ErrNotFound }
func (failingDeployStore) Save(string, any) error { return errors.New("disk full") }
