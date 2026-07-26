package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestVPCNetworkIdentityIsProjectScopedAndDockerSafe(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	first := VPCNetworkIdentity{Project: "project-a", Network: "shared"}
	second := VPCNetworkIdentity{Project: "project-b", Network: "shared"}

	firstName, err := first.DockerName()
	if err != nil {
		t.Fatal(err)
	}
	secondName, err := second.DockerName()
	if err != nil {
		t.Fatal(err)
	}
	if firstName == secondName {
		t.Fatalf("project-scoped identities collided at %q", firstName)
	}
	if len(firstName) > 63 || !validDockerResourceName(firstName) {
		t.Fatalf("Docker name %q is not safe", firstName)
	}
	if got := first.CanonicalResource(); got != "projects/project-a/global/networks/shared" {
		t.Fatalf("canonical resource = %q", got)
	}
	for _, invalid := range []VPCNetworkIdentity{
		{Project: "", Network: "shared"},
		{Project: "../escape", Network: "shared"},
		{Project: "project-a", Network: "Bad"},
	} {
		if _, err := invalid.DockerName(); err == nil {
			t.Fatalf("invalid identity accepted: %#v", invalid)
		}
	}
}

func TestEnsureVPCNetworkIPAMCreatesExactOwnedBridge(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	var created map[string]any
	manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path != "/networks":
			return dockerResponse(http.StatusNotFound, `{}`), nil
		case request.Method == http.MethodGet && request.URL.Path == "/networks":
			return dockerResponse(http.StatusOK, `[]`), nil
		case request.Method == http.MethodPost && request.URL.Path == "/networks/create":
			if err := json.NewDecoder(request.Body).Decode(&created); err != nil {
				t.Fatal(err)
			}
			return dockerResponse(http.StatusCreated, `{"Id":"bridge-id"}`), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})

	state, err := manager.EnsureVPCNetworkIPAM(context.Background(), identity, "10.42.0.0/24")
	if err != nil {
		t.Fatal(err)
	}
	if !state.Created || state.CIDR != "10.42.0.0/24" || state.Name == "" {
		t.Fatalf("state = %#v", state)
	}
	labels := created["Labels"].(map[string]any)
	for key, want := range map[string]string{
		"managed-by":                 "minisky",
		"minisky.profile":            "ipam-test",
		"minisky.service":            "compute-network",
		"minisky.project":            "project-a",
		"minisky.network":            "private",
		"minisky.canonical-resource": identity.CanonicalResource(),
	} {
		if labels[key] != want {
			t.Fatalf("label %s = %v, want %q; all=%#v", key, labels[key], want, labels)
		}
	}
	ipam := created["IPAM"].(map[string]any)
	if got := ipam["Config"].([]any)[0].(map[string]any)["Subnet"]; got != "10.42.0.0/24" {
		t.Fatalf("subnet = %v", got)
	}
}

func TestEnsureVPCNetworkIPAMExistingRequiresExactOwnershipAndCIDR(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	labels, err := identity.labels()
	if err != nil {
		t.Fatal(err)
	}
	labelJSON, _ := json.Marshal(labels)
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name: "matching",
			body: `{"Name":"ignored","Driver":"bridge","Labels":` + string(labelJSON) +
				`,"IPAM":{"Driver":"default","Config":[{"Subnet":"10.42.0.0/24"}]}}`,
		},
		{
			name: "mismatch",
			body: `{"Driver":"bridge","Labels":` + string(labelJSON) +
				`,"IPAM":{"Driver":"default","Config":[{"Subnet":"10.43.0.0/24"}]}}`,
			wantErr: "CIDR",
		},
		{
			name:    "unowned",
			body:    `{"Driver":"bridge","Labels":{"managed-by":"someone-else"},"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`,
			wantErr: "not exactly owned",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutations := 0
			manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
				if request.Method != http.MethodGet {
					mutations++
				}
				return dockerResponse(http.StatusOK, test.body), nil
			})
			state, err := manager.EnsureVPCNetworkIPAM(context.Background(), identity, "10.42.0.0/24")
			if test.wantErr == "" {
				if err != nil || state.Created {
					t.Fatalf("state=%#v error=%v", state, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error=%v, want %q", err, test.wantErr)
			}
			if mutations != 0 {
				t.Fatalf("existing network caused %d mutations", mutations)
			}
		})
	}
}

func TestEnsureVPCNetworkIPAMRejectsOwnedOverlapBeforeCreate(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	other := VPCNetworkIdentity{Project: "project-b", Network: "other"}
	labels, _ := other.labels()
	labelJSON, _ := json.Marshal(labels)
	manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/networks/" + mustVPCName(t, identity):
			return dockerResponse(http.StatusNotFound, `{}`), nil
		case "/networks":
			return dockerResponse(http.StatusOK, `[{"Name":"other","Labels":`+string(labelJSON)+
				`,"IPAM":{"Config":[{"Subnet":"10.42.0.128/25"}]}}]`), nil
		default:
			t.Fatalf("overlap caused mutation: %s %s", request.Method, request.URL)
			return nil, nil
		}
	})
	if _, err := manager.EnsureVPCNetworkIPAM(context.Background(), identity, "10.42.0.0/24"); err == nil ||
		!strings.Contains(err.Error(), "overlaps") {
		t.Fatalf("overlap error = %v", err)
	}
}

func TestEnsureVPCNetworkIPAMReconcilesConcurrentCreateConflict(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	labels, _ := identity.labels()
	labelJSON, _ := json.Marshal(labels)
	inspectCalls := 0
	manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path != "/networks":
			inspectCalls++
			if inspectCalls == 1 {
				return dockerResponse(http.StatusNotFound, `{}`), nil
			}
			return dockerResponse(http.StatusOK, `{"Id":"winner","Driver":"bridge","Labels":`+
				string(labelJSON)+`,"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`), nil
		case request.Method == http.MethodGet:
			return dockerResponse(http.StatusOK, `[]`), nil
		case request.Method == http.MethodPost:
			return dockerResponse(http.StatusConflict, `{"message":"already exists"}`), nil
		default:
			t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})
	state, err := manager.EnsureVPCNetworkIPAM(context.Background(), identity, "10.42.0.0/24")
	if err != nil || state.Created || state.ID != "winner" {
		t.Fatalf("concurrent ensure state=%#v error=%v", state, err)
	}
}

func TestDeleteVPCNetworkIPAMVerifiesOwnershipCIDRAndPropagatesConflict(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	labels, _ := identity.labels()
	labelJSON, _ := json.Marshal(labels)
	deleted := false
	manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			return dockerResponse(http.StatusOK, `{"Id":"owned-id","Driver":"bridge","Labels":`+string(labelJSON)+
				`,"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`), nil
		}
		deleted = true
		return dockerResponse(http.StatusConflict, `{"message":"network has active endpoints"}`), nil
	})
	err := manager.DeleteVPCNetworkIPAM(context.Background(), identity, "10.42.0.0/24")
	if err == nil || !strings.Contains(err.Error(), "active endpoints") || !deleted {
		t.Fatalf("delete error=%v deleted=%t", err, deleted)
	}
}

func TestVPCNetworkIPAMUsesBoundedContextAndClosesBodies(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	closed := false
	manager := &ServiceManager{
		dockerTimeout: 20 * time.Millisecond,
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			<-request.Context().Done()
			closed = true
			return nil, request.Context().Err()
		})},
	}
	_, err := manager.EnsureVPCNetworkIPAM(context.Background(), identity, "10.42.0.0/24")
	if !errors.Is(err, context.DeadlineExceeded) || !closed {
		t.Fatalf("timeout error=%v observed=%t", err, closed)
	}
}

func TestVPCNetworkIPAMDrainsAndClosesEveryDockerResponse(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	var bodies []*trackingBody
	manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
		status, payload := http.StatusOK, `[]`
		switch {
		case request.Method == http.MethodGet && request.URL.Path != "/networks":
			status, payload = http.StatusNotFound, `{}`
		case request.Method == http.MethodPost:
			status, payload = http.StatusCreated, `{"Id":"created"}`
		}
		body := newTrackingBody(payload)
		bodies = append(bodies, body)
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: body}, nil
	})
	if _, err := manager.EnsureVPCNetworkIPAM(context.Background(), identity, "10.42.0.0/24"); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 3 {
		t.Fatalf("Docker responses=%d, want 3", len(bodies))
	}
	for index, body := range bodies {
		body.mu.Lock()
		closed := body.closed
		body.mu.Unlock()
		if !closed {
			t.Fatalf("Docker response body %d was not closed", index)
		}
	}
}

func TestVPCNetworkIPAMLimitsDockerErrorBodies(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet && request.URL.Path != "/networks" {
			return dockerResponse(http.StatusNotFound, `{}`), nil
		}
		if request.Method == http.MethodGet {
			return dockerResponse(http.StatusOK, `[]`), nil
		}
		return dockerResponse(http.StatusInternalServerError, strings.Repeat("x", maxDockerErrorBody*2)), nil
	})
	_, err := manager.EnsureVPCNetworkIPAM(context.Background(), identity, "10.42.0.0/24")
	if err == nil || len(err.Error()) > maxDockerErrorBody+256 {
		t.Fatalf("bounded create error length=%d error=%v", len(err.Error()), err)
	}
}

func newVPCDockerManager(t *testing.T, fn func(*http.Request) (*http.Response, error)) *ServiceManager {
	t.Helper()
	return &ServiceManager{
		dockerTimeout: time.Second,
		dockerClient:  &http.Client{Transport: roundTripFunc(fn)},
	}
}

func mustVPCName(t *testing.T, identity VPCNetworkIdentity) string {
	t.Helper()
	name, err := identity.DockerName()
	if err != nil {
		t.Fatal(err)
	}
	return name
}

type trackingBody struct {
	io.Reader
	mu     sync.Mutex
	closed bool
}

func (body *trackingBody) Close() error {
	body.mu.Lock()
	body.closed = true
	body.mu.Unlock()
	return nil
}

func newTrackingBody(value string) *trackingBody {
	return &trackingBody{Reader: bytes.NewBufferString(value)}
}
