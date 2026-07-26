package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestComputeInstanceIdentitySeparatesProjectAndZone(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "compute-identity")
	identities := []ComputeInstanceIdentity{
		{Project: "project-a", Zone: "us-central1-a", Instance: "shared"},
		{Project: "project-b", Zone: "us-central1-a", Instance: "shared"},
		{Project: "project-a", Zone: "us-central1-b", Instance: "shared"},
	}
	names := map[string]bool{}
	for _, identity := range identities {
		name, err := identity.DockerName()
		if err != nil {
			t.Fatal(err)
		}
		if len(name) > 63 || !validDockerResourceName(name) || names[name] {
			t.Fatalf("unsafe or duplicate name %q", name)
		}
		names[name] = true
	}
}

func TestComputeInstanceOwnershipRequiresCanonicalLabels(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "compute-identity")
	identity := ComputeInstanceIdentity{Project: "project-a", Zone: "us-central1-a", Instance: "shared"}
	name, err := identity.DockerName()
	if err != nil {
		t.Fatal(err)
	}
	labels, err := identity.labels()
	if err != nil {
		t.Fatal(err)
	}
	if labels["minisky.canonical-resource"] != identity.CanonicalResource() ||
		labels["minisky.project"] != identity.Project || labels["minisky.zone"] != identity.Zone {
		t.Fatalf("identity labels=%#v", labels)
	}
	legacy := ownedDockerLabels()
	legacy["minisky.service"] = "compute-instance"
	legacy["minisky.resource"] = name
	if exactLabels(legacy, labels) {
		t.Fatal("legacy profile/resource-only labels were accepted")
	}

	manager := &ServiceManager{
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodGet {
				t.Fatalf("legacy collision caused mutation: %s %s", request.Method, request.URL)
			}
			encoded, _ := json.Marshal(legacy)
			return dockerResponse(http.StatusOK,
				`{"State":{"Status":"running"},"Config":{"Labels":`+string(encoded)+`}}`), nil
		})},
	}
	if err := manager.ProvisionComputeInstance(
		context.Background(), identity, "image", "default", nil, nil, nil,
	); err == nil {
		t.Fatal("legacy Compute container was adopted")
	}
}

func TestComputeInstanceReadsRequireExactOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "compute-identity")
	identity := ComputeInstanceIdentity{Project: "project-a", Zone: "us-central1-a", Instance: "shared"}
	name, _ := identity.DockerName()
	exact, _ := identity.labels()
	exactJSON, _ := json.Marshal(exact)
	manager := &ServiceManager{
		portRegistry: map[string][]PortMapping{name: {{ContainerPort: "80", HostPort: "1234"}}},
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return dockerResponse(http.StatusOK, `{
				"State":{"Status":"running"},
				"Config":{"Labels":`+string(exactJSON)+`},
				"NetworkSettings":{"Networks":{"minisky-net":{"IPAddress":"172.18.0.2"}}}
			}`), nil
		})},
	}
	if mappings := manager.GetComputeInstancePortMappings(identity); len(mappings) != 1 {
		t.Fatalf("exact mappings=%#v", mappings)
	}
	if ip := manager.GetComputeInstanceIP(identity); ip != "172.18.0.2" {
		t.Fatalf("exact IP=%q", ip)
	}

	manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return dockerResponse(http.StatusOK, `{
			"State":{"Status":"running"},
			"Config":{"Labels":{"managed-by":"minisky","minisky.profile":"compute-identity"}},
			"NetworkSettings":{"Networks":{"minisky-net":{"IPAddress":"172.18.0.9"}}}
		}`), nil
	})}
	if mappings := manager.GetComputeInstancePortMappings(identity); len(mappings) != 0 {
		t.Fatalf("legacy mappings exposed=%#v", mappings)
	}
	if ip := manager.GetComputeInstanceIP(identity); ip != "" {
		t.Fatalf("legacy IP exposed=%q", ip)
	}
}

func TestDeleteVPCNetworkIPAMTargetsInspectedImmutableID(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	labels, _ := identity.labels()
	labelJSON, _ := json.Marshal(labels)
	var deletePath string
	manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			return dockerResponse(http.StatusOK, `{"Id":"immutable/id","Driver":"bridge","Labels":`+
				string(labelJSON)+`,"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`), nil
		}
		deletePath = request.URL.EscapedPath()
		return dockerResponse(http.StatusNoContent, ``), nil
	})
	if err := manager.DeleteVPCNetworkIPAM(context.Background(), identity, "10.42.0.0/24"); err != nil {
		t.Fatal(err)
	}
	if deletePath != "/networks/immutable%2Fid" {
		t.Fatalf("delete path=%q", deletePath)
	}
}

func TestEnsureVPCNetworkIPAMAmbiguousCreateReinspects(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	labels, _ := identity.labels()
	labelJSON, _ := json.Marshal(labels)
	for _, test := range []struct {
		name       string
		createBody string
		createErr  error
		inspect    string
		wantError  bool
	}{
		{
			name: "transport error after creation", createErr: errors.New("connection reset"),
			inspect: `{"Id":"created","Driver":"bridge","Labels":` + string(labelJSON) +
				`,"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`,
		},
		{
			name: "malformed created response", createBody: `{`,
			inspect: `{"Id":"created","Driver":"bridge","Labels":` + string(labelJSON) +
				`,"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`,
		},
		{
			name: "unowned ambiguous result", createErr: errors.New("timeout"),
			inspect:   `{"Id":"other","Driver":"bridge","Labels":{"managed-by":"other"},"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`,
			wantError: true,
		},
		{
			name: "mismatched ambiguous result", createBody: ``,
			inspect: `{"Id":"other","Driver":"bridge","Labels":` + string(labelJSON) +
				`,"IPAM":{"Config":[{"Subnet":"10.43.0.0/24"}]}}`,
			wantError: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			inspectCount := 0
			manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
				switch {
				case request.Method == http.MethodGet && request.URL.Path != "/networks":
					inspectCount++
					if inspectCount == 1 {
						return dockerResponse(http.StatusNotFound, `{}`), nil
					}
					return dockerResponse(http.StatusOK, test.inspect), nil
				case request.Method == http.MethodGet:
					return dockerResponse(http.StatusOK, `[]`), nil
				case request.Method == http.MethodPost:
					if test.createErr != nil {
						return nil, test.createErr
					}
					return dockerResponse(http.StatusCreated, test.createBody), nil
				default:
					t.Fatalf("unexpected request %s %s", request.Method, request.URL)
					return nil, nil
				}
			})
			state, err := manager.EnsureVPCNetworkIPAM(context.Background(), identity, "10.42.0.0/24")
			if test.wantError {
				if err == nil {
					t.Fatal("ambiguous unowned create succeeded")
				}
			} else if err != nil || state.Created || !strings.Contains(state.ID, "created") {
				t.Fatalf("state=%#v error=%v", state, err)
			}
		})
	}
}
