package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestAmbiguousCreateReinspectSurvivesCallerCancellation(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	labels, _ := identity.labels()
	labelJSON, _ := json.Marshal(labels)
	ctx, cancel := context.WithCancel(context.Background())
	inspectCalls := 0
	manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path != "/networks":
			inspectCalls++
			if inspectCalls == 1 {
				return dockerResponse(http.StatusNotFound, `{}`), nil
			}
			if request.Context().Err() != nil {
				t.Fatalf("ambiguous create reinspection reused canceled context")
			}
			return dockerResponse(http.StatusOK, `{"Id":"created","Driver":"bridge","Labels":`+
				string(labelJSON)+`,"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`), nil
		case request.Method == http.MethodGet:
			return dockerResponse(http.StatusOK, `[]`), nil
		case request.Method == http.MethodPost:
			cancel()
			return nil, context.Canceled
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})
	state, err := manager.EnsureVPCNetworkIPAM(ctx, identity, "10.42.0.0/24")
	if err != nil || state.ID != "created" || state.Created {
		t.Fatalf("state=%#v error=%v", state, err)
	}
}

func TestAmbiguousCanceledCreateDoesNotAdoptUnownedBridge(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "ipam-test")
	identity := VPCNetworkIdentity{Project: "project-a", Network: "private"}
	ctx, cancel := context.WithCancel(context.Background())
	inspectCalls := 0
	manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path != "/networks":
			inspectCalls++
			if inspectCalls == 1 {
				return dockerResponse(http.StatusNotFound, `{}`), nil
			}
			return dockerResponse(http.StatusOK,
				`{"Id":"other","Driver":"bridge","Labels":{"managed-by":"other"},"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`), nil
		case request.Method == http.MethodGet:
			return dockerResponse(http.StatusOK, `[]`), nil
		case request.Method == http.MethodPost:
			cancel()
			return nil, context.Canceled
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})
	if _, err := manager.EnsureVPCNetworkIPAM(ctx, identity, "10.42.0.0/24"); err == nil {
		t.Fatal("canceled ambiguous create adopted an unowned bridge")
	}
}

func TestDeleteLegacyComputeVMRequiresExactCurrentProfileOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "legacy-test")
	const instance = "shared"
	const legacyName = "minisky-vm-" + instance
	exact := ownedDockerLabels()
	exact["minisky.service"] = "compute-instance"
	exact["minisky.resource"] = legacyName

	for _, test := range []struct {
		name      string
		labels    map[string]string
		wantError bool
	}{
		{name: "exact", labels: exact},
		{name: "wrong profile", labels: map[string]string{
			"managed-by": "minisky", "minisky.profile": "other",
			"minisky.service": "compute-instance", "minisky.resource": legacyName,
		}, wantError: true},
		{name: "unowned", labels: map[string]string{"managed-by": "someone-else"}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, _ := json.Marshal(test.labels)
			deleted := false
			manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
				func(request *http.Request) (*http.Response, error) {
					if request.Method == http.MethodGet {
						return dockerResponse(http.StatusOK, `{"State":{"Status":"running"},"Config":{"Labels":`+
							string(encoded)+`}}`), nil
					}
					if !strings.Contains(request.URL.Path, legacyName) {
						t.Fatalf("delete path=%q", request.URL.Path)
					}
					deleted = true
					return dockerResponse(http.StatusNoContent, ``), nil
				},
			)}}
			err := manager.DeleteLegacyComputeVM(instance)
			if test.wantError {
				if err == nil || deleted {
					t.Fatalf("error=%v deleted=%t", err, deleted)
				}
			} else if err != nil || !deleted {
				t.Fatalf("error=%v deleted=%t", err, deleted)
			}
		})
	}
}

func TestDeleteLegacyComputeVMRepeatedCleanupIsIdempotent(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "legacy-test")
	const instance = "shared"
	const legacyName = "minisky-vm-" + instance
	labels := ownedDockerLabels()
	labels["minisky.service"] = "compute-instance"
	labels["minisky.resource"] = legacyName
	encoded, _ := json.Marshal(labels)
	exists := true
	deletes := 0
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet {
				if !exists {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				return dockerResponse(http.StatusOK, `{"State":{"Status":"running"},"Config":{"Labels":`+
					string(encoded)+`}}`), nil
			}
			if request.Method == http.MethodDelete {
				exists = false
				deletes++
			}
			return dockerResponse(http.StatusNoContent, ``), nil
		},
	)}}
	if err := manager.DeleteLegacyComputeVM(instance); err != nil {
		t.Fatal(err)
	}
	if err := manager.DeleteLegacyComputeVM(instance); err != nil {
		t.Fatal(err)
	}
	if deletes != 1 {
		t.Fatalf("legacy deletes=%d", deletes)
	}
}

func TestApplyFirewallUsesCanonicalRuleKeyAndScopedDockerVPC(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "firewall-test")
	identity := ComputeInstanceIdentity{Project: "project-a", Zone: "us-central1-a", Instance: "vm"}
	containerName, _ := identity.DockerName()
	labels, _ := identity.labels()
	labelJSON, _ := json.Marshal(labels)
	const firewallKey = "https://www.googleapis.com/compute/v1/projects/project-a/global/networks/shared"
	dockerVPC, _ := (VPCNetworkIdentity{Project: "project-a", Network: "shared"}).DockerName()
	containerGets := 0
	var created map[string]any
	manager := &ServiceManager{
		fwRules: map[string][]FirewallEntry{
			firewallKey: {{Name: "allow-app", Direction: "INGRESS", Action: "allow", Ports: []string{"8080"}}},
		},
		portRegistry: map[string][]PortMapping{},
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && strings.Contains(request.URL.Path, "/containers/"):
				containerGets++
				switch containerGets {
				case 1:
					return dockerResponse(http.StatusOK, `{"State":{"Status":"running"},"Config":{"Labels":`+
						string(labelJSON)+`}}`), nil
				case 2:
					return dockerResponse(http.StatusNotFound, `{}`), nil
				default:
					return dockerResponse(http.StatusOK,
						`{"NetworkSettings":{"Ports":{"8080/tcp":[{"HostPort":"3210"}]}}}`), nil
				}
			case request.Method == http.MethodDelete:
				return dockerResponse(http.StatusNoContent, ``), nil
			case request.Method == http.MethodGet && strings.Contains(request.URL.Path, "/images/"):
				return dockerResponse(http.StatusOK, `{}`), nil
			case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/stop"):
				return dockerResponse(http.StatusNoContent, ``), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				if err := json.NewDecoder(request.Body).Decode(&created); err != nil {
					t.Fatal(err)
				}
				return dockerResponse(http.StatusCreated, `{}`), nil
			case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/start"):
				return dockerResponse(http.StatusNoContent, ``), nil
			default:
				t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
				return nil, nil
			}
		})},
	}
	if err := manager.ApplyFirewallPortsToComputeInstances(
		firewallKey, dockerVPC, []ComputeInstanceIdentity{identity}, []string{"ubuntu:latest"},
	); err != nil {
		t.Fatal(err)
	}
	host := created["HostConfig"].(map[string]any)
	if host["NetworkMode"] != dockerVPC {
		t.Fatalf("network mode=%v", host["NetworkMode"])
	}
	bindings := host["PortBindings"].(map[string]any)
	if _, ok := bindings["8080/tcp"]; !ok {
		t.Fatalf("firewall ports not preserved in create payload: %#v", created)
	}
	if mappings := manager.portRegistry[containerName]; len(mappings) != 1 ||
		mappings[0].ContainerPort != "8080" {
		t.Fatalf("port registry=%#v", mappings)
	}
}
