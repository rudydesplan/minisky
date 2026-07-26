package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestProvisionComputeInstanceOnVPCUsesExactOwnedBridgeID(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "instance-vpc")
	instance := ComputeInstanceIdentity{Project: "project-a", Zone: "us-central1-a", Instance: "web"}
	vpc := VPCNetworkIdentity{Project: "project-a", Network: "custom"}
	instanceLabels, _ := instance.labels()
	instanceLabels["org.opencontainers.image.title"] = "nginx"
	networkLabels, _ := vpc.labels()
	instanceJSON, _ := json.Marshal(instanceLabels)
	networkJSON, _ := json.Marshal(networkLabels)
	containerName, _ := instance.DockerName()
	bridgeName, _ := vpc.DockerName()
	created := false
	var createPayload map[string]any

	manager := &ServiceManager{
		portRegistry:  map[string][]PortMapping{},
		dockerTimeout: 0,
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/networks/"+bridgeName:
				return dockerResponse(http.StatusOK, `{"Name":"`+bridgeName+`","Id":"bridge/id","Driver":"bridge","Labels":`+
					string(networkJSON)+`,"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/containers/"):
				if !created {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				return dockerResponse(http.StatusOK, `{
					"Id":"container/id",
					"State":{"Status":"running"},
					"Config":{"Labels":`+string(instanceJSON)+`},
					"NetworkSettings":{
						"Ports":{},
						"Networks":{"`+bridgeName+`":{"NetworkID":"bridge/id","IPAddress":"10.42.0.2"}}
					}
				}`), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
				return dockerResponse(http.StatusOK, `{}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				if request.URL.Query().Get("name") != containerName {
					t.Fatalf("container name=%q", request.URL.Query().Get("name"))
				}
				if err := json.NewDecoder(request.Body).Decode(&createPayload); err != nil {
					t.Fatal(err)
				}
				created = true
				return dockerResponse(http.StatusCreated, `{"Id":"container/id"}`), nil
			case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/start"):
				return dockerResponse(http.StatusNoContent, ``), nil
			default:
				t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
				return nil, nil
			}
		})},
	}

	runtime, err := manager.ProvisionComputeInstanceOnVPC(
		context.Background(),
		instance,
		"nginx:1.27-alpine",
		ComputeInstanceNetwork{VPC: vpc, CIDR: "10.42.0.0/24"},
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	hostConfig := createPayload["HostConfig"].(map[string]any)
	if hostConfig["NetworkMode"] != "bridge/id" {
		t.Fatalf("network mode=%v, want immutable bridge ID", hostConfig["NetworkMode"])
	}
	if runtime.NetworkName != bridgeName || runtime.NetworkID != "bridge/id" ||
		runtime.IPAddress != "10.42.0.2" || runtime.ContainerID != "container/id" {
		t.Fatalf("runtime=%#v", runtime)
	}
}

func TestReconcileComputeInstanceOnVPCRefusesWrongAttachment(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "instance-vpc")
	instance := ComputeInstanceIdentity{Project: "project-a", Zone: "us-central1-a", Instance: "web"}
	vpc := VPCNetworkIdentity{Project: "project-a", Network: "custom"}
	instanceLabels, _ := instance.labels()
	instanceLabels["org.opencontainers.image.title"] = "nginx"
	networkLabels, _ := vpc.labels()
	instanceJSON, _ := json.Marshal(instanceLabels)
	networkJSON, _ := json.Marshal(networkLabels)
	bridgeName, _ := vpc.DockerName()

	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(request *http.Request) (*http.Response, error) {
			if strings.HasPrefix(request.URL.Path, "/networks/") {
				return dockerResponse(http.StatusOK, `{"Name":"`+bridgeName+`","Id":"bridge/id","Driver":"bridge","Labels":`+
					string(networkJSON)+`,"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`), nil
			}
			return dockerResponse(http.StatusOK, `{
				"Id":"container/id",
				"State":{"Status":"running"},
				"Config":{"Labels":`+string(instanceJSON)+`},
				"NetworkSettings":{"Networks":{"minisky-net":{"NetworkID":"shared","IPAddress":"172.18.0.2"}}}
			}`), nil
		},
	)}}
	if _, found, err := manager.ReconcileComputeInstanceOnVPC(
		context.Background(),
		instance,
		ComputeInstanceNetwork{VPC: vpc, CIDR: "10.42.0.0/24"},
	); err == nil || !found {
		t.Fatalf("found=%t error=%v", found, err)
	}
}

func TestProvisionComputeInstanceOnVPCCleansOwnedContainerAfterStartFailure(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "instance-vpc")
	instance := ComputeInstanceIdentity{Project: "project-a", Zone: "us-central1-a", Instance: "web"}
	vpc := VPCNetworkIdentity{Project: "project-a", Network: "custom"}
	instanceLabels, _ := instance.labels()
	networkLabels, _ := vpc.labels()
	instanceJSON, _ := json.Marshal(instanceLabels)
	networkJSON, _ := json.Marshal(networkLabels)
	bridgeName, _ := vpc.DockerName()
	created := false
	deleted := false

	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/networks/"):
				return dockerResponse(http.StatusOK, `{"Name":"`+bridgeName+`","Id":"bridge/id","Driver":"bridge","Labels":`+
					string(networkJSON)+`,"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/containers/"):
				if !created {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				return dockerResponse(http.StatusOK, `{"State":{"Status":"created"},"Config":{"Labels":`+
					string(instanceJSON)+`}}`), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
				return dockerResponse(http.StatusOK, `{}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				created = true
				return dockerResponse(http.StatusCreated, `{"Id":"container/id"}`), nil
			case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/start"):
				return dockerResponse(http.StatusInternalServerError, `{"message":"start failed"}`), nil
			case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/stop"):
				return dockerResponse(http.StatusNotModified, ``), nil
			case request.Method == http.MethodDelete && strings.HasPrefix(request.URL.Path, "/containers/"):
				deleted = true
				return dockerResponse(http.StatusNoContent, ``), nil
			default:
				t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
				return nil, nil
			}
		},
	)}}
	if _, err := manager.ProvisionComputeInstanceOnVPC(
		context.Background(), instance, "nginx:1.27-alpine",
		ComputeInstanceNetwork{VPC: vpc, CIDR: "10.42.0.0/24"}, nil, nil, nil,
	); err == nil || !deleted {
		t.Fatalf("error=%v deleted=%t", err, deleted)
	}
}

func TestProvisionComputeInstanceOnVPCCleansOwnedContainerAfterAmbiguousCreateTransportError(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "instance-vpc")
	instance := ComputeInstanceIdentity{Project: "project-a", Zone: "us-central1-a", Instance: "web"}
	vpc := VPCNetworkIdentity{Project: "project-a", Network: "custom"}
	instanceLabels, _ := instance.labels()
	networkLabels, _ := vpc.labels()
	instanceJSON, _ := json.Marshal(instanceLabels)
	networkJSON, _ := json.Marshal(networkLabels)
	bridgeName, _ := vpc.DockerName()
	created := false
	deleted := false

	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/networks/"):
				return dockerResponse(http.StatusOK, `{"Name":"`+bridgeName+`","Id":"bridge/id","Driver":"bridge","Labels":`+
					string(networkJSON)+`,"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/containers/"):
				if !created {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				return dockerResponse(http.StatusOK, `{
					"Id":"container/id",
					"State":{"Status":"created"},
					"Config":{"Labels":`+string(instanceJSON)+`},
					"NetworkSettings":{"Networks":{"`+bridgeName+`":{"NetworkID":"bridge/id","IPAddress":"10.42.0.2"}}}
				}`), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
				return dockerResponse(http.StatusOK, `{}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				created = true
				return nil, errors.New("connection reset after Docker accepted create")
			case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/stop"):
				return dockerResponse(http.StatusNotModified, ``), nil
			case request.Method == http.MethodDelete && strings.HasPrefix(request.URL.Path, "/containers/"):
				deleted = true
				return dockerResponse(http.StatusNoContent, ``), nil
			default:
				t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
				return nil, nil
			}
		},
	)}}
	if _, err := manager.ProvisionComputeInstanceOnVPC(
		context.Background(), instance, "nginx:1.27-alpine",
		ComputeInstanceNetwork{VPC: vpc, CIDR: "10.42.0.0/24"}, nil, nil, nil,
	); err == nil || !deleted {
		t.Fatalf("error=%v deleted=%t", err, deleted)
	}
}

func TestProvisionComputeInstanceOnVPCDoesNotCleanForeignContainerAfterAmbiguousCreate(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "instance-vpc")
	instance := ComputeInstanceIdentity{Project: "project-a", Zone: "us-central1-a", Instance: "web"}
	foreign := ComputeInstanceIdentity{Project: "project-b", Zone: "us-central1-a", Instance: "web"}
	vpc := VPCNetworkIdentity{Project: "project-a", Network: "custom"}
	foreignLabels, _ := foreign.labels()
	networkLabels, _ := vpc.labels()
	foreignJSON, _ := json.Marshal(foreignLabels)
	networkJSON, _ := json.Marshal(networkLabels)
	bridgeName, _ := vpc.DockerName()
	created := false
	deleteCalls := 0

	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/networks/"):
				return dockerResponse(http.StatusOK, `{"Name":"`+bridgeName+`","Id":"bridge/id","Driver":"bridge","Labels":`+
					string(networkJSON)+`,"IPAM":{"Config":[{"Subnet":"10.42.0.0/24"}]}}`), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/containers/"):
				if !created {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				return dockerResponse(http.StatusOK, `{
					"Id":"foreign/container",
					"State":{"Status":"created"},
					"Config":{"Labels":`+string(foreignJSON)+`}
				}`), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
				return dockerResponse(http.StatusOK, `{}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				created = true
				return nil, errors.New("connection reset after ambiguous create")
			case request.Method == http.MethodDelete:
				deleteCalls++
				return dockerResponse(http.StatusNoContent, ``), nil
			default:
				t.Fatalf("unexpected Docker request %s %s", request.Method, request.URL)
				return nil, nil
			}
		},
	)}}
	if _, err := manager.ProvisionComputeInstanceOnVPC(
		context.Background(), instance, "nginx:1.27-alpine",
		ComputeInstanceNetwork{VPC: vpc, CIDR: "10.42.0.0/24"}, nil, nil, nil,
	); err == nil || !strings.Contains(err.Error(), "refusing to remove") {
		t.Fatalf("error=%v", err)
	}
	if deleteCalls != 0 {
		t.Fatalf("foreign container delete calls=%d", deleteCalls)
	}
}

func TestDeleteComputeInstanceHonorsContextDeadline(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "instance-vpc")
	instance := ComputeInstanceIdentity{Project: "project-a", Zone: "us-central1-a", Instance: "web"}
	labels, _ := instance.labels()
	labelsJSON, _ := json.Marshal(labels)
	containerName, _ := instance.DockerName()
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet &&
				request.URL.Path == "/containers/"+containerName+"/json" {
				return dockerResponse(http.StatusOK, `{"State":{"Status":"running"},"Config":{"Labels":`+
					string(labelsJSON)+`}}`), nil
			}
			<-request.Context().Done()
			return nil, request.Context().Err()
		},
	)}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := manager.DeleteComputeInstance(ctx, instance)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded delete took %s", elapsed)
	}
}
