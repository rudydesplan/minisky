package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProvisionMemcachedCreatesExactOwnedLoopbackBackend(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "memcached-create")
	const resourceID = testMemcacheBackendID
	name := memcachedDockerName(resourceID)
	image, err := memcacheImageForVersion("MEMCACHE_1_5")
	if err != nil {
		t.Fatal(err)
	}
	labels := memcachedLabels(resourceID)
	var created, started bool
	var immutableInspects int

	manager := &ServiceManager{
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json":
				return dockerResponse(http.StatusNotFound, `{}`), nil
			case request.Method == http.MethodGet && request.URL.Path == "/images/"+image+"/json":
				return dockerResponse(http.StatusOK, `{}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				if got := request.URL.Query().Get("name"); got != name {
					t.Fatalf("create name = %q, want %q", got, name)
				}
				var payload struct {
					Image        string                     `json:"Image"`
					Labels       map[string]string          `json:"Labels"`
					ExposedPorts map[string]json.RawMessage `json:"ExposedPorts"`
					HostConfig   struct {
						PortBindings map[string][]struct {
							HostIP   string `json:"HostIp"`
							HostPort string `json:"HostPort"`
						} `json:"PortBindings"`
						Binds []string `json:"Binds"`
					} `json:"HostConfig"`
				}
				if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				if payload.Image != image {
					t.Fatalf("image = %q, want configured %q", payload.Image, image)
				}
				if !exactLabels(payload.Labels, labels) {
					t.Fatalf("labels = %#v, want exact %#v", payload.Labels, labels)
				}
				if _, ok := payload.ExposedPorts[memcachedContainerPort]; !ok {
					t.Fatalf("exposed ports = %#v", payload.ExposedPorts)
				}
				bindings := payload.HostConfig.PortBindings[memcachedContainerPort]
				if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" ||
					bindings[0].HostPort != "0" {
					t.Fatalf("port bindings = %#v", bindings)
				}
				if len(payload.HostConfig.Binds) != 0 {
					t.Fatalf("Memcached unexpectedly requested durable storage: %#v", payload.HostConfig.Binds)
				}
				created = true
				return dockerResponse(http.StatusCreated, `{"Id":"`+testMemcacheContainerID+`"}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/"+testMemcacheContainerID+"/start":
				started = true
				return dockerResponse(http.StatusNoContent, ``), nil
			case request.Method == http.MethodGet && request.URL.Path == "/containers/"+testMemcacheContainerID+"/json":
				immutableInspects++
				if immutableInspects == 1 {
					return memcachedInspectResponse(http.StatusOK, testMemcacheContainerID, "created", labels, "127.0.0.1", "40123"), nil
				}
				return memcachedInspectResponse(http.StatusOK, testMemcacheContainerID, "running", labels, "127.0.0.1", "40123"), nil
			default:
				return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
			}
		})},
		memcachedReady: func(context.Context, string, string, time.Duration) error { return nil },
	}

	endpoint, err := provisionMemcacheForTest(manager, context.Background(), resourceID)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "127.0.0.1:40123" {
		t.Fatalf("endpoint = %q", endpoint)
	}
	if !created || !started {
		t.Fatalf("created=%v started=%v", created, started)
	}
}

func TestReconcileMemcachedRestartsOnlyExactOwnedStoppedContainer(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "memcached-reconcile")
	const resourceID = testMemcacheBackendID
	name := memcachedDockerName(resourceID)
	labels := memcachedLabels(resourceID)
	var starts int
	manager := &ServiceManager{
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json":
				return memcachedInspectResponse(http.StatusOK, testMemcacheContainerID, "exited", labels, "127.0.0.1", "40124"), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/"+testMemcacheContainerID+"/start":
				starts++
				return dockerResponse(http.StatusNoContent, ``), nil
			case request.Method == http.MethodGet && request.URL.Path == "/containers/"+testMemcacheContainerID+"/json":
				return memcachedInspectResponse(http.StatusOK, testMemcacheContainerID, "running", labels, "127.0.0.1", "40124"), nil
			default:
				return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
			}
		})},
		memcachedReady: func(context.Context, string, string, time.Duration) error { return nil },
	}

	endpoint, found, err := reconcileMemcacheForTest(
		manager, context.Background(), resourceID, "MEMCACHE_1_5",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found || endpoint != "127.0.0.1:40124" || starts != 1 {
		t.Fatalf("endpoint=%q found=%v starts=%d", endpoint, found, starts)
	}
}

func TestReconcileMemcachedReportsMissingBackendWithoutCreating(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "memcached-missing")
	const resourceID = testMemcacheBackendID
	var mutations int
	manager := &ServiceManager{
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodGet {
				mutations++
			}
			return dockerResponse(http.StatusNotFound, `{}`), nil
		})},
		memcachedReady: func(context.Context, string, string, time.Duration) error { return nil },
	}

	endpoint, found, err := reconcileMemcacheForTest(
		manager, context.Background(), resourceID, "MEMCACHE_1_5",
	)
	if err != nil {
		t.Fatal(err)
	}
	if found || endpoint != "" || mutations != 0 {
		t.Fatalf("endpoint=%q found=%v mutations=%d", endpoint, found, mutations)
	}
}

func TestMemcachedLifecycleRefusesForeignOwnership(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "memcached-foreign")
	const resourceID = testMemcacheBackendID
	name := memcachedDockerName(resourceID)
	foreign := memcachedLabels(resourceID)
	foreign["minisky.profile"] = "other-profile"

	for _, test := range []struct {
		name string
		call func(*ServiceManager) error
	}{
		{name: "provision", call: func(manager *ServiceManager) error {
			_, err := provisionMemcacheForTest(manager, context.Background(), resourceID)
			return err
		}},
		{name: "reconcile", call: func(manager *ServiceManager) error {
			_, _, err := reconcileMemcacheForTest(
				manager, context.Background(), resourceID, "MEMCACHE_1_5",
			)
			return err
		}},
		{name: "delete", call: func(manager *ServiceManager) error {
			return manager.DeleteMemcache(context.Background(), resourceID)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
				func(request *http.Request) (*http.Response, error) {
					if request.Method != http.MethodGet || request.URL.Path != "/containers/"+name+"/json" {
						t.Fatalf("foreign collision reached mutation %s %s", request.Method, request.URL)
					}
					return memcachedInspectResponse(http.StatusOK, testOtherContainerID, "exited", foreign, "127.0.0.1", "40125"), nil
				},
			)}}
			err := test.call(manager)
			if !errors.Is(err, ErrDockerOwnershipConflict) {
				t.Fatalf("error = %v, want ownership conflict", err)
			}
		})
	}
}

func TestProvisionMemcachedRefusesForeignNameReuseRace(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "memcached-race")
	const resourceID = testMemcacheBackendID
	name := memcachedDockerName(resourceID)
	image, err := memcacheImageForVersion("MEMCACHE_1_5")
	if err != nil {
		t.Fatal(err)
	}
	foreign := memcachedLabels(resourceID)
	foreign["managed-by"] = "attacker"
	var nameInspects int

	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json":
				nameInspects++
				if nameInspects == 1 {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				return memcachedInspectResponse(http.StatusOK, testOtherContainerID, "running", foreign, "127.0.0.1", "40126"), nil
			case request.Method == http.MethodGet && request.URL.Path == "/images/"+image+"/json":
				return dockerResponse(http.StatusOK, `{}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				return dockerResponse(http.StatusConflict, `{"message":"name already in use"}`), nil
			default:
				t.Fatalf("name reuse race reached unsafe request %s %s", request.Method, request.URL)
				return nil, nil
			}
		},
	)}}

	_, err = provisionMemcacheForTest(manager, context.Background(), resourceID)
	if !errors.Is(err, ErrDockerOwnershipConflict) {
		t.Fatalf("error = %v, want ownership conflict", err)
	}
}

func TestDeleteMemcachedUsesInspectedImmutableIDAndIsIdempotent(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "memcached-delete")
	const resourceID = testMemcacheBackendID
	name := memcachedDockerName(resourceID)
	labels := memcachedLabels(resourceID)
	var deletes int
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json":
				return memcachedInspectResponse(http.StatusOK, testMemcacheContainerID, "running", labels, "127.0.0.1", "40127"), nil
			case request.Method == http.MethodDelete && request.URL.Path == "/containers/"+testMemcacheContainerID:
				deletes++
				return dockerResponse(http.StatusNotFound, `{}`), nil
			default:
				return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
			}
		},
	)}}
	if err := manager.DeleteMemcache(context.Background(), resourceID); err != nil {
		t.Fatal(err)
	}
	if deletes != 1 {
		t.Fatalf("deletes = %d", deletes)
	}

	missing := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(*http.Request) (*http.Response, error) {
			return dockerResponse(http.StatusNotFound, `{}`), nil
		},
	)}}
	if err := missing.DeleteMemcache(context.Background(), resourceID); err != nil {
		t.Fatalf("delete absent backend: %v", err)
	}
}

func TestMemcachedLifecycleDistinguishesDockerFailureFromAbsence(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "memcached-daemon-error")
	const resourceID = testMemcacheBackendID
	daemonErr := errors.New("permission denied connecting to Docker")
	for _, test := range []struct {
		name string
		call func(*ServiceManager) error
	}{
		{name: "provision", call: func(manager *ServiceManager) error {
			_, err := provisionMemcacheForTest(manager, context.Background(), resourceID)
			return err
		}},
		{name: "reconcile", call: func(manager *ServiceManager) error {
			_, _, err := reconcileMemcacheForTest(
				manager, context.Background(), resourceID, "MEMCACHE_1_5",
			)
			return err
		}},
		{name: "delete", call: func(manager *ServiceManager) error {
			return manager.DeleteMemcache(context.Background(), resourceID)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
				func(*http.Request) (*http.Response, error) { return nil, daemonErr },
			)}}
			err := test.call(manager)
			if err == nil || !strings.Contains(err.Error(), daemonErr.Error()) {
				t.Fatalf("error = %v, want Docker failure", err)
			}
		})
	}
}

func TestMemcachedLifecyclePropagatesContextCancellation(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "memcached-context")
	const resourceID = testMemcacheBackendID
	for _, test := range []struct {
		name string
		call func(context.Context, *ServiceManager) error
	}{
		{name: "provision", call: func(ctx context.Context, manager *ServiceManager) error {
			_, err := provisionMemcacheForTest(manager, ctx, resourceID)
			return err
		}},
		{name: "reconcile", call: func(ctx context.Context, manager *ServiceManager) error {
			_, _, err := reconcileMemcacheForTest(manager, ctx, resourceID, "MEMCACHE_1_5")
			return err
		}},
		{name: "delete", call: func(ctx context.Context, manager *ServiceManager) error {
			return manager.DeleteMemcache(ctx, resourceID)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			reached := make(chan struct{}, 1)
			manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
				func(request *http.Request) (*http.Response, error) {
					reached <- struct{}{}
					<-request.Context().Done()
					return nil, request.Context().Err()
				},
			)}}
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- test.call(ctx, manager) }()
			<-reached
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v, want context cancellation", err)
				}
			case <-time.After(time.Second):
				t.Fatal("lifecycle did not stop after context cancellation")
			}
		})
	}
}

func TestMemcachedEndpointValidatesLoopbackPortBinding(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		hostIP   string
		hostPort string
		want     string
		wantErr  bool
	}{
		{name: "IPv4 loopback", hostIP: "127.0.0.1", hostPort: "11212", want: "127.0.0.1:11212"},
		{name: "IPv6 loopback", hostIP: "::1", hostPort: "11213", want: "[::1]:11213"},
		{name: "non-loopback", hostIP: "0.0.0.0", hostPort: "11214", wantErr: true},
		{name: "not numeric", hostIP: "127.0.0.1", hostPort: "abc", wantErr: true},
		{name: "zero", hostIP: "127.0.0.1", hostPort: "0", wantErr: true},
		{name: "out of range", hostIP: "127.0.0.1", hostPort: "65536", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			container := memcachedContainerInspect{ID: "id"}
			container.NetworkSettings.Ports = map[string][]dockerPortBinding{
				memcachedContainerPort: {{HostIP: test.hostIP, HostPort: test.hostPort}},
			}
			got, err := memcachedEndpoint(container)
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("endpoint = %q, want %q", got, test.want)
			}
		})
	}
}

func TestWaitUntilMemcachedReadyRetriesMalformedProtocolResponse(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var attempts atomic.Int32
	go func() {
		for attempts.Load() < 2 {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			attempt := attempts.Add(1)
			request := make([]byte, len("version\r\n"))
			_, readErr := io.ReadFull(connection, request)
			if readErr == nil && string(request) == "version\r\n" {
				if attempt == 1 {
					_, _ = connection.Write([]byte("NOT_VERSION\r\n"))
				} else {
					_, _ = connection.Write([]byte("VERSION 1.6.33\r\n"))
				}
			}
			_ = connection.Close()
		}
	}()
	if err := waitUntilMemcachedReady(
		context.Background(), listener.Addr().String(), "1.6.33", time.Second,
	); err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestWaitUntilMemcachedReadyHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := waitUntilMemcachedReady(ctx, "127.0.0.1:1", "1.5.16", time.Minute)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
}

func TestWaitUntilMemcachedReadyRejectsVersionMismatchAndExtraTokens(t *testing.T) {
	for _, response := range []string{
		"VERSION 1.6.15\r\n",
		"VERSION 1.5.16 extra\r\n",
		"VERSION\r\n",
		"VERSION 1.5.16\n",
		"VERSION " + strings.Repeat("1", 600) + "\r\n",
	} {
		t.Run(strings.TrimSpace(response), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					connection, acceptErr := listener.Accept()
					if acceptErr != nil {
						return
					}
					request := make([]byte, len("version\r\n"))
					_, _ = io.ReadFull(connection, request)
					_, _ = connection.Write([]byte(response))
					_ = connection.Close()
				}
			}()
			err = waitUntilMemcachedReady(
				context.Background(), listener.Addr().String(), "1.5.16", 150*time.Millisecond,
			)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("response %q error = %v, want deadline", response, err)
			}
			_ = listener.Close()
			<-done
		})
	}
}

func TestCleanupAllProfilesDiscoversCanonicalMemcachedLabels(t *testing.T) {
	ownedLabels := map[string]string{
		"managed-by":       "minisky",
		"minisky.profile":  "cleanup-profile",
		"minisky.service":  "memorystore-memcached",
		"minisky.resource": "projects/p/locations/l/instances/cache",
	}
	var deleted []string
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/containers/json":
				body, _ := json.Marshal([]map[string]any{
					{"Id": "memcached-id", "Labels": ownedLabels},
					{"Id": "foreign-id", "Labels": map[string]string{"managed-by": "other", "minisky.profile": "cleanup-profile"}},
				})
				return dockerResponse(http.StatusOK, string(body)), nil
			case request.Method == http.MethodDelete:
				deleted = append(deleted, request.URL.Path)
				return dockerResponse(http.StatusNoContent, ``), nil
			case request.Method == http.MethodGet && request.URL.Path == "/networks":
				return dockerResponse(http.StatusOK, `[]`), nil
			case request.Method == http.MethodGet && request.URL.Path == "/volumes":
				return dockerResponse(http.StatusOK, `{"Volumes":[]}`), nil
			default:
				return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
			}
		},
	)}}
	if err := manager.CleanupAllProfiles(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 1 || deleted[0] != "/containers/memcached-id" {
		t.Fatalf("deleted = %#v", deleted)
	}
}

func provisionMemcacheForTest(
	manager *ServiceManager,
	ctx context.Context,
	resourceID string,
) (string, error) {
	endpoints, _, _, err := manager.ProvisionMemcache(
		ctx, resourceID, 1, 1, 1024, "MEMCACHE_1_5", nil,
	)
	if err != nil || len(endpoints) == 0 {
		return "", err
	}
	return endpoints[0], nil
}

func reconcileMemcacheForTest(
	manager *ServiceManager,
	ctx context.Context,
	resourceID string,
	version string,
) (string, bool, error) {
	endpoints, _, exists, err := manager.ReconcileMemcache(
		ctx, resourceID, 1, 1, 1024, version, nil,
	)
	if err != nil || !exists || len(endpoints) == 0 {
		return "", exists, err
	}
	return endpoints[0], true, nil
}

func memcachedInspectResponse(
	statusCode int,
	id string,
	status string,
	labels map[string]string,
	hostIP string,
	hostPort string,
) *http.Response {
	return memcachedInspectResponseWithImage(
		statusCode,
		id,
		status,
		labels,
		hostIP,
		hostPort,
		"memcached:1.5.16-alpine",
	)
}

func memcachedInspectResponseWithImage(
	statusCode int,
	id string,
	status string,
	labels map[string]string,
	hostIP string,
	hostPort string,
	image string,
) *http.Response {
	body, _ := json.Marshal(map[string]any{
		"Id": id,
		"State": map[string]string{
			"Status": status,
		},
		"Config": map[string]any{
			"Labels": labels,
			"Image":  image,
		},
		"NetworkSettings": map[string]any{
			"Ports": map[string]any{
				memcachedContainerPort: []map[string]string{{
					"HostIp": hostIP, "HostPort": hostPort,
				}},
			},
		},
	})
	return &http.Response{
		StatusCode: statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
}

func memcachedSet(endpoint, key, value string) error {
	connection, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		return err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintf(connection, "set %s 0 0 %d\r\n%s\r\n", key, len(value), value); err != nil {
		return err
	}
	response := make([]byte, len("STORED\r\n"))
	if _, err := io.ReadFull(connection, response); err != nil {
		return err
	}
	if string(response) != "STORED\r\n" {
		return fmt.Errorf("SET response %q", response)
	}
	return nil
}

func memcachedGet(endpoint, key string) (string, bool, error) {
	connection, err := net.DialTimeout("tcp", endpoint, 2*time.Second)
	if err != nil {
		return "", false, err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintf(connection, "get %s\r\n", key); err != nil {
		return "", false, err
	}
	reader := bufio.NewReader(io.LimitReader(connection, 1<<20))
	header, err := reader.ReadString('\n')
	if err != nil {
		return "", false, err
	}
	if header == "END\r\n" {
		return "", false, nil
	}
	fields := strings.Fields(strings.TrimSpace(header))
	if len(fields) != 4 || fields[0] != "VALUE" || fields[1] != key {
		return "", false, fmt.Errorf("GET header %q", header)
	}
	length, err := strconv.Atoi(fields[3])
	if err != nil {
		return "", false, fmt.Errorf("GET value length: %w", err)
	}
	if length < 0 {
		return "", false, fmt.Errorf("GET value length %d is negative", length)
	}
	value := make([]byte, length+2)
	if _, err := io.ReadFull(reader, value); err != nil {
		return "", false, err
	}
	end, err := reader.ReadString('\n')
	if err != nil {
		return "", false, err
	}
	if string(value[length:]) != "\r\n" || end != "END\r\n" {
		return "", false, fmt.Errorf("GET response terminator value=%q end=%q", value, end)
	}
	return string(value[:length]), true, nil
}
