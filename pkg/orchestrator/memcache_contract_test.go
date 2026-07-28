package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testMemcacheBackendID   = "memcache-0123456789abcdef0123456789abcdef"
	testMemcacheContainerID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testOtherContainerID    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type exactMemcacheContract interface {
	ProvisionMemcache(context.Context, string, int, int, int, string, map[string]string) ([]string, bool, bool, error)
	UpdateMemcache(context.Context, string, int, int, int, string, map[string]string) ([]string, bool, bool, error)
	ReconcileMemcache(context.Context, string, int, int, int, string, map[string]string) ([]string, bool, bool, error)
	DeleteMemcache(context.Context, string) error
}

var _ exactMemcacheContract = (*ServiceManager)(nil)

func TestMemcacheContractRejectsInvalidIdentityBeforeDocker(t *testing.T) {
	invalidIDs := []string{
		"",
		"memcache-0123456789ABCDEF0123456789abcdef",
		"memcache-0123456789abcdef0123456789abcde",
		"memcache-0123456789abcdef0123456789abcdef/",
		"../memcache-0123456789abcdef0123456789abcdef",
		"memcache-%2f23456789abcdef0123456789abcdef",
		"memcache-0123456789abcdef0123456789abcde\n",
	}
	for _, id := range invalidIDs {
		t.Run(fmt.Sprintf("id_%q", id), func(t *testing.T) {
			t.Setenv("MINISKY_PROFILE", "valid-profile")
			var calls atomic.Int32
			manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
				func(*http.Request) (*http.Response, error) {
					calls.Add(1)
					return nil, errors.New("Docker must not be called")
				},
			)}}
			if _, _, _, err := manager.ProvisionMemcache(
				context.Background(), id, 1, 1, 1024, "MEMCACHE_1_5", nil,
			); err == nil {
				t.Fatal("invalid backend ID was accepted")
			}
			if calls.Load() != 0 {
				t.Fatalf("Docker calls = %d, want zero", calls.Load())
			}
		})
	}

	for _, profile := range []string{"../escape", "bad/profile", "bad\\profile", ".hidden", "bad\nprofile"} {
		t.Run(fmt.Sprintf("profile_%q", profile), func(t *testing.T) {
			t.Setenv("MINISKY_PROFILE", profile)
			var calls atomic.Int32
			manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
				func(*http.Request) (*http.Response, error) {
					calls.Add(1)
					return nil, errors.New("Docker must not be called")
				},
			)}}
			if _, _, _, err := manager.ReconcileMemcache(
				context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_5", nil,
			); err == nil {
				t.Fatal("invalid profile was accepted")
			}
			if calls.Load() != 0 {
				t.Fatalf("Docker calls = %d, want zero", calls.Load())
			}
		})
	}
}

func TestMemcacheContractRejectsUnsupportedNodeCountBeforeMutation(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "bounded-update")
	var calls atomic.Int32
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return nil, errors.New("Docker must not be called")
		},
	)}}
	if _, _, _, err := manager.ProvisionMemcache(
		context.Background(), testMemcacheBackendID, 2, 1, 1024, "MEMCACHE_1_5", nil,
	); err == nil || !strings.Contains(err.Error(), "one local node") {
		t.Fatalf("provision error = %v", err)
	}
	if _, _, _, err := manager.UpdateMemcache(
		context.Background(), testMemcacheBackendID, 2, 1, 1024, "MEMCACHE_1_5", nil,
	); err == nil || !strings.Contains(err.Error(), "one local node") {
		t.Fatalf("update error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("Docker calls = %d, want zero", calls.Load())
	}
}

func TestUpdateMemcacheReconcilesBoundedSingleNode(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "bounded-update")
	name := memcachedDockerName(testMemcacheBackendID)
	labels := memcachedLabels(testMemcacheBackendID)
	manager := &ServiceManager{
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json" {
				return memcachedInspectResponse(
					http.StatusOK, testMemcacheContainerID, "running", labels, "127.0.0.1", "40128",
				), nil
			}
			return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
		})},
		memcachedReady: func(context.Context, string, string, time.Duration) error { return nil },
	}
	endpoints, owned, exists, err := manager.UpdateMemcache(
		context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_5", nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !owned || !exists || len(endpoints) != 1 || endpoints[0] != "127.0.0.1:40128" {
		t.Fatalf("endpoints=%v owned=%v exists=%v", endpoints, owned, exists)
	}
}

func TestReconcileMemcacheRequiresExpectedImageAndProtocolVersion(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "version-aware-reconcile")
	name := memcachedDockerName(testMemcacheBackendID)
	labels := memcachedLabels(testMemcacheBackendID)
	var readyExpected string
	manager := &ServiceManager{
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json" {
				return memcachedInspectResponseWithImage(
					http.StatusOK, testMemcacheContainerID, "running", labels,
					"127.0.0.1", "40135", "memcached:1.5.16-alpine",
				), nil
			}
			return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
		})},
		memcachedReady: func(_ context.Context, _ string, expected string, _ time.Duration) error {
			readyExpected = expected
			return errors.New("mutable tag serves VERSION 1.6.15")
		},
	}
	_, owned, exists, err := manager.ReconcileMemcache(
		context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_5", nil,
	)
	if err == nil || !owned || !exists {
		t.Fatalf("error=%v owned=%v exists=%v", err, owned, exists)
	}
	if readyExpected != "1.5.16" {
		t.Fatalf("readiness expected version = %q", readyExpected)
	}
}

func TestReconcileMemcacheRejectsWrongConfiguredImageBeforeReadiness(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "version-image-reconcile")
	name := memcachedDockerName(testMemcacheBackendID)
	labels := memcachedLabels(testMemcacheBackendID)
	var readinessCalls atomic.Int32
	manager := &ServiceManager{
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json" {
				return memcachedInspectResponseWithImage(
					http.StatusOK, testMemcacheContainerID, "running", labels,
					"127.0.0.1", "40136", "memcached:1.6.15-alpine",
				), nil
			}
			return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
		})},
		memcachedReady: func(context.Context, string, string, time.Duration) error {
			readinessCalls.Add(1)
			return nil
		},
	}
	_, owned, exists, err := manager.ReconcileMemcache(
		context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_5", nil,
	)
	if err == nil || !strings.Contains(err.Error(), "does not match requested image") ||
		!owned || !exists {
		t.Fatalf("error=%v owned=%v exists=%v", err, owned, exists)
	}
	if readinessCalls.Load() != 0 {
		t.Fatalf("readiness calls = %d", readinessCalls.Load())
	}
}

func TestProvisionMemcacheCompensatesEveryPostCreateFailure(t *testing.T) {
	for _, stage := range []string{
		"inspect",
		"start",
		"endpoint",
		"readiness",
		"cancellation",
	} {
		t.Run(stage, func(t *testing.T) {
			t.Setenv("MINISKY_PROFILE", "compensate-"+stage)
			manager, deleted, cleanupContext := postCreateFailureManager(t, stage, false)
			_, owned, exists, err := manager.ProvisionMemcache(
				context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_5", nil,
			)
			if err == nil {
				t.Fatal("post-create failure returned success")
			}
			if owned || exists {
				t.Fatalf("owned=%v exists=%v after failed compensated create", owned, exists)
			}
			if deleted.Load() != 1 {
				t.Fatalf("immutable-ID deletes = %d, want 1", deleted.Load())
			}
			if stage == "cancellation" {
				select {
				case cleanupWasActive := <-cleanupContext:
					if !cleanupWasActive {
						t.Fatal("cleanup inherited canceled caller context")
					}
				default:
					t.Fatal("cleanup context was not observed")
				}
			}
		})
	}
}

func TestProvisionMemcacheRecoversUncapturedExactOwnedCreate(t *testing.T) {
	for _, stage := range []string{"conflict", "transport", "response-decode", "empty-id", "malformed-id"} {
		t.Run(stage, func(t *testing.T) {
			t.Setenv("MINISKY_PROFILE", "recover-"+stage)
			name := memcachedDockerName(testMemcacheBackendID)
			labels := memcachedLabels(testMemcacheBackendID)
			image, err := memcacheImageForVersion("MEMCACHE_1_5")
			if err != nil {
				t.Fatal(err)
			}
			var nameInspects atomic.Int32
			var deletes atomic.Int32
			manager := &ServiceManager{
				dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					switch {
					case request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json":
						if nameInspects.Add(1) == 1 {
							return dockerResponse(http.StatusNotFound, `{}`), nil
						}
						return memcachedInspectResponseWithImage(
							http.StatusOK, testMemcacheContainerID, "running", labels,
							"127.0.0.1", "40131", image,
						), nil
					case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
						return dockerResponse(http.StatusOK, `{}`), nil
					case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
						switch stage {
						case "conflict":
							return dockerResponse(http.StatusConflict, `{"message":"name in use"}`), nil
						case "transport":
							return nil, errors.New("ambiguous create transport failure")
						case "response-decode":
							return dockerResponse(http.StatusCreated, `{`), nil
						case "empty-id":
							return dockerResponse(http.StatusCreated, `{}`), nil
						default:
							return dockerResponse(http.StatusCreated, `{"Id":"not-an-id"}`), nil
						}
					case request.Method == http.MethodDelete:
						deletes.Add(1)
						return dockerResponse(http.StatusNoContent, ``), nil
					default:
						return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
					}
				})},
				memcachedReady: func(_ context.Context, _ string, expected string, _ time.Duration) error {
					if expected != "1.5.16" {
						return fmt.Errorf("expected protocol version = %q", expected)
					}
					return nil
				},
			}
			endpoints, owned, exists, err := manager.ProvisionMemcache(
				context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_5", nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !owned || !exists || len(endpoints) != 1 || endpoints[0] != "127.0.0.1:40131" {
				t.Fatalf("endpoints=%v owned=%v exists=%v", endpoints, owned, exists)
			}
			if deletes.Load() != 0 {
				t.Fatalf("uncaptured recovery deleted %d containers", deletes.Load())
			}
		})
	}
}

func TestProvisionMemcachePreservesAmbiguousRecoveryUncertainty(t *testing.T) {
	for _, stage := range []string{"daemon", "permission", "canceled"} {
		t.Run(stage, func(t *testing.T) {
			t.Setenv("MINISKY_PROFILE", "uncertain-"+stage)
			name := memcachedDockerName(testMemcacheBackendID)
			var nameInspects atomic.Int32
			var deletes atomic.Int32
			manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
				func(request *http.Request) (*http.Response, error) {
					switch {
					case request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json":
						if nameInspects.Add(1) == 1 {
							return dockerResponse(http.StatusNotFound, `{}`), nil
						}
						switch stage {
						case "daemon":
							return nil, errors.New("Docker daemon unavailable")
						case "permission":
							return dockerResponse(http.StatusForbidden, `permission denied`), nil
						default:
							return nil, context.Canceled
						}
					case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
						return dockerResponse(http.StatusOK, `{}`), nil
					case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
						return nil, errors.New("ambiguous create transport failure")
					case request.Method == http.MethodDelete:
						deletes.Add(1)
						return dockerResponse(http.StatusNoContent, ``), nil
					default:
						return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
					}
				},
			)}}
			_, owned, exists, err := manager.ProvisionMemcache(
				context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_5", nil,
			)
			if err == nil || !owned || !exists {
				t.Fatalf("error=%v owned=%v exists=%v", err, owned, exists)
			}
			inspectsBeforeDelete := nameInspects.Load()
			if deleteErr := manager.DeleteMemcache(context.Background(), testMemcacheBackendID); deleteErr == nil ||
				!strings.Contains(deleteErr.Error(), "uncertain") {
				t.Fatalf("uncertain delete error = %v", deleteErr)
			}
			if nameInspects.Load() != inspectsBeforeDelete || deletes.Load() != 0 {
				t.Fatalf("uncertain cleanup inspected=%d deleted=%d", nameInspects.Load(), deletes.Load())
			}
		})
	}
}

func TestProvisionMemcacheRejectsSameOwnedWrongVersionCandidate(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "recover-wrong-version")
	name := memcachedDockerName(testMemcacheBackendID)
	labels := memcachedLabels(testMemcacheBackendID)
	var nameInspects atomic.Int32
	var deletes atomic.Int32
	manager := &ServiceManager{
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json":
				if nameInspects.Add(1) == 1 {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				return memcachedInspectResponseWithImage(
					http.StatusOK, testMemcacheContainerID, "running", labels,
					"127.0.0.1", "40132", "memcached:1.6.15-alpine",
				), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
				return dockerResponse(http.StatusOK, `{}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				return dockerResponse(http.StatusConflict, `{"message":"name in use"}`), nil
			case request.Method == http.MethodDelete:
				deletes.Add(1)
				return dockerResponse(http.StatusNoContent, ``), nil
			default:
				return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
			}
		})},
	}
	_, owned, exists, err := manager.ProvisionMemcache(
		context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_5", nil,
	)
	if err == nil || !strings.Contains(err.Error(), "does not match requested image") {
		t.Fatalf("error = %v", err)
	}
	if !owned || !exists || deletes.Load() != 0 {
		t.Fatalf("owned=%v exists=%v deletes=%d", owned, exists, deletes.Load())
	}
}

func TestProvisionMemcacheCapturedIDCompensationIgnoresNameReuse(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "captured-name-reuse")
	name := memcachedDockerName(testMemcacheBackendID)
	labels := memcachedLabels(testMemcacheBackendID)
	var nameInspects atomic.Int32
	var idInspects atomic.Int32
	var deletes atomic.Int32
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json":
				if nameInspects.Add(1) == 1 {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				foreign := memcachedLabels(testMemcacheBackendID)
				foreign["managed-by"] = "attacker"
				return memcachedInspectResponse(
					http.StatusOK, testOtherContainerID, "running", foreign, "127.0.0.1", "40134",
				), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
				return dockerResponse(http.StatusOK, `{}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				return dockerResponse(http.StatusCreated, `{"Id":"`+testMemcacheContainerID+`"}`), nil
			case request.Method == http.MethodGet && request.URL.Path == "/containers/"+testMemcacheContainerID+"/json":
				if idInspects.Add(1) == 1 {
					return nil, errors.New("post-create inspect failed")
				}
				return memcachedInspectResponse(
					http.StatusOK, testMemcacheContainerID, "created", labels, "127.0.0.1", "40133",
				), nil
			case request.Method == http.MethodDelete && request.URL.Path == "/containers/"+testMemcacheContainerID:
				deletes.Add(1)
				return dockerResponse(http.StatusNoContent, ``), nil
			default:
				return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
			}
		},
	)}}
	_, owned, exists, err := manager.ProvisionMemcache(
		context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_5", nil,
	)
	if err == nil || owned || exists {
		t.Fatalf("error=%v owned=%v exists=%v", err, owned, exists)
	}
	if nameInspects.Load() != 1 || idInspects.Load() != 2 || deletes.Load() != 1 {
		t.Fatalf("nameInspects=%d idInspects=%d deletes=%d",
			nameInspects.Load(), idInspects.Load(), deletes.Load())
	}
}

func TestProvisionMemcacheSelectsExactVersionImage(t *testing.T) {
	for _, test := range []struct {
		version string
		image   string
	}{
		{version: "MEMCACHE_1_5", image: "memcached:1.5.16-alpine"},
		{version: "MEMCACHE_1_6_15", image: "memcached:1.6.15-alpine"},
	} {
		t.Run(test.version, func(t *testing.T) {
			image, err := memcacheImageForVersion(test.version)
			if err != nil {
				t.Fatal(err)
			}
			if image != test.image {
				t.Fatalf("image = %q, want %q", image, test.image)
			}
		})
	}
}

func TestProvisionMemcacheRejectsUnknownVersionBeforeDocker(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "unknown-version")
	var calls atomic.Int32
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(
		func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return nil, errors.New("Docker must not be called")
		},
	)}}
	if _, _, _, err := manager.ProvisionMemcache(
		context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_6", nil,
	); err == nil {
		t.Fatal("unknown version was accepted")
	}
	if calls.Load() != 0 {
		t.Fatalf("Docker calls = %d, want zero", calls.Load())
	}
}

func TestProvisionMemcacheJoinsCleanupFailureWithOriginalError(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "compensate-cleanup-error")
	manager, _, _ := postCreateFailureManager(t, "readiness", true)
	_, owned, exists, err := manager.ProvisionMemcache(
		context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_5", nil,
	)
	if err == nil || !strings.Contains(err.Error(), "readiness failed") ||
		!strings.Contains(err.Error(), "cleanup delete failed") {
		t.Fatalf("joined error = %v", err)
	}
	if !owned || !exists {
		t.Fatalf("failed cleanup state owned=%v exists=%v", owned, exists)
	}
}

func TestProvisionMemcacheCompensationRefusesNameReuse(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "compensate-name-reuse")
	name := memcachedDockerName(testMemcacheBackendID)
	foreign := memcachedLabels(testMemcacheBackendID)
	foreign["minisky.profile"] = "attacker"
	var nameInspects atomic.Int32
	var deletes atomic.Int32
	manager := &ServiceManager{
		dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			switch {
			case request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json":
				if nameInspects.Add(1) == 1 {
					return dockerResponse(http.StatusNotFound, `{}`), nil
				}
				return memcachedInspectResponse(
					http.StatusOK, strings.Repeat("b", 64), "created", foreign, "127.0.0.1", "40129",
				), nil
			case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
				return dockerResponse(http.StatusOK, `{}`), nil
			case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
				return dockerResponse(http.StatusCreated, `{`), nil
			case request.Method == http.MethodDelete:
				deletes.Add(1)
				return dockerResponse(http.StatusNoContent, ``), nil
			default:
				return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
			}
		})},
	}
	_, owned, exists, err := manager.ProvisionMemcache(
		context.Background(), testMemcacheBackendID, 1, 1, 1024, "MEMCACHE_1_5", nil,
	)
	if !errors.Is(err, ErrDockerOwnershipConflict) {
		t.Fatalf("error = %v, want ownership conflict", err)
	}
	if deletes.Load() != 0 {
		t.Fatalf("foreign name-reuse deletes = %d", deletes.Load())
	}
	if owned || !exists {
		t.Fatalf("foreign reuse state owned=%v exists=%v", owned, exists)
	}
}

func postCreateFailureManager(
	t *testing.T,
	stage string,
	cleanupFails bool,
) (*ServiceManager, *atomic.Int32, <-chan bool) {
	t.Helper()
	name := memcachedDockerName(testMemcacheBackendID)
	labels := memcachedLabels(testMemcacheBackendID)
	var nameInspects atomic.Int32
	var immutableInspects atomic.Int32
	var deletes atomic.Int32
	cleanupContext := make(chan bool, 1)
	manager := &ServiceManager{}
	manager.dockerClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/containers/"+name+"/json":
			if nameInspects.Add(1) == 1 {
				return dockerResponse(http.StatusNotFound, `{}`), nil
			}
			return memcachedInspectResponse(
				http.StatusOK, testMemcacheContainerID, "created", labels, "127.0.0.1", "40130",
			), nil
		case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/images/"):
			return dockerResponse(http.StatusOK, `{}`), nil
		case request.Method == http.MethodPost && request.URL.Path == "/containers/create":
			switch stage {
			case "response-decode":
				return dockerResponse(http.StatusCreated, `{`), nil
			case "empty-id":
				return dockerResponse(http.StatusCreated, `{}`), nil
			case "malformed-id":
				return dockerResponse(http.StatusCreated, `{"Id":"not-an-id"}`), nil
			default:
				return dockerResponse(http.StatusCreated, `{"Id":"`+testMemcacheContainerID+`"}`), nil
			}
		case request.Method == http.MethodGet && request.URL.Path == "/containers/"+testMemcacheContainerID+"/json":
			attempt := immutableInspects.Add(1)
			if stage == "inspect" && attempt == 1 {
				return nil, errors.New("inspect created container failed")
			}
			status := "created"
			hostPort := "40130"
			if stage == "endpoint" || stage == "readiness" || stage == "cancellation" {
				if attempt >= 2 {
					status = "running"
				}
				if stage == "endpoint" && attempt == 2 {
					hostPort = ""
				}
			}
			return memcachedInspectResponse(
				http.StatusOK, testMemcacheContainerID, status, labels, "127.0.0.1", hostPort,
			), nil
		case request.Method == http.MethodPost &&
			request.URL.Path == "/containers/"+testMemcacheContainerID+"/start":
			if stage == "start" {
				return dockerResponse(http.StatusInternalServerError, `start failed`), nil
			}
			return dockerResponse(http.StatusNoContent, ``), nil
		case request.Method == http.MethodDelete &&
			request.URL.Path == "/containers/"+testMemcacheContainerID:
			deletes.Add(1)
			select {
			case cleanupContext <- request.Context().Err() == nil:
			default:
			}
			if cleanupFails {
				return dockerResponse(http.StatusInternalServerError, `cleanup delete failed`), nil
			}
			return dockerResponse(http.StatusNoContent, ``), nil
		default:
			return nil, fmt.Errorf("unexpected Docker request %s %s", request.Method, request.URL)
		}
	})}
	manager.memcachedReady = func(context.Context, string, string, time.Duration) error {
		switch stage {
		case "readiness":
			return errors.New("readiness failed")
		case "cancellation":
			return context.Canceled
		default:
			return nil
		}
	}
	return manager, &deletes, cleanupContext
}
