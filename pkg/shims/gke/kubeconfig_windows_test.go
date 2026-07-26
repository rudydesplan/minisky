//go:build windows

package gke

import (
	"context"
	"errors"
	"testing"
)

func TestWindowsKubeconfigOperationsFailSafeUnsupported(t *testing.T) {
	if err := secureKubeconfigPlatformCheck(); !errors.Is(err, errSecureKubeconfigUnsupported) {
		t.Fatalf("platform check error = %v", err)
	}
	if _, err := securePrepareKubeconfig(`C:\unsafe`); !errors.Is(err, errSecureKubeconfigUnsupported) {
		t.Fatalf("prepare error = %v", err)
	}
	if err := securePublishKubeconfig(nil, `C:\final`); !errors.Is(err, errSecureKubeconfigUnsupported) {
		t.Fatalf("publish error = %v", err)
	}
	if _, err := secureReadKubeconfig(`C:\unsafe`); !errors.Is(err, errSecureKubeconfigUnsupported) {
		t.Fatalf("read error = %v", err)
	}
	backend := &KindBackend{enabled: true}
	identity := ClusterIdentity{Profile: "test", Project: "demo", Zone: "zone", Cluster: "cluster"}
	if err := backend.DeleteClusterContext(context.Background(), identity); !errors.Is(err, errSecureKubeconfigUnsupported) {
		t.Fatalf("backend delete error = %v", err)
	}
}
