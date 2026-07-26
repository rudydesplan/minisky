package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestNormalizeVPCIPv4PrefixRejectsSpecialUseOverlap(t *testing.T) {
	tests := []struct {
		cidr string
		ok   bool
	}{
		{cidr: "169.0.0.0/8"},
		{cidr: "169.254.0.0/16"},
		{cidr: "0.0.0.0/8"},
		{cidr: "127.0.0.0/8"},
		{cidr: "224.0.0.0/4"},
		{cidr: "240.0.0.0/4"},
		{cidr: "255.255.255.248/29"},
		{cidr: "169.253.0.0/16", ok: true},
		{cidr: "169.255.0.0/16", ok: true},
		{cidr: "10.0.0.0/8", ok: true},
		{cidr: "172.16.0.0/12", ok: true},
		{cidr: "192.168.0.0/16", ok: true},
	}
	for _, test := range tests {
		t.Run(test.cidr, func(t *testing.T) {
			prefix, err := NormalizeVPCIPv4Prefix(test.cidr)
			if test.ok {
				if err != nil || prefix.String() != test.cidr {
					t.Fatalf("prefix=%v error=%v", prefix, err)
				}
			} else if err == nil {
				t.Fatalf("invalid CIDR accepted: %s", prefix)
			}
		})
	}
}

func TestDeleteLegacyVPCNetworkExactOwnershipAndImmutableID(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "legacy-vpc")
	const network = "shared"
	exact := map[string]string{
		"managed-by":       "minisky",
		"minisky.profile":  "legacy-vpc",
		"minisky.service":  "compute-network",
		"minisky.resource": network,
	}
	for _, test := range []struct {
		name       string
		status     int
		labels     map[string]string
		driver     string
		deleteCode int
		wantErr    bool
	}{
		{name: "exact", status: http.StatusOK, labels: exact, driver: "bridge", deleteCode: http.StatusNoContent},
		{name: "delete race missing", status: http.StatusOK, labels: exact, driver: "bridge", deleteCode: http.StatusNotFound},
		{name: "attached", status: http.StatusOK, labels: exact, driver: "bridge", deleteCode: http.StatusConflict, wantErr: true},
		{name: "wrong profile", status: http.StatusOK, labels: map[string]string{
			"managed-by": "minisky", "minisky.profile": "other",
			"minisky.service": "compute-network", "minisky.resource": network,
		}, driver: "bridge", wantErr: true},
		{name: "unowned", status: http.StatusOK, labels: map[string]string{"managed-by": "other"}, driver: "bridge", wantErr: true},
		{name: "new scoped", status: http.StatusOK, labels: map[string]string{
			"managed-by": "minisky", "minisky.profile": "legacy-vpc",
			"minisky.service": "compute-network", "minisky.project": "project-a",
			"minisky.network": network, "minisky.canonical-resource": "projects/project-a/global/networks/shared",
		}, driver: "bridge", wantErr: true},
		{name: "missing", status: http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			deletedPath := ""
			manager := newVPCDockerManager(t, func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodGet {
					if test.status == http.StatusNotFound {
						return dockerResponse(http.StatusNotFound, `{}`), nil
					}
					labels, _ := json.Marshal(test.labels)
					return dockerResponse(test.status, `{"Id":"legacy/id","Driver":"`+test.driver+
						`","Labels":`+string(labels)+`}`), nil
				}
				deletedPath = request.URL.EscapedPath()
				code := test.deleteCode
				if code == 0 {
					code = http.StatusNoContent
				}
				return dockerResponse(code, `{"message":"network has active endpoints"}`), nil
			})
			err := manager.DeleteLegacyVPCNetwork(context.Background(), network)
			if test.wantErr {
				if err == nil {
					t.Fatal("legacy cleanup unexpectedly succeeded")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if test.name == "exact" && deletedPath != "/networks/legacy%2Fid" {
				t.Fatalf("immutable delete path=%q", deletedPath)
			}
			if test.wantErr && test.name != "attached" && deletedPath != "" {
				t.Fatalf("unsafe cleanup targeted %q", deletedPath)
			}
			if test.name == "attached" && (err == nil || !strings.Contains(err.Error(), "active endpoints")) {
				t.Fatalf("attached error=%v", err)
			}
		})
	}
}
