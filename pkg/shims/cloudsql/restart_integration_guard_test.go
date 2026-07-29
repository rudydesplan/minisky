//go:build !windows

package cloudsql

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCloudSQLLiveOptInPrerequisitesAreFatalStructural(t *testing.T) {
	source, err := os.ReadFile(filepath.Join(".", "restart_integration_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(source, []byte("t.Skipf(")) {
		t.Fatal("live Cloud SQL test still skips a prerequisite after explicit opt-in")
	}
}

func TestCloudSQLLiveOptInAliasesIncludeMakeTargetGuard(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   bool
	}{
		{name: "none", values: map[string]string{}, want: false},
		{name: "Make and CI guard", values: map[string]string{cloudSQLDockerIntegrationOptIn: "1"}, want: true},
		{name: "local restart alias", values: map[string]string{cloudSQLRestartIntegrationOptInAlias: "1"}, want: true},
		{name: "wrong value", values: map[string]string{cloudSQLDockerIntegrationOptIn: "true"}, want: false},
		{name: "unrelated broad integration", values: map[string]string{"MINISKY_INTEGRATION": "1"}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := cloudSQLLiveIntegrationEnabled(func(name string) string {
				return test.values[name]
			}); got != test.want {
				t.Fatalf("enabled=%t, want %t", got, test.want)
			}
		})
	}
}
