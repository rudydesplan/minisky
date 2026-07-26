package orchestrator

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDeleteComputeVMContextRefusesUnownedSameName(t *testing.T) {
	t.Setenv("MINISKY_PROFILE", "active")
	mutated := false
	manager := &ServiceManager{dockerClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			mutated = true
			return dockerResponse(http.StatusNoContent, ``), nil
		}
		return dockerResponse(http.StatusOK, `{
			"State":{"Status":"created"},
			"Config":{"Labels":{
				"managed-by":"minisky",
				"minisky.profile":"other",
				"minisky.service":"compute-instance",
				"minisky.resource":"minisky-dataproc-cluster-m"
			}}
		}`), nil
	})}}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := manager.DeleteComputeVMContext(ctx, "minisky-dataproc-cluster-m")
	if err == nil || !strings.Contains(err.Error(), "refusing to delete unowned") {
		t.Fatalf("unowned collision error = %v", err)
	}
	if mutated {
		t.Fatal("unowned same-name container was mutated")
	}
}
