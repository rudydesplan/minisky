package composer

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestDockerAirflowDAGIntegration(t *testing.T) {
	if os.Getenv("MINISKY_DOCKER_PHASE19_AIRFLOW") != "1" {
		t.Skip("set MINISKY_DOCKER_PHASE19_AIRFLOW=1 to run the pinned Airflow integration")
	}
	t.Setenv("MINISKY_PROFILE", "phase19-airflow-integration")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	backend := newDockerAirflowBackend()
	resource := fmt.Sprintf("projects/p/locations/l/environments/integration-%d", time.Now().UnixNano())
	if _, err := backend.Provision(ctx, resource); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := backend.Delete(cleanupCtx, resource); err != nil {
			t.Errorf("cleanup Airflow backend: %v", err)
		}
	})
	const dag = `from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG("phase19", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    EmptyOperator(task_id="done")
`
	if err := backend.UploadDAG(ctx, resource, "phase19", dag); err != nil {
		t.Fatal(err)
	}
	if err := backend.TriggerDAG(ctx, resource, "phase19", "integration-run"); err != nil {
		t.Fatal(err)
	}
}
