package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	composer "google.golang.org/api/composer/v1"
	dataflow "google.golang.org/api/dataflow/v1b3"
	dataform "google.golang.org/api/dataform/v1beta1"
	"google.golang.org/api/googleapi"
	managedkafka "google.golang.org/api/managedkafka/v1"
	"google.golang.org/api/option"
	pubsublite "google.golang.org/api/pubsublite/v1"
)

const (
	experimentalOptInEnv = "MINISKY_PHASE19_EXPERIMENTAL_OPT_IN"
	evidenceVersion      = 1
	maxEvidenceBytes     = 16 << 10
)

var resourceIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

type config struct {
	mode, endpoint, project, location               string
	dataflowName, repositoryID, workspaceID         string
	clusterID, topicID, environmentID, evidencePath string
}

type evidence struct {
	Version                int    `json:"version"`
	Project                string `json:"project"`
	Location               string `json:"location"`
	DataflowID             string `json:"dataflowId"`
	DataflowName           string `json:"dataflowName"`
	DataflowState          string `json:"dataflowState"`
	CancelledDataflowID    string `json:"cancelledDataflowId"`
	CancelledDataflowState string `json:"cancelledDataflowState"`
	DataformRepository     string `json:"dataformRepository"`
	DataformWorkspace      string `json:"dataformWorkspace"`
	DataformCompilation    string `json:"dataformCompilation"`
	DataformInvocation     string `json:"dataformInvocation"`
	KafkaCluster           string `json:"kafkaCluster,omitempty"`
	KafkaTopic             string `json:"kafkaTopic,omitempty"`
	ComposerEnvironment    string `json:"composerEnvironment,omitempty"`
	ComposerState          string `json:"composerState,omitempty"`
}

type generatedClients struct {
	dataflow   *dataflow.Service
	dataform   *dataform.Service
	kafka      *managedkafka.Service
	composer   *composer.Service
	pubsublite *pubsublite.Service
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Phase 19 generated Go client smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	timeout := 45 * time.Second
	if strings.HasPrefix(cfg.mode, "docker-") {
		timeout = 8 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	clients, err := newGeneratedClients(ctx, cfg.endpoint)
	if err != nil {
		return err
	}
	switch cfg.mode {
	case "gate":
		return proveDefaultGate(ctx, clients, cfg)
	case "create":
		return createCore(ctx, clients, cfg)
	case "verify":
		return verifyCore(ctx, clients, cfg)
	case "delete":
		return deleteCore(ctx, clients, cfg)
	case "docker-create":
		return createDockerBackends(ctx, clients, cfg)
	case "docker-verify":
		return verifyDockerBackends(ctx, clients, cfg)
	case "docker-delete":
		return deleteDockerBackends(ctx, clients, cfg)
	default:
		return fmt.Errorf("unsupported MINISKY_PHASE19_MODE %q", cfg.mode)
	}
}

func configFromEnv() (config, error) {
	cfg := config{
		mode: env("MINISKY_PHASE19_MODE", "gate"), endpoint: strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/"),
		project: env("MINISKY_PROJECT_ID", "phase19-project"), location: env("MINISKY_PHASE19_LOCATION", "us-central1"),
		dataflowName:  env("MINISKY_PHASE19_DATAFLOW_NAME", "phase19-dataflow"),
		repositoryID:  env("MINISKY_PHASE19_REPOSITORY_ID", "phase19-repo"),
		workspaceID:   env("MINISKY_PHASE19_WORKSPACE_ID", "phase19-workspace"),
		clusterID:     env("MINISKY_PHASE19_CLUSTER_ID", "phase19-kafka"),
		topicID:       env("MINISKY_PHASE19_TOPIC_ID", "phase19-topic"),
		environmentID: env("MINISKY_PHASE19_ENVIRONMENT_ID", "phase19-airflow"),
		evidencePath:  strings.TrimSpace(os.Getenv("MINISKY_PHASE19_EVIDENCE")),
	}
	if err := validateLoopbackEndpoint(cfg.endpoint); err != nil {
		return config{}, err
	}
	for name, value := range map[string]string{
		"project": cfg.project, "location": cfg.location, "Dataflow name": cfg.dataflowName,
		"repository ID": cfg.repositoryID, "workspace ID": cfg.workspaceID, "cluster ID": cfg.clusterID,
		"topic ID": cfg.topicID, "environment ID": cfg.environmentID,
	} {
		if !resourceIDPattern.MatchString(value) {
			return config{}, fmt.Errorf("%s %q must match %s", name, value, resourceIDPattern)
		}
	}
	if cfg.evidencePath == "" || !filepath.IsAbs(cfg.evidencePath) {
		return config{}, errors.New("MINISKY_PHASE19_EVIDENCE must be an absolute path")
	}
	if cfg.mode != "gate" && os.Getenv(experimentalOptInEnv) != "1" {
		return config{}, fmt.Errorf("%s mode requires explicit %s=1", cfg.mode, experimentalOptInEnv)
	}
	return cfg, nil
}

func validateLoopbackEndpoint(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse MINISKY_ENDPOINT: %w", err)
	}
	if parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || parsed.Path != "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Port() == "" {
		return errors.New("MINISKY_ENDPOINT must be an HTTP loopback origin with an explicit port and no path")
	}
	if host := parsed.Hostname(); !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("MINISKY_ENDPOINT must target localhost or a loopback IP")
		}
	}
	return nil
}

func newGeneratedClients(ctx context.Context, endpoint string) (*generatedClients, error) {
	opts := func(domain string) []option.ClientOption {
		return []option.ClientOption{option.WithoutAuthentication(), option.WithEndpoint(endpoint + "/_minisky/" + domain + "/")}
	}
	df, err := dataflow.NewService(ctx, opts("dataflow.googleapis.com")...)
	if err != nil {
		return nil, err
	}
	form, err := dataform.NewService(ctx, opts("dataform.googleapis.com")...)
	if err != nil {
		return nil, err
	}
	kafka, err := managedkafka.NewService(ctx, opts("managedkafka.googleapis.com")...)
	if err != nil {
		return nil, err
	}
	comp, err := composer.NewService(ctx, opts("composer.googleapis.com")...)
	if err != nil {
		return nil, err
	}
	lite, err := pubsublite.NewService(ctx, opts("pubsublite.googleapis.com")...)
	if err != nil {
		return nil, err
	}
	return &generatedClients{df, form, kafka, comp, lite}, nil
}

func proveDefaultGate(ctx context.Context, clients *generatedClients, cfg config) error {
	parent := locationParent(cfg)
	checks := []struct {
		name string
		call func() error
	}{
		{"Dataflow", func() error {
			_, err := clients.dataflow.Projects.Locations.Jobs.List(cfg.project, cfg.location).Context(ctx).Do()
			return err
		}},
		{"Dataform", func() error {
			_, err := clients.dataform.Projects.Locations.Repositories.List(parent).Context(ctx).Do()
			return err
		}},
		{"Managed Kafka", func() error {
			_, err := clients.kafka.Projects.Locations.Clusters.List(parent).Context(ctx).Do()
			return err
		}},
		{"Composer", func() error {
			_, err := clients.composer.Projects.Locations.Environments.List(parent).Context(ctx).Do()
			return err
		}},
		{"Pub/Sub Lite", func() error {
			_, err := clients.pubsublite.Admin.Projects.Locations.Topics.List(parent).Context(ctx).Do()
			return err
		}},
	}
	for _, check := range checks {
		if err := expectGoogleStatus(check.call(), 501, "UNIMPLEMENTED"); err != nil {
			return fmt.Errorf("%s default gate: %w", check.name, err)
		}
	}
	fmt.Println("default-disabled Phase 19 gate verified with five generated clients; Pub/Sub Lite returned explicit 501")
	return nil
}

func createCore(ctx context.Context, clients *generatedClients, cfg config) error {
	createProps := googleapi.RawMessage(`{"elements":["one","two","three"]}`)
	countProps := googleapi.RawMessage(`{}`)
	job, err := clients.dataflow.Projects.Locations.Jobs.Create(cfg.project, cfg.location, &dataflow.Job{
		Name: cfg.dataflowName, Type: "JOB_TYPE_BATCH",
		Steps: []*dataflow.Step{{Name: "create", Kind: "Create", Properties: createProps}, {Name: "count", Kind: "Count", Properties: countProps}},
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create bounded Dataflow job: %w", err)
	}
	job, err = waitDataflow(ctx, clients.dataflow, cfg, job.Id, "JOB_STATE_DONE")
	if err != nil {
		return err
	}
	cancelled, err := clients.dataflow.Projects.Locations.Jobs.Create(cfg.project, cfg.location, &dataflow.Job{
		Name: cfg.dataflowName + "-cancel", Type: "JOB_TYPE_BATCH",
		Steps: []*dataflow.Step{{Name: "create", Kind: "Create", Properties: createProps}, {Name: "count", Kind: "Count", Properties: countProps}},
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create cancellable Dataflow job: %w", err)
	}
	cancelled, err = clients.dataflow.Projects.Locations.Jobs.Update(cfg.project, cfg.location, cancelled.Id,
		&dataflow.Job{RequestedState: "JOB_STATE_CANCELLED"}).Context(ctx).Do()
	if err != nil || cancelled.CurrentState != "JOB_STATE_CANCELLED" {
		return fmt.Errorf("cancel Dataflow job state=%q: %w", resourceDataflowState(cancelled), err)
	}

	parent := locationParent(cfg)
	repo, err := clients.dataform.Projects.Locations.Repositories.Create(parent, &dataform.Repository{}).
		RepositoryId(cfg.repositoryID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Dataform repository: %w", err)
	}
	workspace, err := clients.dataform.Projects.Locations.Repositories.Workspaces.Create(repo.Name, &dataform.Workspace{}).
		WorkspaceId(cfg.workspaceID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Dataform workspace: %w", err)
	}
	compilation, err := clients.dataform.Projects.Locations.Repositories.CompilationResults.Create(repo.Name,
		&dataform.CompilationResult{Workspace: workspace.Name}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("compile Dataform workspace: %w", err)
	}
	invocation, err := clients.dataform.Projects.Locations.Repositories.WorkflowInvocations.Create(repo.Name,
		&dataform.WorkflowInvocation{CompilationResult: compilation.Name}).Context(ctx).Do()
	if err != nil || invocation.State != "SUCCEEDED" {
		return fmt.Errorf("invoke Dataform compilation state=%q: %w", invocationState(invocation), err)
	}
	record := evidence{
		Version: evidenceVersion, Project: cfg.project, Location: cfg.location,
		DataflowID: job.Id, DataflowName: job.Name, DataflowState: job.CurrentState,
		CancelledDataflowID: cancelled.Id, CancelledDataflowState: cancelled.CurrentState,
		DataformRepository: repo.Name, DataformWorkspace: workspace.Name,
		DataformCompilation: compilation.Name, DataformInvocation: invocation.Name,
	}
	if err := writeEvidence(cfg.evidencePath, record); err != nil {
		return err
	}
	fmt.Println("generated clients proved Dataflow Create/Count terminal and cancel plus Dataform compile/invocation hierarchy")
	return nil
}

func verifyCore(ctx context.Context, clients *generatedClients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	job, err := clients.dataflow.Projects.Locations.Jobs.Get(cfg.project, cfg.location, record.DataflowID).Context(ctx).Do()
	if err != nil || job.CurrentState != record.DataflowState {
		return fmt.Errorf("restart Dataflow terminal state=%q: %w", resourceDataflowState(job), err)
	}
	cancelled, err := clients.dataflow.Projects.Locations.Jobs.Get(cfg.project, cfg.location, record.CancelledDataflowID).Context(ctx).Do()
	if err != nil || cancelled.CurrentState != record.CancelledDataflowState {
		return fmt.Errorf("restart Dataflow cancelled state=%q: %w", resourceDataflowState(cancelled), err)
	}
	if got, err := clients.dataform.Projects.Locations.Repositories.CompilationResults.Get(record.DataformCompilation).Context(ctx).Do(); err != nil || got.Workspace != record.DataformWorkspace {
		return fmt.Errorf("restart Dataform compilation: %w", err)
	}
	if got, err := clients.dataform.Projects.Locations.Repositories.WorkflowInvocations.Get(record.DataformInvocation).Context(ctx).Do(); err != nil || got.CompilationResult != record.DataformCompilation || got.State != "SUCCEEDED" {
		return fmt.Errorf("restart Dataform invocation: %w", err)
	}
	fmt.Println("generated-client restart persistence verified for terminal/cancelled Dataflow and Dataform descendants")
	return nil
}

func deleteCore(ctx context.Context, clients *generatedClients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	if _, err := clients.dataform.Projects.Locations.Repositories.Delete(record.DataformRepository).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete Dataform repository: %w", err)
	}
	for name, call := range map[string]func() error{
		"repository": func() error {
			_, err := clients.dataform.Projects.Locations.Repositories.Get(record.DataformRepository).Context(ctx).Do()
			return err
		},
		"workspace": func() error {
			_, err := clients.dataform.Projects.Locations.Repositories.Workspaces.Get(record.DataformWorkspace).Context(ctx).Do()
			return err
		},
		"compilation": func() error {
			_, err := clients.dataform.Projects.Locations.Repositories.CompilationResults.Get(record.DataformCompilation).Context(ctx).Do()
			return err
		},
		"invocation": func() error {
			_, err := clients.dataform.Projects.Locations.Repositories.WorkflowInvocations.Get(record.DataformInvocation).Context(ctx).Do()
			return err
		},
	} {
		if err := expectGoogleStatus(call(), 404, "NOT_FOUND"); err != nil {
			return fmt.Errorf("Dataform deleted %s: %w", name, err)
		}
	}
	_, err = clients.dataflow.Projects.Locations.Jobs.Get(cfg.project, cfg.location, "phase19-missing").Context(ctx).Do()
	if err := expectGoogleStatus(err, 404, "NOT_FOUND"); err != nil {
		return fmt.Errorf("Dataflow missing-resource boundary: %w", err)
	}
	fmt.Println("generated-client Dataform cascade and explicit 404 boundaries verified")
	return nil
}

func createDockerBackends(ctx context.Context, clients *generatedClients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	parent := locationParent(cfg)
	clusterName := parent + "/clusters/" + cfg.clusterID
	op, err := clients.kafka.Projects.Locations.Clusters.Create(parent, &managedkafka.Cluster{Name: clusterName}).
		ClusterId(cfg.clusterID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Managed Kafka cluster: %w", err)
	}
	if err := waitKafkaOperation(ctx, clients.kafka, op.Name); err != nil {
		return err
	}
	cluster, err := clients.kafka.Projects.Locations.Clusters.Get(clusterName).Context(ctx).Do()
	if err != nil || cluster.State != "ACTIVE" {
		return fmt.Errorf("Managed Kafka cluster state=%q: %w", clusterState(cluster), err)
	}
	topicName := clusterName + "/topics/" + cfg.topicID
	topic, err := clients.kafka.Projects.Locations.Clusters.Topics.Create(clusterName,
		&managedkafka.Topic{Name: topicName, PartitionCount: 1, ReplicationFactor: 1}).
		TopicId(cfg.topicID).Context(ctx).Do()
	if err != nil || topic.Name != topicName {
		return fmt.Errorf("create Managed Kafka topic: %w", err)
	}

	envName := parent + "/environments/" + cfg.environmentID
	composerOp, err := clients.composer.Projects.Locations.Environments.Create(parent, &composer.Environment{Name: envName}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Composer environment: %w", err)
	}
	if err := waitComposerOperation(ctx, clients.composer, composerOp.Name); err != nil {
		return err
	}
	environment, err := clients.composer.Projects.Locations.Environments.Get(envName).Context(ctx).Do()
	if err != nil || environment.State != "RUNNING" {
		return fmt.Errorf("Composer environment state=%q: %w", composerState(environment), err)
	}
	record.KafkaCluster, record.KafkaTopic = clusterName, topicName
	record.ComposerEnvironment, record.ComposerState = envName, environment.State
	if err := writeEvidence(cfg.evidencePath, record); err != nil {
		return err
	}
	fmt.Println("generated control clients created pinned Kafka and Composer backends")
	return nil
}

func verifyDockerBackends(ctx context.Context, clients *generatedClients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	if record.KafkaCluster == "" || record.ComposerEnvironment == "" {
		return errors.New("Docker backend evidence is absent")
	}
	cluster, err := clients.kafka.Projects.Locations.Clusters.Get(record.KafkaCluster).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("restart Managed Kafka get: %w", err)
	}
	if cluster.State != "ACTIVE" {
		return fmt.Errorf("restart Managed Kafka state=%q want=ACTIVE", clusterState(cluster))
	}
	if _, err := clients.kafka.Projects.Locations.Clusters.Topics.Get(record.KafkaTopic).Context(ctx).Do(); err != nil {
		return fmt.Errorf("restart Managed Kafka topic: %w", err)
	}
	environment, err := clients.composer.Projects.Locations.Environments.Get(record.ComposerEnvironment).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("restart Composer get: %w", err)
	}
	if environment.State != "RUNNING" {
		return fmt.Errorf("restart Composer state=%q want=RUNNING", composerState(environment))
	}
	fmt.Println("generated-client restart verified durable Kafka/Composer metadata and exact-owned backend reconciliation")
	return nil
}

func deleteDockerBackends(ctx context.Context, clients *generatedClients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	if _, err := clients.kafka.Projects.Locations.Clusters.Topics.Delete(record.KafkaTopic).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete Managed Kafka topic: %w", err)
	}
	if _, err := clients.kafka.Projects.Locations.Clusters.Delete(record.KafkaCluster).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete Managed Kafka cluster: %w", err)
	}
	if _, err := clients.composer.Projects.Locations.Environments.Delete(record.ComposerEnvironment).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete Composer environment: %w", err)
	}
	for name, call := range map[string]func() error{
		"Kafka cluster": func() error {
			_, err := clients.kafka.Projects.Locations.Clusters.Get(record.KafkaCluster).Context(ctx).Do()
			return err
		},
		"Kafka topic": func() error {
			_, err := clients.kafka.Projects.Locations.Clusters.Topics.Get(record.KafkaTopic).Context(ctx).Do()
			return err
		},
		"Composer environment": func() error {
			_, err := clients.composer.Projects.Locations.Environments.Get(record.ComposerEnvironment).Context(ctx).Do()
			return err
		},
	} {
		if err := expectGoogleStatus(call(), 404, "NOT_FOUND"); err != nil {
			return fmt.Errorf("verify deleted %s: %w", name, err)
		}
	}
	fmt.Println("generated-client Docker backend deletes and 404 boundaries verified")
	return nil
}

func waitDataflow(ctx context.Context, service *dataflow.Service, cfg config, id, want string) (*dataflow.Job, error) {
	for {
		job, err := service.Projects.Locations.Jobs.Get(cfg.project, cfg.location, id).Context(ctx).Do()
		if err != nil {
			return nil, err
		}
		if job.CurrentState == want {
			return job, nil
		}
		if job.CurrentState == "JOB_STATE_FAILED" || job.CurrentState == "JOB_STATE_CANCELLED" {
			return nil, fmt.Errorf("Dataflow job reached %s", job.CurrentState)
		}
		if err := waitTick(ctx); err != nil {
			return nil, err
		}
	}
}

func waitKafkaOperation(ctx context.Context, service *managedkafka.Service, name string) error {
	for {
		op, err := service.Projects.Locations.Operations.Get(name).Context(ctx).Do()
		if err != nil {
			return err
		}
		if op.Done {
			if op.Error != nil {
				return fmt.Errorf("Managed Kafka operation: %s", op.Error.Message)
			}
			return nil
		}
		if err := waitTick(ctx); err != nil {
			return err
		}
	}
}

func waitComposerOperation(ctx context.Context, service *composer.Service, name string) error {
	for {
		op, err := service.Projects.Locations.Operations.Get(name).Context(ctx).Do()
		if err != nil {
			return err
		}
		if op.Done {
			if op.Error != nil {
				return fmt.Errorf("Composer operation: %s", op.Error.Message)
			}
			return nil
		}
		if err := waitTick(ctx); err != nil {
			return err
		}
	}
}

func waitTick(ctx context.Context) error {
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func expectGoogleStatus(err error, code int, status string) error {
	if err == nil {
		return fmt.Errorf("expected HTTP %d %s, got success", code, status)
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != code {
		return fmt.Errorf("expected googleapi.Error HTTP %d, got %T: %w", code, err, err)
	}
	var envelope struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(apiErr.Body), &envelope) != nil || envelope.Error.Status != status {
		return fmt.Errorf("status=%q want=%q body=%s", envelope.Error.Status, status, apiErr.Body)
	}
	return nil
}

func writeEvidence(path string, record evidence) error {
	if err := validateEvidence(record); err != nil {
		return err
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".phase19-evidence-*.tmp")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(append(data, '\n')); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func readEvidence(path string, cfg config) (evidence, error) {
	file, err := os.Open(path)
	if err != nil {
		return evidence{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxEvidenceBytes+1))
	if err != nil {
		return evidence{}, err
	}
	if len(data) > maxEvidenceBytes {
		return evidence{}, errors.New("Phase 19 evidence exceeds size limit")
	}
	var record evidence
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return evidence{}, err
	}
	if err := validateEvidence(record); err != nil {
		return evidence{}, err
	}
	if record.Project != cfg.project || record.Location != cfg.location || record.DataflowName != cfg.dataflowName {
		return evidence{}, errors.New("evidence identifiers do not match smoke configuration")
	}
	return record, nil
}

func validateEvidence(record evidence) error {
	if record.Version != evidenceVersion || record.DataflowID == "" || record.CancelledDataflowID == "" {
		return errors.New("incomplete Phase 19 evidence")
	}
	if record.DataflowState != "JOB_STATE_DONE" || record.CancelledDataflowState != "JOB_STATE_CANCELLED" {
		return errors.New("Dataflow evidence is not terminal")
	}
	parent := "projects/" + record.Project + "/locations/" + record.Location
	repoPrefix := parent + "/repositories/"
	if !strings.HasPrefix(record.DataformRepository, repoPrefix) ||
		!strings.HasPrefix(record.DataformWorkspace, record.DataformRepository+"/workspaces/") ||
		!strings.HasPrefix(record.DataformCompilation, record.DataformRepository+"/compilationResults/") ||
		!strings.HasPrefix(record.DataformInvocation, record.DataformRepository+"/workflowInvocations/") {
		return errors.New("Dataform evidence hierarchy is inconsistent")
	}
	dockerEmpty := record.KafkaCluster == "" && record.KafkaTopic == "" &&
		record.ComposerEnvironment == "" && record.ComposerState == ""
	dockerComplete := strings.HasPrefix(record.KafkaCluster, parent+"/clusters/") &&
		strings.HasPrefix(record.KafkaTopic, record.KafkaCluster+"/topics/") &&
		strings.HasPrefix(record.ComposerEnvironment, parent+"/environments/") && record.ComposerState == "RUNNING"
	if !dockerEmpty && !dockerComplete {
		return errors.New("Docker backend evidence is incomplete")
	}
	return nil
}

func locationParent(cfg config) string {
	return "projects/" + cfg.project + "/locations/" + cfg.location
}
func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
func resourceDataflowState(job *dataflow.Job) string {
	if job == nil {
		return ""
	}
	return job.CurrentState
}
func invocationState(invocation *dataform.WorkflowInvocation) string {
	if invocation == nil {
		return ""
	}
	return invocation.State
}
func clusterState(cluster *managedkafka.Cluster) string {
	if cluster == nil {
		return ""
	}
	return cluster.State
}
func composerState(environment *composer.Environment) string {
	if environment == nil {
		return ""
	}
	return environment.State
}
