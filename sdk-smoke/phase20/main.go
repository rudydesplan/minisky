package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	alloydb "google.golang.org/api/alloydb/v1"
	file "google.golang.org/api/file/v1"
	"google.golang.org/api/googleapi"
	identitytoolkit "google.golang.org/api/identitytoolkit/v2"
	"google.golang.org/api/option"
	redis "google.golang.org/api/redis/v1"
	storage "google.golang.org/api/storage/v1"
	storagetransfer "google.golang.org/api/storagetransfer/v1"
)

const (
	optInEnv             = "MINISKY_PHASE20_OPT_IN"
	evidenceVersion      = 1
	maxEvidenceBytes     = 16 << 10
	transferObjectName   = "phase20/source.txt"
	transferObjectBody   = "phase20-generated-client-gcs-transfer"
	transferObjectSHA256 = "1d2785fd668c70c04b6057a1059caae96b9b6f305bbba4f451fb1e70f9658b26"
	defaultPostgresImage = "postgres:15.8-bookworm@sha256:eb3747f5d0a92195ca486d2f15d9a4ee5e9461b0332fe87fbc59069490a5c659"
	defaultValkeyImage   = "valkey/valkey:7.2.12-alpine@sha256:d0809f1d763f9fc3d77cd27e7c0393b1b0d69b6ad9269f932328b4793a620c78"
)

var resourceIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var digestImagePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}$`)

type config struct {
	mode                string
	endpoint            string
	project             string
	location            string
	clusterID           string
	alloyDBInstanceID   string
	filestoreInstanceID string
	redisInstanceID     string
	sourceBucket        string
	sinkBucket          string
	postgresImage       string
	valkeyImage         string
	evidencePath        string
}

type evidence struct {
	Version               int    `json:"version"`
	Project               string `json:"project"`
	Location              string `json:"location"`
	ClusterName           string `json:"clusterName"`
	AlloyDBInstanceName   string `json:"alloyDBInstanceName"`
	TenantName            string `json:"tenantName"`
	FilestoreInstanceName string `json:"filestoreInstanceName"`
	RedisInstanceName     string `json:"redisInstanceName"`
	TransferJobName       string `json:"transferJobName"`
	TransferOperationName string `json:"transferOperationName"`
	TransferRunStatus     string `json:"transferRunStatus"`
	SourceBucket          string `json:"sourceBucket"`
	SinkBucket            string `json:"sinkBucket"`
	ObjectName            string `json:"objectName"`
	ObjectSHA256          string `json:"objectSha256"`
	PostgresImage         string `json:"postgresImage"`
	ValkeyImage           string `json:"valkeyImage"`
}

type generatedClients struct {
	alloy    *alloydb.Service
	identity *identitytoolkit.Service
	file     *file.Service
	redis    *redis.Service
	transfer *storagetransfer.Service
	storage  *storage.Service
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Phase 20 generated Go client smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	if os.Getenv(optInEnv) != "1" {
		return fmt.Errorf("Phase 20 lifecycle requires explicit %s=1", optInEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	clients, err := newGeneratedClients(ctx, cfg.endpoint)
	if err != nil {
		return err
	}
	switch cfg.mode {
	case "boundary":
		return proveStorageTransferBoundary(ctx, clients, cfg)
	case "create":
		return createAndRecord(ctx, clients, cfg)
	case "verify":
		return verifyRestart(ctx, clients, cfg)
	case "delete":
		return deleteAndVerify(ctx, clients, cfg)
	default:
		return fmt.Errorf("unsupported MINISKY_PHASE20_MODE %q", cfg.mode)
	}
}

func proveStorageTransferBoundary(ctx context.Context, clients *generatedClients, cfg config) error {
	job, err := clients.transfer.TransferJobs.Create(&storagetransfer.TransferJob{
		ProjectId: cfg.project, Status: "ENABLED", Description: "Phase 20 generated-client boundary",
		TransferSpec: &storagetransfer.TransferSpec{
			GcsDataSource: &storagetransfer.GcsData{BucketName: cfg.sourceBucket},
			GcsDataSink:   &storagetransfer.GcsData{BucketName: cfg.sinkBucket},
		},
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Storage Transfer boundary job: %w", err)
	}
	runOperation, err := clients.transfer.TransferJobs.Run(job.Name,
		&storagetransfer.RunTransferJobRequest{ProjectId: cfg.project}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("run Storage Transfer boundary job: %w", err)
	}
	if !runOperation.Done {
		return errors.New("Storage Transfer boundary operation did not complete")
	}
	if runOperation.Error != nil {
		return fmt.Errorf("Storage Transfer boundary operation failed: %s", runOperation.Error.Message)
	}
	if _, err := clients.transfer.TransferJobs.Patch(job.Name, &storagetransfer.UpdateTransferJobRequest{
		ProjectId: cfg.project, TransferJob: &storagetransfer.TransferJob{Status: "DELETED"},
		UpdateTransferJobFieldMask: "status",
	}).Context(ctx).Do(); err != nil {
		return fmt.Errorf("soft-delete Storage Transfer boundary job: %w", err)
	}
	fmt.Println("Storage Transfer generated :run client returned a completed operation")
	return nil
}

func configFromEnv() (config, error) {
	cfg := config{
		mode:                env("MINISKY_PHASE20_MODE", "create"),
		endpoint:            strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/"),
		project:             env("MINISKY_PROJECT_ID", "phase20-project"),
		location:            env("MINISKY_PHASE20_LOCATION", "us-central1"),
		clusterID:           env("MINISKY_PHASE20_CLUSTER_ID", "phase20-cluster"),
		alloyDBInstanceID:   env("MINISKY_PHASE20_ALLOYDB_INSTANCE_ID", "phase20-primary"),
		filestoreInstanceID: env("MINISKY_PHASE20_FILESTORE_INSTANCE_ID", "phase20-files"),
		redisInstanceID:     env("MINISKY_PHASE20_REDIS_INSTANCE_ID", "phase20-cache"),
		sourceBucket:        env("MINISKY_PHASE20_SOURCE_BUCKET", "phase20-source"),
		sinkBucket:          env("MINISKY_PHASE20_SINK_BUCKET", "phase20-sink"),
		postgresImage:       env("MINISKY_PHASE20_POSTGRES_IMAGE", defaultPostgresImage),
		valkeyImage:         env("MINISKY_PHASE20_VALKEY_IMAGE", defaultValkeyImage),
		evidencePath:        strings.TrimSpace(os.Getenv("MINISKY_PHASE20_EVIDENCE")),
	}
	if err := validateLoopbackEndpoint(cfg.endpoint); err != nil {
		return config{}, err
	}
	for name, value := range map[string]string{
		"project": cfg.project, "location": cfg.location, "cluster ID": cfg.clusterID,
		"AlloyDB instance ID": cfg.alloyDBInstanceID, "Filestore instance ID": cfg.filestoreInstanceID,
		"Redis instance ID": cfg.redisInstanceID, "source bucket": cfg.sourceBucket, "sink bucket": cfg.sinkBucket,
	} {
		if !resourceIDPattern.MatchString(value) {
			return config{}, fmt.Errorf("%s %q must match %s", name, value, resourceIDPattern)
		}
	}
	if cfg.sourceBucket == cfg.sinkBucket {
		return config{}, errors.New("source and sink buckets must differ")
	}
	if cfg.evidencePath == "" || !filepath.IsAbs(cfg.evidencePath) {
		return config{}, errors.New("MINISKY_PHASE20_EVIDENCE must be an absolute path")
	}
	if !digestImagePattern.MatchString(cfg.postgresImage) {
		return config{}, errors.New("MINISKY_PHASE20_POSTGRES_IMAGE must be pinned by sha256 digest")
	}
	if !digestImagePattern.MatchString(cfg.valkeyImage) {
		return config{}, errors.New("MINISKY_PHASE20_VALKEY_IMAGE must be pinned by sha256 digest")
	}
	return cfg, nil
}

func validateLoopbackEndpoint(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse MINISKY_ENDPOINT: %w", err)
	}
	if parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("MINISKY_ENDPOINT must be an HTTP loopback origin without path, query, fragment, or userinfo")
	}
	host := parsed.Hostname()
	if !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("MINISKY_ENDPOINT must target localhost or a loopback IP")
		}
	}
	if parsed.Port() == "" {
		return errors.New("MINISKY_ENDPOINT must include an explicit port")
	}
	return nil
}

func newGeneratedClients(ctx context.Context, endpoint string) (*generatedClients, error) {
	options := func(domain string) []option.ClientOption {
		return []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithEndpoint(endpoint + "/_minisky/" + domain + "/"),
		}
	}
	alloyClient, err := alloydb.NewService(ctx, options("alloydb.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create AlloyDB client: %w", err)
	}
	identityClient, err := identitytoolkit.NewService(ctx, options("identityplatform.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Identity Platform client: %w", err)
	}
	fileClient, err := file.NewService(ctx, options("file.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Filestore client: %w", err)
	}
	redisClient, err := redis.NewService(ctx, options("redis.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Memorystore Redis client: %w", err)
	}
	transferClient, err := storagetransfer.NewService(ctx, options("storagetransfer.googleapis.com")...)
	if err != nil {
		return nil, fmt.Errorf("create Storage Transfer client: %w", err)
	}
	storageClient, err := storage.NewService(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(endpoint+"/_minisky/storage.googleapis.com/storage/v1/"),
		option.WithHTTPClient(&http.Client{Transport: &canonicalStorageTransport{
			base: http.DefaultTransport, prefix: "/_minisky/storage.googleapis.com",
		}}),
	)
	if err != nil {
		return nil, fmt.Errorf("create Storage client: %w", err)
	}
	return &generatedClients{
		alloy: alloyClient, identity: identityClient, file: fileClient,
		redis: redisClient, transfer: transferClient, storage: storageClient,
	}, nil
}

type canonicalStorageTransport struct {
	base   http.RoundTripper
	prefix string
}

func (t *canonicalStorageTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	if strings.HasPrefix(clone.URL.Path, "/upload/storage/v1/") {
		clone.URL.Path = t.prefix + clone.URL.Path
	}
	return t.base.RoundTrip(clone)
}

func createAndRecord(ctx context.Context, clients *generatedClients, cfg config) error {
	parent := locationParent(cfg)
	clusterName := parent + "/clusters/" + cfg.clusterID
	alloyInstanceName := clusterName + "/instances/" + cfg.alloyDBInstanceID
	filestoreName := parent + "/instances/" + cfg.filestoreInstanceID
	redisName := parent + "/instances/" + cfg.redisInstanceID

	if _, err := clients.storage.Buckets.Insert(cfg.project,
		&storage.Bucket{Name: cfg.sourceBucket}).Context(ctx).Do(); err != nil {
		return fmt.Errorf("create source bucket: %w", err)
	}
	if _, err := clients.storage.Buckets.Insert(cfg.project, &storage.Bucket{Name: cfg.sinkBucket}).Context(ctx).Do(); err != nil {
		return fmt.Errorf("create sink bucket: %w", err)
	}
	if _, err := clients.storage.Objects.Insert(cfg.sourceBucket, &storage.Object{Name: transferObjectName}).
		Media(strings.NewReader(transferObjectBody)).Context(ctx).Do(); err != nil {
		return fmt.Errorf("upload source object with generated Storage client: %w", err)
	}

	clusterOp, err := clients.alloy.Projects.Locations.Clusters.Create(parent, &alloydb.Cluster{
		Network: "projects/" + cfg.project + "/global/networks/default",
	}).ClusterId(cfg.clusterID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create AlloyDB cluster: %w", err)
	}
	if !clusterOp.Done {
		return errors.New("AlloyDB cluster create did not complete")
	}
	instanceOp, err := clients.alloy.Projects.Locations.Clusters.Instances.Create(clusterName, &alloydb.Instance{
		InstanceType: "PRIMARY",
	}).InstanceId(cfg.alloyDBInstanceID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create AlloyDB primary instance: %w", err)
	}
	if !instanceOp.Done {
		return errors.New("AlloyDB instance create did not complete")
	}
	alloyInstance, err := clients.alloy.Projects.Locations.Clusters.Instances.Get(alloyInstanceName).Context(ctx).Do()
	if err != nil || alloyInstance.State != "READY" || alloyInstance.IpAddress != "127.0.0.1" {
		return fmt.Errorf("AlloyDB primary backend state=%q ip=%q: %w", valueAlloyState(alloyInstance), valueAlloyIP(alloyInstance), err)
	}

	tenant, err := clients.identity.Projects.Tenants.Create("projects/"+cfg.project,
		&identitytoolkit.GoogleCloudIdentitytoolkitAdminV2Tenant{DisplayName: "Phase 20 generated client"}).
		Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Identity Platform tenant: %w", err)
	}

	fileOp, err := clients.file.Projects.Locations.Instances.Create(parent, &file.Instance{
		Tier:       "BASIC_HDD",
		FileShares: []*file.FileShareConfig{{Name: "share", CapacityGb: 1024}},
		Networks:   []*file.NetworkConfig{{Network: "default", Modes: []string{"MODE_IPV4"}}},
	}).InstanceId(cfg.filestoreInstanceID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Filestore instance/share: %w", err)
	}
	if _, err := waitFileOperation(ctx, clients.file, fileOp.Name); err != nil {
		return err
	}

	redisOp, err := clients.redis.Projects.Locations.Instances.Create(parent, &redis.Instance{
		Tier: "BASIC", MemorySizeGb: 1, RedisVersion: "REDIS_7_2",
	}).InstanceId(cfg.redisInstanceID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Redis-domain Valkey instance: %w", err)
	}
	if _, err := waitRedisOperation(ctx, clients.redis, redisOp.Name); err != nil {
		return err
	}

	job, err := clients.transfer.TransferJobs.Create(&storagetransfer.TransferJob{
		ProjectId: cfg.project, Status: "ENABLED", Description: "Phase 20 generated-client GCS transfer",
		TransferSpec: &storagetransfer.TransferSpec{
			GcsDataSource: &storagetransfer.GcsData{BucketName: cfg.sourceBucket, Path: "phase20/"},
			GcsDataSink:   &storagetransfer.GcsData{BucketName: cfg.sinkBucket, Path: "copied/"},
		},
	}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create Storage Transfer job: %w", err)
	}
	transferOp, err := clients.transfer.TransferJobs.Run(job.Name,
		&storagetransfer.RunTransferJobRequest{ProjectId: cfg.project}).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("run bounded GCS-to-GCS transfer: %w", err)
	}
	if !transferOp.Done {
		return errors.New("Storage Transfer operation did not complete")
	}
	if transferOp.Error != nil {
		return fmt.Errorf("Storage Transfer operation failed: %s", transferOp.Error.Message)
	}
	if err := verifyObject(ctx, clients.storage, cfg.sinkBucket, "copied/source.txt"); err != nil {
		return err
	}

	record := evidence{
		Version: evidenceVersion, Project: cfg.project, Location: cfg.location,
		ClusterName: clusterName, AlloyDBInstanceName: alloyInstanceName, TenantName: tenant.Name,
		FilestoreInstanceName: filestoreName, RedisInstanceName: redisName,
		TransferJobName: job.Name, TransferOperationName: transferOp.Name,
		TransferRunStatus: "SUCCEEDED",
		SourceBucket:      cfg.sourceBucket, SinkBucket: cfg.sinkBucket, ObjectName: transferObjectName,
		ObjectSHA256: transferObjectSHA256, PostgresImage: cfg.postgresImage, ValkeyImage: cfg.valkeyImage,
	}
	if err := writeEvidence(cfg.evidencePath, record); err != nil {
		return err
	}
	fmt.Println("Phase 20 generated clients created five bounded service slices; Storage Transfer run=SUCCEEDED")
	return nil
}

func verifyRestart(ctx context.Context, clients *generatedClients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	alloyInstance, err := clients.alloy.Projects.Locations.Clusters.Instances.Get(record.AlloyDBInstanceName).Context(ctx).Do()
	if err != nil || alloyInstance.State != "READY" || alloyInstance.IpAddress != "127.0.0.1" {
		return fmt.Errorf("restart AlloyDB instance state=%q ip=%q: %w", valueAlloyState(alloyInstance), valueAlloyIP(alloyInstance), err)
	}
	if _, err := clients.identity.Projects.Tenants.Get(record.TenantName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("restart Identity Platform tenant: %w", err)
	}
	if _, err := clients.file.Projects.Locations.Instances.Get(record.FilestoreInstanceName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("restart Filestore instance: %w", err)
	}
	redisInstance, err := clients.redis.Projects.Locations.Instances.Get(record.RedisInstanceName).Context(ctx).Do()
	if err != nil || redisInstance.State != "READY" || redisInstance.Host != "127.0.0.1" || redisInstance.Port == 0 {
		return fmt.Errorf("restart Redis-domain Valkey state=%q host=%q port=%d: %w",
			valueRedisState(redisInstance), valueRedisHost(redisInstance), valueRedisPort(redisInstance), err)
	}
	if _, err := clients.transfer.TransferJobs.Get(record.TransferJobName, cfg.project).Context(ctx).Do(); err != nil {
		return fmt.Errorf("restart Storage Transfer job: %w", err)
	}
	if err := verifyObject(ctx, clients.storage, record.SourceBucket, record.ObjectName); err != nil {
		return err
	}
	if err := verifyObject(ctx, clients.storage, record.SinkBucket, "copied/source.txt"); err != nil {
		return err
	}
	fmt.Println("Phase 20 generated-client restart verification passed")
	return nil
}

func deleteAndVerify(ctx context.Context, clients *generatedClients, cfg config) error {
	record, err := readEvidence(cfg.evidencePath, cfg)
	if err != nil {
		return err
	}
	if _, err := clients.transfer.TransferJobs.Patch(record.TransferJobName, &storagetransfer.UpdateTransferJobRequest{
		ProjectId: cfg.project, TransferJob: &storagetransfer.TransferJob{Status: "DELETED"},
		UpdateTransferJobFieldMask: "status",
	}).Context(ctx).Do(); err != nil {
		return fmt.Errorf("soft-delete Storage Transfer job: %w", err)
	}
	list, err := clients.transfer.TransferJobs.List(fmt.Sprintf(`{"projectId":%q}`, cfg.project)).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("list after Storage Transfer soft delete: %w", err)
	}
	for _, job := range list.TransferJobs {
		if job.Name == record.TransferJobName {
			return errors.New("soft-deleted Storage Transfer job remained listed")
		}
	}

	redisOp, err := clients.redis.Projects.Locations.Instances.Delete(record.RedisInstanceName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("delete Redis-domain Valkey instance: %w", err)
	}
	if _, err := waitRedisOperation(ctx, clients.redis, redisOp.Name); err != nil {
		return err
	}
	if err := expectGoogleStatus(getRedis(ctx, clients.redis, record.RedisInstanceName), 404, "NOT_FOUND"); err != nil {
		return fmt.Errorf("verify deleted Redis instance: %w", err)
	}

	if _, err := clients.file.Projects.Locations.Instances.Delete(record.FilestoreInstanceName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete Filestore instance: %w", err)
	}
	if err := expectGoogleStatus(getFile(ctx, clients.file, record.FilestoreInstanceName), 404, "NOT_FOUND"); err != nil {
		return fmt.Errorf("verify deleted Filestore instance: %w", err)
	}
	if _, err := clients.identity.Projects.Tenants.Delete(record.TenantName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete Identity Platform tenant: %w", err)
	}
	if err := expectGoogleStatus(getTenant(ctx, clients.identity, record.TenantName), 404, "NOT_FOUND"); err != nil {
		return fmt.Errorf("verify deleted Identity Platform tenant: %w", err)
	}
	if _, err := clients.alloy.Projects.Locations.Clusters.Instances.Delete(record.AlloyDBInstanceName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete AlloyDB primary instance: %w", err)
	}
	if err := expectGoogleStatus(getAlloyInstance(ctx, clients.alloy, record.AlloyDBInstanceName), 404, "NOT_FOUND"); err != nil {
		return fmt.Errorf("verify deleted AlloyDB instance: %w", err)
	}
	if _, err := clients.alloy.Projects.Locations.Clusters.Delete(record.ClusterName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete AlloyDB cluster: %w", err)
	}
	if err := expectGoogleStatus(getAlloyCluster(ctx, clients.alloy, record.ClusterName), 404, "NOT_FOUND"); err != nil {
		return fmt.Errorf("verify deleted AlloyDB cluster: %w", err)
	}

	if err := clients.storage.Objects.Delete(record.SourceBucket, record.ObjectName).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete source object: %w", err)
	}
	if err := clients.storage.Objects.Delete(record.SinkBucket, "copied/source.txt").Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete sink object: %w", err)
	}
	if err := clients.storage.Buckets.Delete(record.SourceBucket).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete source bucket: %w", err)
	}
	if err := clients.storage.Buckets.Delete(record.SinkBucket).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete sink bucket: %w", err)
	}
	fmt.Println("Phase 20 generated delete/404 and soft-delete/list-absence verification passed")
	return nil
}

func waitFileOperation(ctx context.Context, client *file.Service, name string) (*file.Operation, error) {
	for {
		operation, err := client.Projects.Locations.Operations.Get(name).Context(ctx).Do()
		if err != nil {
			return nil, fmt.Errorf("poll Filestore operation: %w", err)
		}
		if operation.Done {
			if operation.Error != nil {
				return nil, fmt.Errorf("Filestore operation failed: %s", operation.Error.Message)
			}
			return operation, nil
		}
		if err := waitTick(ctx); err != nil {
			return nil, err
		}
	}
}

func waitRedisOperation(ctx context.Context, client *redis.Service, name string) (*redis.Operation, error) {
	for {
		operation, err := client.Projects.Locations.Operations.Get(name).Context(ctx).Do()
		if err != nil {
			return nil, fmt.Errorf("poll Redis operation: %w", err)
		}
		if operation.Done {
			if operation.Error != nil {
				return nil, fmt.Errorf("Redis operation failed: %s", operation.Error.Message)
			}
			return operation, nil
		}
		if err := waitTick(ctx); err != nil {
			return nil, err
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

func verifyObject(ctx context.Context, client *storage.Service, bucket, object string) error {
	response, err := client.Objects.Get(bucket, object).Context(ctx).Download()
	if err != nil {
		return fmt.Errorf("download transferred object: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read transferred object: %w", err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != transferObjectSHA256 {
		return fmt.Errorf("transferred object digest mismatch")
	}
	return nil
}

func expectGoogleStatus(err error, code int, status string) error {
	if err == nil {
		return fmt.Errorf("expected HTTP %d %s, got success", code, status)
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		return fmt.Errorf("expected googleapi.Error, got %T: %w", err, err)
	}
	if apiErr.Code != code {
		return fmt.Errorf("HTTP code=%d want=%d body=%s", apiErr.Code, code, apiErr.Body)
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

func getRedis(ctx context.Context, client *redis.Service, name string) error {
	_, err := client.Projects.Locations.Instances.Get(name).Context(ctx).Do()
	return err
}

func getFile(ctx context.Context, client *file.Service, name string) error {
	_, err := client.Projects.Locations.Instances.Get(name).Context(ctx).Do()
	return err
}

func getTenant(ctx context.Context, client *identitytoolkit.Service, name string) error {
	_, err := client.Projects.Tenants.Get(name).Context(ctx).Do()
	return err
}

func getAlloyInstance(ctx context.Context, client *alloydb.Service, name string) error {
	_, err := client.Projects.Locations.Clusters.Instances.Get(name).Context(ctx).Do()
	return err
}

func getAlloyCluster(ctx context.Context, client *alloydb.Service, name string) error {
	_, err := client.Projects.Locations.Clusters.Get(name).Context(ctx).Do()
	return err
}

func writeEvidence(path string, record evidence) error {
	if err := validateEvidence(record); err != nil {
		return err
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".phase20-evidence-*.tmp")
	if err != nil {
		return fmt.Errorf("create evidence temp file: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
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
	if err := os.Rename(tempName, path); err != nil {
		return fmt.Errorf("publish evidence: %w", err)
	}
	return nil
}

func readEvidence(path string, cfg config) (evidence, error) {
	fileHandle, err := os.Open(path)
	if err != nil {
		return evidence{}, fmt.Errorf("read evidence: %w", err)
	}
	defer fileHandle.Close()
	data, err := io.ReadAll(io.LimitReader(fileHandle, maxEvidenceBytes+1))
	if err != nil {
		return evidence{}, err
	}
	if len(data) > maxEvidenceBytes {
		return evidence{}, fmt.Errorf("evidence exceeds %d-byte limit", maxEvidenceBytes)
	}
	var record evidence
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return evidence{}, fmt.Errorf("decode evidence: %w", err)
	}
	if err := validateEvidence(record); err != nil {
		return evidence{}, err
	}
	if record.Project != cfg.project || record.Location != cfg.location ||
		record.SourceBucket != cfg.sourceBucket || record.SinkBucket != cfg.sinkBucket ||
		record.PostgresImage != cfg.postgresImage || record.ValkeyImage != cfg.valkeyImage {
		return evidence{}, errors.New("evidence identifiers do not match requested smoke configuration")
	}
	return record, nil
}

func validateEvidence(record evidence) error {
	if record.Version != evidenceVersion {
		return fmt.Errorf("evidence version=%d want=%d", record.Version, evidenceVersion)
	}
	required := map[string]string{
		"project": record.Project, "location": record.Location, "cluster": record.ClusterName,
		"AlloyDB instance": record.AlloyDBInstanceName, "tenant": record.TenantName,
		"Filestore instance": record.FilestoreInstanceName, "Redis instance": record.RedisInstanceName,
		"transfer job":  record.TransferJobName,
		"source bucket": record.SourceBucket, "sink bucket": record.SinkBucket, "object": record.ObjectName,
	}
	for name, value := range required {
		if value == "" {
			return fmt.Errorf("evidence %s is empty", name)
		}
	}
	if record.ObjectName != transferObjectName || record.ObjectSHA256 != transferObjectSHA256 {
		return errors.New("evidence object identity or digest is invalid")
	}
	if record.TransferRunStatus != "SUCCEEDED" {
		return errors.New("evidence Storage Transfer run must have succeeded")
	}
	if record.TransferOperationName == "" {
		return errors.New("successful Storage Transfer evidence requires an operation")
	}
	if !digestImagePattern.MatchString(record.PostgresImage) || !digestImagePattern.MatchString(record.ValkeyImage) {
		return errors.New("evidence backend images must be pinned by sha256 digest")
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

func valueAlloyState(instance *alloydb.Instance) string {
	if instance == nil {
		return ""
	}
	return instance.State
}

func valueAlloyIP(instance *alloydb.Instance) string {
	if instance == nil {
		return ""
	}
	return instance.IpAddress
}

func valueRedisState(instance *redis.Instance) string {
	if instance == nil {
		return ""
	}
	return instance.State
}

func valueRedisHost(instance *redis.Instance) string {
	if instance == nil {
		return ""
	}
	return instance.Host
}

func valueRedisPort(instance *redis.Instance) int64 {
	if instance == nil {
		return 0
	}
	return instance.Port
}
