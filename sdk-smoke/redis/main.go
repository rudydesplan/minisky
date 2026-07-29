package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	redis "google.golang.org/api/redis/v1"
)

var instanceIDPattern = regexp.MustCompile(`^[a-z](?:[-a-z0-9]{0,38}[a-z0-9])?$`)

type config struct {
	mode, endpoint, project, location, instanceID string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Redis generated-client smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	service, err := newRedisService(ctx, cfg.endpoint)
	if err != nil {
		return err
	}
	switch cfg.mode {
	case "create":
		return createInstance(ctx, service, cfg)
	case "verify":
		return verifyInstance(ctx, service, cfg)
	case "delete":
		return deleteInstance(ctx, service, cfg)
	default:
		return fmt.Errorf("unsupported MINISKY_REDIS_MODE %q", cfg.mode)
	}
}

func configFromEnv() (config, error) {
	cfg := config{
		mode:       env("MINISKY_REDIS_MODE", "verify"),
		endpoint:   strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/"),
		project:    env("MINISKY_PROJECT_ID", "local-dev-project"),
		location:   env("MINISKY_REDIS_LOCATION", "us-central1"),
		instanceID: env("MINISKY_REDIS_INSTANCE_ID", "minisky-redis"),
	}
	if err := validateLoopbackEndpoint(cfg.endpoint); err != nil {
		return config{}, err
	}
	if !instanceIDPattern.MatchString(cfg.instanceID) {
		return config{}, fmt.Errorf("MINISKY_REDIS_INSTANCE_ID %q is invalid", cfg.instanceID)
	}
	if cfg.project == "" || strings.Contains(cfg.project, "/") ||
		cfg.location == "" || strings.Contains(cfg.location, "/") {
		return config{}, errors.New("project and location must be non-empty path segments")
	}
	return cfg, nil
}

func validateLoopbackEndpoint(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse MINISKY_ENDPOINT: %w", err)
	}
	if parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Port() == "" {
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

func newRedisService(ctx context.Context, endpoint string) (*redis.Service, error) {
	return redis.NewService(
		ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(strings.TrimRight(endpoint, "/")+"/_minisky/redis.googleapis.com/"),
	)
}

func createInstance(ctx context.Context, service *redis.Service, cfg config) error {
	operation, err := service.Projects.Locations.Instances.Create(instanceParent(cfg), &redis.Instance{
		Tier:                  "BASIC",
		MemorySizeGb:          1,
		RedisVersion:          "REDIS_7_2",
		ConnectMode:           "DIRECT_PEERING",
		TransitEncryptionMode: "DISABLED",
	}).InstanceId(cfg.instanceID).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("create instance: %w", err)
	}
	if err := waitOperation(ctx, service, operation); err != nil {
		return fmt.Errorf("wait for create: %w", err)
	}
	if err := verifyInstance(ctx, service, cfg); err != nil {
		return err
	}
	fmt.Printf("Redis generated-client create/read/list passed: %s\n", instanceName(cfg))
	return nil
}

func verifyInstance(ctx context.Context, service *redis.Service, cfg config) error {
	name := instanceName(cfg)
	instance, err := service.Projects.Locations.Instances.Get(name).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get instance: %w", err)
	}
	if instance.Name != name || instance.State != "READY" || instance.Tier != "BASIC" ||
		instance.MemorySizeGb != 1 || instance.RedisVersion != "REDIS_7_2" ||
		instance.Host == "" || instance.Port < 1 || instance.Port > 65535 {
		return fmt.Errorf("instance response does not match bounded Redis lifecycle: %#v", instance)
	}
	list, err := service.Projects.Locations.Instances.List(instanceParent(cfg)).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("list instances: %w", err)
	}
	for _, candidate := range list.Instances {
		if candidate != nil && candidate.Name == name {
			fmt.Printf("Redis generated-client read/list passed: %s\n", name)
			return nil
		}
	}
	return fmt.Errorf("list response omitted %s", name)
}

func deleteInstance(ctx context.Context, service *redis.Service, cfg config) error {
	name := instanceName(cfg)
	operation, err := service.Projects.Locations.Instances.Delete(name).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("delete instance: %w", err)
	}
	if err := waitOperation(ctx, service, operation); err != nil {
		return fmt.Errorf("wait for delete: %w", err)
	}
	_, err = service.Projects.Locations.Instances.Get(name).Context(ctx).Do()
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusNotFound {
		return fmt.Errorf("get after delete = %v, want HTTP 404", err)
	}
	fmt.Printf("Redis generated-client delete passed: %s\n", name)
	return nil
}

func waitOperation(ctx context.Context, service *redis.Service, operation *redis.Operation) error {
	if operation == nil || operation.Name == "" {
		return errors.New("operation response has no name")
	}
	for {
		current, err := service.Projects.Locations.Operations.Get(operation.Name).Context(ctx).Do()
		if err != nil {
			return err
		}
		if current.Done {
			if current.Error != nil {
				return fmt.Errorf("operation failed: %s", current.Error.Message)
			}
			return nil
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func instanceParent(cfg config) string {
	return "projects/" + cfg.project + "/locations/" + cfg.location
}

func instanceName(cfg config) string {
	return instanceParent(cfg) + "/instances/" + cfg.instanceID
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
