package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/api/googleapi"
	logging "google.golang.org/api/logging/v2"
	"google.golang.org/api/option"
)

const (
	logID    = "phase16-app"
	sinkName = "phase16-errors"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "phase 16 Logging smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	mode := strings.TrimSpace(os.Getenv("MINISKY_PHASE16_LOGGING_MODE"))
	gateway := strings.TrimRight(strings.TrimSpace(os.Getenv("MINISKY_ENDPOINT")), "/")
	project := env("MINISKY_PROJECT_ID", "phase16-project")
	otherProject := env("MINISKY_PHASE16_LOGGING_OTHER_PROJECT", project+"-other")
	if gateway == "" {
		return errors.New("MINISKY_ENDPOINT is required")
	}
	for name, value := range map[string]string{"project": project, "other project": otherProject} {
		if value == "" || strings.Contains(value, "/") {
			return fmt.Errorf("%s must be a nonempty path segment", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch mode {
	case "seed":
		return seed(ctx, gateway, project, otherProject)
	case "verify":
		return verify(ctx, gateway, project)
	case "cleanup":
		return cleanup(ctx, gateway, project)
	case "verify-cleanup":
		return verifyCleanup(ctx, gateway, project)
	default:
		return fmt.Errorf("MINISKY_PHASE16_LOGGING_MODE must be seed, verify, cleanup, or verify-cleanup")
	}
}

func loggingService(ctx context.Context, gateway string) (*logging.Service, error) {
	return logging.NewService(ctx,
		option.WithoutAuthentication(),
		option.WithEndpoint(strings.TrimRight(gateway, "/")+"/_minisky/logging/"),
	)
}

func seed(ctx context.Context, gateway, project, otherProject string) error {
	service, err := loggingService(ctx, gateway)
	if err != nil {
		return fmt.Errorf("create Logging client: %w", err)
	}
	parent := "projects/" + project
	sink := &logging.LogSink{
		Name:        sinkName,
		Destination: "file://phase16-errors",
		Filter:      "severity>=EMERGENCY",
		Description: "Phase 16 restart SDK smoke",
	}
	if _, err := service.Projects.Sinks.Create(parent, sink).Context(ctx).Do(); err != nil {
		return fmt.Errorf("create sink with generated client: %w", err)
	}
	request := &logging.WriteLogEntriesRequest{
		LogName: "projects/" + project + "/logs/" + logID,
		Resource: &logging.MonitoredResource{
			Type:   "global",
			Labels: map[string]string{"project_id": project},
		},
		Labels: map[string]string{"phase": "16", "shared": "default"},
		Entries: []*logging.LogEntry{
			{
				InsertId:    "phase16-info",
				Timestamp:   "2026-07-26T08:00:00Z",
				Severity:    "INFO",
				TextPayload: "phase16 info",
			},
			{
				InsertId:    "phase16-error",
				Timestamp:   "2026-07-26T08:01:00Z",
				Severity:    "ERROR",
				TextPayload: "phase16 error",
				Labels:      map[string]string{"entry": "error", "shared": "entry"},
			},
			{
				InsertId:    "phase16-other-error",
				Timestamp:   "2026-07-26T08:02:00Z",
				Severity:    "ERROR",
				TextPayload: "phase16 other project",
				LogName:     "projects/" + otherProject + "/logs/" + logID,
			},
		},
	}
	if _, err := service.Entries.Write(request).Context(ctx).Do(); err != nil {
		return fmt.Errorf("write entries with generated client: %w", err)
	}
	fmt.Printf("seeded Logging entries=3 sink=%s\n", sinkName)
	return nil
}

func verify(ctx context.Context, gateway, project string) error {
	service, err := loggingService(ctx, gateway)
	if err != nil {
		return fmt.Errorf("create Logging client: %w", err)
	}
	entry, err := persistedErrorEntry(ctx, service, project)
	if err != nil {
		return err
	}
	fullSinkName := "projects/" + project + "/sinks/" + sinkName
	sink, err := service.Projects.Sinks.Get(fullSinkName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("get sink with generated client: %w", err)
	}
	if err := validateSink(sink); err != nil {
		return err
	}
	sinks, err := service.Projects.Sinks.List("projects/" + project).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("list sinks with generated client: %w", err)
	}
	if len(sinks.Sinks) != 1 {
		return fmt.Errorf("listed sinks=%d want=1", len(sinks.Sinks))
	}
	if err := validateSink(sinks.Sinks[0]); err != nil {
		return err
	}
	fmt.Printf("verified persisted Logging entry=%s sink=%s\n", entry.InsertId, sinkName)
	return nil
}

func persistedErrorEntry(ctx context.Context, service *logging.Service, project string) (*logging.LogEntry, error) {
	logName := "projects/" + project + "/logs/" + logID
	response, err := service.Entries.List(&logging.ListLogEntriesRequest{
		ResourceNames: []string{"projects/" + project},
		Filter:        `severity>=ERROR AND logName="` + logName + `" AND resource.type="global"`,
		OrderBy:       "timestamp asc",
		PageSize:      10,
	}).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("list entries with generated client: %w", err)
	}
	if len(response.Entries) != 1 {
		return nil, fmt.Errorf("listed entries=%d want=1", len(response.Entries))
	}
	entry := response.Entries[0]
	if entry.InsertId != "phase16-error" || entry.Timestamp != "2026-07-26T08:01:00Z" ||
		entry.Severity != "ERROR" || entry.TextPayload != "phase16 error" || entry.LogName != logName {
		return nil, fmt.Errorf("unexpected persisted entry: %#v", entry)
	}
	if entry.Resource == nil || entry.Resource.Type != "global" ||
		entry.Resource.Labels["project_id"] != project ||
		entry.Labels["phase"] != "16" || entry.Labels["entry"] != "error" ||
		entry.Labels["shared"] != "entry" {
		return nil, fmt.Errorf("unexpected inherited entry fields: %#v", entry)
	}
	return entry, nil
}

func cleanup(ctx context.Context, gateway, project string) error {
	service, err := loggingService(ctx, gateway)
	if err != nil {
		return fmt.Errorf("create Logging client: %w", err)
	}
	name := "projects/" + project + "/sinks/" + sinkName
	if _, err := service.Projects.Sinks.Delete(name).Context(ctx).Do(); err != nil {
		return fmt.Errorf("delete sink with generated client: %w", err)
	}
	if err := confirmSinkMissing(ctx, service, name); err != nil {
		return err
	}
	fmt.Printf("deleted Logging sink=%s and confirmed 404\n", sinkName)
	return nil
}

func verifyCleanup(ctx context.Context, gateway, project string) error {
	service, err := loggingService(ctx, gateway)
	if err != nil {
		return fmt.Errorf("create Logging client: %w", err)
	}
	entry, err := persistedErrorEntry(ctx, service, project)
	if err != nil {
		return err
	}
	name := "projects/" + project + "/sinks/" + sinkName
	if err := confirmSinkMissing(ctx, service, name); err != nil {
		return err
	}
	fmt.Printf("verified cleanup persisted entry=%s and sink remains 404\n", entry.InsertId)
	return nil
}

func confirmSinkMissing(ctx context.Context, service *logging.Service, name string) error {
	if _, err := service.Projects.Sinks.Get(name).Context(ctx).Do(); err == nil {
		return errors.New("deleted sink still exists")
	} else {
		var apiErr *googleapi.Error
		if !errors.As(err, &apiErr) || apiErr.Code != 404 {
			return fmt.Errorf("get deleted sink: %w", err)
		}
	}
	return nil
}

func validateSink(sink *logging.LogSink) error {
	if sink == nil || sink.Name != sinkName || sink.Destination != "file://phase16-errors" ||
		sink.Filter != "severity>=EMERGENCY" ||
		sink.Description != "Phase 16 restart SDK smoke" {
		return fmt.Errorf("unexpected sink metadata: %#v", sink)
	}
	return nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
