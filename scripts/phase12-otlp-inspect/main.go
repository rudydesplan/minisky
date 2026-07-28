package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

type limits struct {
	maxFiles         int
	maxBodyBytes     int64
	maxTotalBytes    int64
	requiredTraceID  []byte
	requiredServices map[string]struct{}
	resourceService  string
}

type inspectionStats struct {
	files              int
	bytes              int64
	spans              int
	resourceAttributes int
	spanAttributes     int
	events             int
	links              int
	requiredTrace      bool
	resourceService    bool
}

type inspector struct {
	forbidden        map[string][]byte
	requiredTraceID  []byte
	requiredServices map[string]struct{}
	seenServices     map[string]struct{}
	resourceService  string
	stats            inspectionStats
}

func main() {
	captureDir := flag.String("capture-dir", "", "directory containing bounded OTLP protobuf captures")
	forbiddenFile := flag.String("forbidden-file", "", "JSON object mapping canary labels to forbidden values")
	maxFiles := flag.Int("max-files", 32, "maximum capture files to inspect")
	maxBodyBytes := flag.Int64("max-body-bytes", 1<<20, "maximum bytes per capture")
	maxTotalBytes := flag.Int64("max-total-bytes", 4<<20, "maximum aggregate capture bytes")
	requiredTraceID := flag.String("required-trace-id", "", "hex trace ID that must occur in the captured payload")
	requiredServices := make(map[string]struct{})
	flag.Func("required-service", "bounded minisky.service value that must occur; may be repeated", func(value string) error {
		if strings.TrimSpace(value) == "" || len(value) > 253 {
			return errors.New("required-service must be a bounded non-empty value")
		}
		requiredServices[value] = struct{}{}
		return nil
	})
	resourceService := flag.String("resource-service", "", "required service.name resource identity")
	flag.Parse()

	if *captureDir == "" || *forbiddenFile == "" {
		fmt.Fprintln(os.Stderr, "capture-dir and forbidden-file are required")
		os.Exit(2)
	}
	var requiredTraceBytes []byte
	if *requiredTraceID != "" {
		var err error
		requiredTraceBytes, err = hex.DecodeString(*requiredTraceID)
		if err != nil || len(requiredTraceBytes) != 16 {
			fmt.Fprintln(os.Stderr, "required-trace-id must be 32 hexadecimal characters")
			os.Exit(2)
		}
	}
	stats, forbiddenCount, err := inspectCaptures(*captureDir, *forbiddenFile, limits{
		maxFiles:         *maxFiles,
		maxBodyBytes:     *maxBodyBytes,
		maxTotalBytes:    *maxTotalBytes,
		requiredTraceID:  requiredTraceBytes,
		requiredServices: requiredServices,
		resourceService:  *resourceService,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "OTLP payload inspection failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf(
		"OTLP payload inspection passed: files=%d bytes=%d spans=%d resource_attributes=%d span_attributes=%d events=%d links=%d forbidden_values=%d required_trace=%t required_services=%d resource_service=%t\n",
		stats.files,
		stats.bytes,
		stats.spans,
		stats.resourceAttributes,
		stats.spanAttributes,
		stats.events,
		stats.links,
		forbiddenCount,
		stats.requiredTrace,
		len(requiredServices),
		stats.resourceService,
	)
}

func inspectCaptures(captureDir, forbiddenFile string, bounds limits) (inspectionStats, int, error) {
	if bounds.maxFiles < 1 || bounds.maxBodyBytes < 1 || bounds.maxTotalBytes < 1 {
		return inspectionStats{}, 0, errors.New("inspection limits must be positive")
	}
	forbidden, err := loadForbidden(forbiddenFile)
	if err != nil {
		return inspectionStats{}, 0, err
	}
	entries, err := os.ReadDir(captureDir)
	if err != nil {
		return inspectionStats{}, 0, fmt.Errorf("read capture directory: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		if strings.HasPrefix(entry.Name(), "otlp-") && strings.HasSuffix(entry.Name(), ".pb") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return inspectionStats{}, len(forbidden), errors.New("no OTLP trace payloads were captured")
	}
	if len(names) > bounds.maxFiles {
		return inspectionStats{}, len(forbidden), errors.New("OTLP capture file limit exceeded")
	}

	checker := &inspector{
		forbidden:        forbidden,
		requiredTraceID:  bounds.requiredTraceID,
		requiredServices: bounds.requiredServices,
		seenServices:     make(map[string]struct{}),
		resourceService:  bounds.resourceService,
	}
	for _, name := range names {
		path := filepath.Join(captureDir, name)
		info, err := os.Stat(path)
		if err != nil {
			return inspectionStats{}, len(forbidden), fmt.Errorf("stat OTLP capture: %w", err)
		}
		if info.Size() > bounds.maxBodyBytes {
			return inspectionStats{}, len(forbidden), errors.New("OTLP capture body limit exceeded")
		}
		checker.stats.bytes += info.Size()
		if checker.stats.bytes > bounds.maxTotalBytes {
			return inspectionStats{}, len(forbidden), errors.New("OTLP aggregate capture limit exceeded")
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return inspectionStats{}, len(forbidden), fmt.Errorf("read OTLP capture: %w", err)
		}
		if err := checker.checkBytes("raw protobuf payload", body); err != nil {
			return inspectionStats{}, len(forbidden), err
		}
		var request collectortracev1.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			return inspectionStats{}, len(forbidden), errors.New("decode OTLP trace protobuf")
		}
		if err := checker.inspectRequest(&request); err != nil {
			return inspectionStats{}, len(forbidden), err
		}
		checker.stats.files++
	}
	if checker.stats.spans == 0 {
		return inspectionStats{}, len(forbidden), errors.New("captured OTLP payloads contained no spans")
	}
	if len(bounds.requiredTraceID) != 0 && !checker.stats.requiredTrace {
		return inspectionStats{}, len(forbidden), errors.New("required sensitive-probe trace was not exported")
	}
	if bounds.resourceService != "" && !checker.stats.resourceService {
		return inspectionStats{}, len(forbidden), errors.New("required service.name resource identity was not exported")
	}
	for service := range bounds.requiredServices {
		if _, ok := checker.seenServices[service]; !ok {
			return inspectionStats{}, len(forbidden), errors.New("required bounded minisky.service identity was not exported")
		}
	}
	return checker.stats, len(forbidden), nil
}

func loadForbidden(path string) (map[string][]byte, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read forbidden values: %w", err)
	}
	var values map[string]string
	if err := json.Unmarshal(content, &values); err != nil {
		return nil, errors.New("decode forbidden values JSON")
	}
	if len(values) == 0 {
		return nil, errors.New("forbidden values must not be empty")
	}
	result := make(map[string][]byte, len(values))
	for label, value := range values {
		if strings.TrimSpace(label) == "" || value == "" || len(value) > 4096 {
			return nil, errors.New("forbidden value entries must have bounded non-empty labels and values")
		}
		result[label] = []byte(value)
	}
	return result, nil
}

func (i *inspector) inspectRequest(request *collectortracev1.ExportTraceServiceRequest) error {
	for _, resourceSpans := range request.ResourceSpans {
		if err := i.checkString("resource schema URL", resourceSpans.SchemaUrl); err != nil {
			return err
		}
		if resourceSpans.Resource != nil {
			i.stats.resourceAttributes += len(resourceSpans.Resource.Attributes)
			if err := i.inspectResourceService(resourceSpans.Resource.Attributes); err != nil {
				return err
			}
			if err := i.inspectAttributes("resource attribute", resourceSpans.Resource.Attributes); err != nil {
				return err
			}
		}
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			if err := i.checkString("scope schema URL", scopeSpans.SchemaUrl); err != nil {
				return err
			}
			if scopeSpans.Scope != nil {
				if err := i.checkString("scope name", scopeSpans.Scope.Name); err != nil {
					return err
				}
				if err := i.checkString("scope version", scopeSpans.Scope.Version); err != nil {
					return err
				}
				if err := i.inspectAttributes("scope attribute", scopeSpans.Scope.Attributes); err != nil {
					return err
				}
			}
			for _, span := range scopeSpans.Spans {
				if err := i.inspectSpan(span); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (i *inspector) inspectSpan(span *tracev1.Span) error {
	i.stats.spans++
	if bytes.Equal(span.TraceId, i.requiredTraceID) {
		i.stats.requiredTrace = true
	}
	if err := i.checkString("span name", span.Name); err != nil {
		return err
	}
	if err := i.checkString("span trace state", span.TraceState); err != nil {
		return err
	}
	i.stats.spanAttributes += len(span.Attributes)
	if err := i.inspectSpanService(span.Attributes); err != nil {
		return err
	}
	if err := i.inspectAttributes("span attribute", span.Attributes); err != nil {
		return err
	}
	for _, event := range span.Events {
		i.stats.events++
		if err := i.checkString("span event name", event.Name); err != nil {
			return err
		}
		if err := i.inspectAttributes("span event attribute", event.Attributes); err != nil {
			return err
		}
	}
	for _, link := range span.Links {
		i.stats.links++
		if err := i.checkString("span link trace state", link.TraceState); err != nil {
			return err
		}
		if err := i.inspectAttributes("span link attribute", link.Attributes); err != nil {
			return err
		}
	}
	if span.Status != nil {
		if err := i.checkString("span status message", span.Status.Message); err != nil {
			return err
		}
	}
	return nil
}

func (i *inspector) inspectResourceService(attributes []*commonv1.KeyValue) error {
	for _, attribute := range attributes {
		if attribute == nil || attribute.Key != "service.name" {
			continue
		}
		if i.resourceService != "" && attribute.Value.GetStringValue() != i.resourceService {
			return errors.New("resource contained an unapproved service.name identity")
		}
		i.stats.resourceService = true
	}
	return nil
}

func (i *inspector) inspectSpanService(attributes []*commonv1.KeyValue) error {
	if len(i.requiredServices) == 0 {
		return nil
	}
	count := 0
	for _, attribute := range attributes {
		if attribute == nil || attribute.Key != "minisky.service" {
			continue
		}
		count++
		service := attribute.Value.GetStringValue()
		if _, ok := i.requiredServices[service]; !ok {
			return errors.New("span contained an unapproved minisky.service identity")
		}
		i.seenServices[service] = struct{}{}
	}
	if count != 1 {
		return errors.New("span did not contain exactly one bounded minisky.service identity")
	}
	return nil
}

func (i *inspector) inspectAttributes(category string, attributes []*commonv1.KeyValue) error {
	for _, attribute := range attributes {
		if attribute == nil {
			continue
		}
		if err := i.checkString(category+" key", attribute.Key); err != nil {
			return err
		}
		if err := i.inspectAnyValue(category+" value", attribute.Value); err != nil {
			return err
		}
	}
	return nil
}

func (i *inspector) inspectAnyValue(category string, value *commonv1.AnyValue) error {
	if value == nil {
		return nil
	}
	switch typed := value.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return i.checkString(category, typed.StringValue)
	case *commonv1.AnyValue_BytesValue:
		return i.checkBytes(category, typed.BytesValue)
	case *commonv1.AnyValue_ArrayValue:
		for _, child := range typed.ArrayValue.Values {
			if err := i.inspectAnyValue(category, child); err != nil {
				return err
			}
		}
	case *commonv1.AnyValue_KvlistValue:
		return i.inspectAttributes(category, typed.KvlistValue.Values)
	}
	return nil
}

func (i *inspector) checkString(category, value string) error {
	return i.checkBytes(category, []byte(value))
}

func (i *inspector) checkBytes(category string, value []byte) error {
	for label, forbidden := range i.forbidden {
		if bytes.Contains(value, forbidden) {
			return fmt.Errorf("%s contains forbidden %s canary", category, label)
		}
	}
	return nil
}
