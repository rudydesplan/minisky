package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestInspectCapturesAcceptsSanitizedBoundedPayload(t *testing.T) {
	dir := t.TempDir()
	request := baseRequest()
	body, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "otlp-0001.pb"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	forbiddenPath := filepath.Join(dir, "forbidden.json")
	forbidden := map[string]string{
		"project ID":  "raw-project-123",
		"resource ID": "raw-resource-456",
		"query value": "raw-query-secret",
	}
	encoded, err := json.Marshal(forbidden)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(forbiddenPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	stats, forbiddenCount, err := inspectCaptures(dir, forbiddenPath, limits{
		maxFiles:         2,
		maxBodyBytes:     64 << 10,
		maxTotalBytes:    64 << 10,
		requiredTraceID:  append(bytes.Repeat([]byte{0}, 15), 1),
		requiredServices: map[string]struct{}{"compute.googleapis.com": {}},
		resourceService:  "minisky",
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.files != 1 || stats.spans != 1 || !stats.requiredTrace || forbiddenCount != len(forbidden) {
		t.Fatalf("stats = %#v, forbidden count = %d", stats, forbiddenCount)
	}
}

func TestInspectorRejectsSensitiveValuesInEveryTelemetrySurface(t *testing.T) {
	const canary = "raw-sensitive-canary"
	tests := []struct {
		name     string
		category string
		mutate   func(*collectortracev1.ExportTraceServiceRequest)
	}{
		{
			name:     "resource attribute",
			category: "resource attribute",
			mutate: func(request *collectortracev1.ExportTraceServiceRequest) {
				request.ResourceSpans[0].Resource.Attributes[0].Value = stringValue(canary)
			},
		},
		{
			name:     "span name",
			category: "span name",
			mutate: func(request *collectortracev1.ExportTraceServiceRequest) {
				request.ResourceSpans[0].ScopeSpans[0].Spans[0].Name = canary
			},
		},
		{
			name:     "span attribute",
			category: "span attribute",
			mutate: func(request *collectortracev1.ExportTraceServiceRequest) {
				request.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[0].Value = stringValue(canary)
			},
		},
		{
			name:     "event",
			category: "span event name",
			mutate: func(request *collectortracev1.ExportTraceServiceRequest) {
				request.ResourceSpans[0].ScopeSpans[0].Spans[0].Events[0].Name = canary
			},
		},
		{
			name:     "event attribute",
			category: "span event attribute",
			mutate: func(request *collectortracev1.ExportTraceServiceRequest) {
				request.ResourceSpans[0].ScopeSpans[0].Spans[0].Events[0].Attributes[0].Value = stringValue(canary)
			},
		},
		{
			name:     "link",
			category: "span link attribute",
			mutate: func(request *collectortracev1.ExportTraceServiceRequest) {
				request.ResourceSpans[0].ScopeSpans[0].Spans[0].Links[0].Attributes[0].Value = stringValue(canary)
			},
		},
		{
			name:     "link trace state",
			category: "span link trace state",
			mutate: func(request *collectortracev1.ExportTraceServiceRequest) {
				request.ResourceSpans[0].ScopeSpans[0].Spans[0].Links[0].TraceState = canary
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := baseRequest()
			test.mutate(request)
			checker := &inspector{forbidden: map[string][]byte{"test": []byte(canary)}}
			err := checker.inspectRequest(request)
			if err == nil || !strings.Contains(err.Error(), test.category) {
				t.Fatalf("error = %v, want category %q", err, test.category)
			}
		})
	}
}

func TestInspectCapturesRejectsForbiddenRawPayload(t *testing.T) {
	const canary = "raw-full-url-canary"
	dir := t.TempDir()
	request := baseRequest()
	request.ResourceSpans[0].ScopeSpans[0].Spans[0].Attributes[0].Value = stringValue(canary)
	body, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "otlp-0001.pb"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	forbiddenPath := filepath.Join(dir, "forbidden.json")
	if err := os.WriteFile(
		forbiddenPath,
		[]byte(`{"full URL":"`+canary+`"}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	_, _, err = inspectCaptures(dir, forbiddenPath, limits{
		maxFiles:      1,
		maxBodyBytes:  64 << 10,
		maxTotalBytes: 64 << 10,
	})
	if err == nil || !strings.Contains(err.Error(), "raw protobuf payload") {
		t.Fatalf("error = %v, want raw payload rejection", err)
	}
}

func TestInspectorRejectsUnapprovedServiceIdentities(t *testing.T) {
	request := baseRequest()
	span := request.ResourceSpans[0].ScopeSpans[0].Spans[0]
	for _, attribute := range span.Attributes {
		if attribute.Key == "minisky.service" {
			attribute.Value = stringValue("unknown-host.example")
		}
	}
	checker := &inspector{
		forbidden:        map[string][]byte{"canary": []byte("not-present")},
		requiredServices: map[string]struct{}{"compute.googleapis.com": {}},
		seenServices:     make(map[string]struct{}),
		resourceService:  "minisky",
	}
	if err := checker.inspectRequest(request); err == nil ||
		!strings.Contains(err.Error(), "unapproved minisky.service") {
		t.Fatalf("error = %v, want unapproved span service rejection", err)
	}

	request = baseRequest()
	request.ResourceSpans[0].Resource.Attributes[0].Value = stringValue("unknown-resource")
	checker = &inspector{
		forbidden:        map[string][]byte{"canary": []byte("not-present")},
		requiredServices: map[string]struct{}{"compute.googleapis.com": {}},
		seenServices:     make(map[string]struct{}),
		resourceService:  "minisky",
	}
	if err := checker.inspectRequest(request); err == nil ||
		!strings.Contains(err.Error(), "unapproved service.name") {
		t.Fatalf("error = %v, want unapproved resource service rejection", err)
	}
}

func baseRequest() *collectortracev1.ExportTraceServiceRequest {
	return &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{{
			Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{{
					Key:   "service.name",
					Value: stringValue("minisky"),
				}},
			},
			ScopeSpans: []*tracev1.ScopeSpans{{
				Scope: &commonv1.InstrumentationScope{Name: "minisky.gateway"},
				Spans: []*tracev1.Span{{
					TraceId: append(bytes.Repeat([]byte{0}, 15), 1),
					Name:    "GET /v3/projects/{id}",
					Attributes: []*commonv1.KeyValue{{
						Key:   "http.request.method",
						Value: stringValue("GET"),
					}, {
						Key:   "minisky.service",
						Value: stringValue("compute.googleapis.com"),
					}},
					Events: []*tracev1.Span_Event{{
						Name: "bounded-event",
						Attributes: []*commonv1.KeyValue{{
							Key:   "event.type",
							Value: stringValue("safe"),
						}},
					}},
					Links: []*tracev1.Span_Link{{
						Attributes: []*commonv1.KeyValue{{
							Key:   "link.type",
							Value: stringValue("safe"),
						}},
					}},
				}},
			}},
		}},
	}
}

func stringValue(value string) *commonv1.AnyValue {
	return &commonv1.AnyValue{
		Value: &commonv1.AnyValue_StringValue{StringValue: value},
	}
}
