package observability

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

type TelemetryConfig struct {
	Enabled        bool
	Endpoint       string
	ServiceVersion string
	ExportTimeout  time.Duration
	SamplingRatio  *float64
}

var (
	newTelemetryExporter = func(ctx context.Context, endpoint string, timeout time.Duration) (sdktrace.SpanExporter, error) {
		return otlptracehttp.New(
			ctx,
			otlptracehttp.WithEndpointURL(endpoint),
			otlptracehttp.WithTimeout(timeout),
		)
	}
	newTelemetryResource = func(ctx context.Context, serviceVersion string) (*resource.Resource, error) {
		return resource.New(
			ctx,
			resource.WithAttributes(
				semconv.ServiceName("minisky"),
				semconv.ServiceVersion(serviceVersion),
			),
		)
	}
)

func SetupTelemetry(ctx context.Context, config TelemetryConfig) (func(context.Context) error, error) {
	timeout := config.ExportTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	var exporter sdktrace.SpanExporter
	if config.Enabled {
		endpoint, parseErr := url.Parse(config.Endpoint)
		if parseErr != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
			return func(context.Context) error { return nil }, fmt.Errorf("invalid OTLP HTTP endpoint")
		}
		var exportErr error
		exporter, exportErr = newTelemetryExporter(ctx, endpoint.String(), timeout)
		if exportErr != nil {
			return func(context.Context) error { return nil }, fmt.Errorf("create OTLP HTTP exporter: %w", exportErr)
		}
	}

	res, resourceErr := newTelemetryResource(ctx, config.ServiceVersion)
	if resourceErr != nil {
		var cleanupErr error
		if exporter != nil {
			cleanupErr = exporter.Shutdown(ctx)
		}
		return func(context.Context) error { return nil }, errors.Join(
			fmt.Errorf("create OpenTelemetry resource: %w", resourceErr),
			cleanupErr,
		)
	}
	samplingRatio := 0.1
	if config.SamplingRatio != nil {
		samplingRatio = *config.SamplingRatio
	}
	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(boundedParentSampler(samplingRatio)),
	}
	if exporter != nil {
		options = append(options, sdktrace.WithBatcher(exporter))
	}
	provider := sdktrace.NewTracerProvider(options...)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	var once sync.Once
	var shutdownErr error
	shutdownDone := make(chan struct{})
	shutdown := func(shutdownCtx context.Context) error {
		once.Do(func() {
			go func() {
				shutdownErr = errors.Join(provider.ForceFlush(shutdownCtx), provider.Shutdown(shutdownCtx))
				close(shutdownDone)
			}()
		})
		select {
		case <-shutdownDone:
			return shutdownErr
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		}
	}

	return shutdown, nil
}

func boundedParentSampler(ratio float64) sdktrace.Sampler {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	root := sdktrace.TraceIDRatioBased(ratio)
	return sdktrace.ParentBased(
		root,
		sdktrace.WithRemoteParentSampled(root),
		sdktrace.WithRemoteParentNotSampled(root),
	)
}
