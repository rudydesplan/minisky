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
}

func SetupTelemetry(ctx context.Context, config TelemetryConfig) (func(context.Context) error, error) {
	if !config.Enabled {
		return func(context.Context) error { return nil }, nil
	}
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return nil, fmt.Errorf("invalid OTLP HTTP endpoint %q", config.Endpoint)
	}
	timeout := config.ExportTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	exporter, err := otlptracehttp.New(
		ctx,
		otlptracehttp.WithEndpointURL(endpoint.String()),
		otlptracehttp.WithTimeout(timeout),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	res, err := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceName("minisky"),
			semconv.ServiceVersion(config.ServiceVersion),
		),
	)
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, fmt.Errorf("create telemetry resource: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	var once sync.Once
	var shutdownErr error
	return func(shutdownCtx context.Context) error {
		once.Do(func() {
			shutdownErr = errors.Join(provider.ForceFlush(shutdownCtx), provider.Shutdown(shutdownCtx))
		})
		return shutdownErr
	}, nil
}
