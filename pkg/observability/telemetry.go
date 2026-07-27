package observability

import (
	"context"
	"errors"
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
	options := make([]sdktrace.TracerProviderOption, 0, 1)
	res, resourceErr := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceName("minisky"),
			semconv.ServiceVersion(config.ServiceVersion),
		),
	)
	if resourceErr == nil {
		options = append(options, sdktrace.WithResource(res))
	}
	provider := sdktrace.NewTracerProvider(options...)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	var once sync.Once
	var shutdownErr error
	shutdown := func(shutdownCtx context.Context) error {
		once.Do(func() {
			shutdownErr = errors.Join(provider.ForceFlush(shutdownCtx), provider.Shutdown(shutdownCtx))
		})
		return shutdownErr
	}

	if config.Enabled {
		endpoint, parseErr := url.Parse(config.Endpoint)
		if parseErr != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
			return shutdown, nil
		}
		timeout := config.ExportTimeout
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		exporter, exportErr := otlptracehttp.New(
			ctx,
			otlptracehttp.WithEndpointURL(endpoint.String()),
			otlptracehttp.WithTimeout(timeout),
		)
		if exportErr != nil {
			return shutdown, nil
		}
		provider.RegisterSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter))
	}
	return shutdown, nil
}
