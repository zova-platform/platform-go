package otelx

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

type Config struct {
	ServiceName string

	ServiceVersion string

	Endpoint string

	Insecure bool

	SpanExporter sdktrace.SpanExporter
	MetricReader sdkmetric.Reader
}

func Setup(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	if cfg.ServiceName == "" {
		return nil, errors.New("otelx: ServiceName is required")
	}

	attrs := []resource.Option{
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, resource.WithAttributes(semconv.ServiceVersion(cfg.ServiceVersion)))
	}
	res, err := resource.New(ctx, attrs...)
	if err != nil {
		return nil, fmt.Errorf("otelx: resource: %w", err)
	}

	spanExp := cfg.SpanExporter
	if spanExp == nil {
		spanExp, err = otlptracegrpc.New(ctx, traceOptions(cfg)...)
		if err != nil {
			return nil, fmt.Errorf("otelx: trace exporter: %w", err)
		}
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(spanExp),
		sdktrace.WithResource(res),
	)

	reader := cfg.MetricReader
	if reader == nil {
		metricExp, mErr := otlpmetricgrpc.New(ctx, metricOptions(cfg)...)
		if mErr != nil {
			_ = tp.Shutdown(ctx)
			return nil, fmt.Errorf("otelx: metric exporter: %w", mErr)
		}
		reader = sdkmetric.NewPeriodicReader(metricExp)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}, nil
}

func traceOptions(cfg Config) []otlptracegrpc.Option {
	var opts []otlptracegrpc.Option
	if cfg.Endpoint != "" {
		opts = append(opts, otlptracegrpc.WithEndpoint(cfg.Endpoint))
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	return opts
}

func metricOptions(cfg Config) []otlpmetricgrpc.Option {
	var opts []otlpmetricgrpc.Option
	if cfg.Endpoint != "" {
		opts = append(opts, otlpmetricgrpc.WithEndpoint(cfg.Endpoint))
	}
	if cfg.Insecure {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}
	return opts
}
