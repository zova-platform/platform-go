package otelx_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/zova-platform/platform-go/otelx"
)

func TestSetupRequiresServiceName(t *testing.T) {
	if _, err := otelx.Setup(context.Background(), otelx.Config{}); err == nil {
		t.Fatal("want error without ServiceName")
	}
}

func TestSetupExportsSpansAndMetrics(t *testing.T) {
	ctx := context.Background()
	spans := tracetest.NewInMemoryExporter()
	reader := sdkmetric.NewManualReader()

	shutdown, err := otelx.Setup(ctx, otelx.Config{
		ServiceName:    "trip-service",
		ServiceVersion: "1.2.3",
		SpanExporter:   spans,
		MetricReader:   reader,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, span := otel.Tracer("test").Start(ctx, "work")
	span.End()

	counter, err := otel.Meter("test").Int64Counter("requests")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(ctx, 3)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("no metrics collected")
	}
	if v, ok := rm.Resource.Set().Value("service.name"); !ok || v.AsString() != "trip-service" {
		t.Fatalf("service.name missing from metric resource: %v", rm.Resource)
	}

	tp, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider)
	if !ok {
		t.Fatalf("global provider is %T, want *sdk/trace.TracerProvider", otel.GetTracerProvider())
	}
	if err := tp.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	got := spans.GetSpans()
	if len(got) != 1 || got[0].Name != "work" {
		t.Fatalf("want one span named work, got %v", got)
	}
	var found bool
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, kv := range got[0].Resource.Attributes() {
		if string(kv.Key) == "service.name" && kv.Value.AsString() == "trip-service" {
			found = true
		}
	}
	if !found {
		t.Fatalf("service.name missing from span resource: %v", got[0].Resource)
	}
}

func TestSetupInstallsTraceContextPropagator(t *testing.T) {
	shutdown, err := otelx.Setup(context.Background(), otelx.Config{
		ServiceName:  "x",
		SpanExporter: tracetest.NewInMemoryExporter(),
		MetricReader: sdkmetric.NewManualReader(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	fields := otel.GetTextMapPropagator().Fields()
	want := map[string]bool{"traceparent": false, "baggage": false}
	for _, f := range fields {
		if _, ok := want[f]; ok {
			want[f] = true
		}
	}
	for k, ok := range want {
		if !ok {
			t.Errorf("propagator is missing %s (fields: %v)", k, fields)
		}
	}
}

func TestSetupWithOTLPExporterDoesNotBlock(t *testing.T) {

	shutdown, err := otelx.Setup(context.Background(), otelx.Config{
		ServiceName: "x",
		Endpoint:    "127.0.0.1:1",
		Insecure:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = shutdown(ctx)
}
