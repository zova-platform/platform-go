package natsx_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	commonv1 "github.com/zova-platform/proto/gen/go/zova/common/v1"
	dispatchv1 "github.com/zova-platform/proto/gen/go/zova/dispatch/v1"

	"github.com/zova-platform/platform-go/natsx"
)

func jobRequested() *dispatchv1.JobRequested {
	return &dispatchv1.JobRequested{
		Meta:        natsx.NewMeta(),
		JobType:     dispatchv1.JobType_JOB_TYPE_RIDE,
		RefId:       "trip-1",
		Pickup:      &commonv1.LatLng{Lat: 13.7456, Lng: 100.5340},
		Dropoff:     &commonv1.LatLng{Lat: 13.7650, Lng: 100.5370},
		VehicleType: commonv1.VehicleType_VEHICLE_TYPE_CAR,
	}
}

func rawConn(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func listen(t *testing.T, e env) *nats.Subscription {
	t.Helper()
	nc := rawConn(t)
	sub, err := nc.SubscribeSync(e.subject)
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return sub
}

func streamMessages(t *testing.T, name string) uint64 {
	t.Helper()
	js, err := jetstream.New(rawConn(t))
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

func installTracing(t *testing.T) {
	t.Helper()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
}

func TestConnectRequiresURL(t *testing.T) {
	if _, err := natsx.Connect(natsx.Config{}); err == nil {
		t.Fatal("want error without URL")
	}
}

func TestReadyReflectsConnection(t *testing.T) {
	e := setup(t)
	if err := e.client.Ready(t.Context()); err != nil {
		t.Fatalf("ready: %v", err)
	}
	e.client.Close()
	if err := e.client.Ready(t.Context()); err == nil {
		t.Fatal("want error after Close")
	}
}

func TestEnsureStreamIsIdempotent(t *testing.T) {
	e := setup(t)
	cfg := natsx.StreamConfig{Name: e.stream, Subjects: []string{"dispatch." + strings.Split(e.subject, ".")[1] + ".>"}, MaxAge: time.Hour}
	if err := e.client.EnsureStream(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := e.client.EnsureStream(t.Context(), natsx.StreamConfig{}); err == nil {
		t.Fatal("want error for empty config")
	}
}

func TestPublishWritesSnakeCaseJSON(t *testing.T) {
	e := setup(t)
	sub := listen(t, e)

	event := jobRequested()
	if err := e.client.Publish(t.Context(), e.subject, event); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	var body map[string]any
	if err := json.Unmarshal(msg.Data, &body); err != nil {
		t.Fatalf("payload is not JSON: %v", err)
	}
	meta, _ := body["meta"].(map[string]any)
	if meta["event_id"] != event.GetMeta().GetEventId() {
		t.Fatalf("meta.event_id missing or wrong: %s", msg.Data)
	}
	if _, ok := meta["occurred_at"]; !ok {
		t.Fatalf("meta.occurred_at missing: %s", msg.Data)
	}
	if body["ref_id"] != "trip-1" || body["job_type"] != "JOB_TYPE_RIDE" {
		t.Fatalf("unexpected payload: %s", msg.Data)
	}
	if strings.Contains(string(msg.Data), "eventId") {
		t.Fatalf("payload uses camelCase: %s", msg.Data)
	}
}

func TestPublishRejectsMissingEventID(t *testing.T) {
	e := setup(t)
	event := jobRequested()
	event.Meta = nil
	if err := e.client.Publish(t.Context(), e.subject, event); err == nil {
		t.Fatal("want error without meta")
	}
	event.Meta = &commonv1.EventMeta{}
	if err := e.client.Publish(t.Context(), e.subject, event); err == nil {
		t.Fatal("want error with empty event_id")
	}
}

func TestPublishCarriesTraceContext(t *testing.T) {
	installTracing(t)
	e := setup(t)
	sub := listen(t, e)

	ctx, span := otel.Tracer("test").Start(t.Context(), "request")
	defer span.End()
	if err := e.client.Publish(ctx, e.subject, jobRequested()); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	got := otel.GetTextMapPropagator().Extract(t.Context(), propagation.HeaderCarrier(msg.Header))
	if got, want := trace.SpanContextFromContext(got).TraceID(), span.SpanContext().TraceID(); !want.IsValid() || got != want {
		t.Fatalf("trace id in message = %s, want %s", got, want)
	}
}

func TestPublishRawContinuesStoredTrace(t *testing.T) {
	installTracing(t)
	e := setup(t)
	sub := listen(t, e)

	ctx, span := otel.Tracer("test").Start(t.Context(), "original request")
	stored := natsx.InjectHeaders(ctx)
	span.End()

	event := jobRequested()
	payload, err := natsx.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.client.PublishRaw(context.Background(), e.subject, event.GetMeta().GetEventId(), payload, stored); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	got := otel.GetTextMapPropagator().Extract(t.Context(), propagation.HeaderCarrier(msg.Header))
	if got, want := trace.SpanContextFromContext(got).TraceID(), span.SpanContext().TraceID(); got != want {
		t.Fatalf("outbox publish lost the original trace: %s, want %s", got, want)
	}
}

func TestDuplicateEventIDIsStoredOnce(t *testing.T) {
	e := setup(t)
	event := jobRequested()
	for range 3 {
		if err := e.client.Publish(t.Context(), e.subject, event); err != nil {
			t.Fatal(err)
		}
	}
	if n := streamMessages(t, e.stream); n != 1 {
		t.Fatalf("stream holds %d messages for one event_id, want 1", n)
	}
}
