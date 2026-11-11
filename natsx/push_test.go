package natsx_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	commonv1 "github.com/zova-platform/proto/gen/go/zova/common/v1"
	dispatchv1 "github.com/zova-platform/proto/gen/go/zova/dispatch/v1"

	"github.com/zova-platform/platform-go/natsx"
)

func jobOffered() *dispatchv1.JobOffered {
	return &dispatchv1.JobOffered{
		Meta:     natsx.NewMeta(),
		JobId:    "job-1",
		DriverId: "driver-1",
		Pickup:   &commonv1.LatLng{Lat: 13.7456, Lng: 100.5340},
	}
}

func TestPushSubject(t *testing.T) {
	got, err := natsx.PushSubject("0193-abc")
	if err != nil || got != "push.user.0193-abc" {
		t.Fatalf("PushSubject = %q, %v", got, err)
	}
	for _, bad := range []string{"", "a.b", "*", ">", "a b", "a\n"} {
		if _, err := natsx.PushSubject(bad); err == nil {
			t.Errorf("PushSubject(%q) must fail so a user id cannot become a wildcard", bad)
		}
	}
}

func TestPushReachesOnlyTheTargetUser(t *testing.T) {
	e := setup(t)
	mine := make(chan natsx.PushMessage, 2)
	others := make(chan natsx.PushMessage, 2)

	stopMine, err := e.client.SubscribePush("driver-1", func(_ context.Context, m natsx.PushMessage) { mine <- m })
	if err != nil {
		t.Fatal(err)
	}
	defer stopMine()
	stopOthers, err := e.client.SubscribePush("driver-2", func(_ context.Context, m natsx.PushMessage) { others <- m })
	if err != nil {
		t.Fatal(err)
	}
	defer stopOthers()

	if err := e.client.Push(t.Context(), "driver-1", "offer.new", jobOffered()); err != nil {
		t.Fatal(err)
	}

	msg := receive(t, mine)
	if msg.Type != "offer.new" {
		t.Fatalf("type = %q", msg.Type)
	}
	var offered dispatchv1.JobOffered
	if err := natsx.Unmarshal(msg.Data, &offered); err != nil {
		t.Fatalf("data is not a JobOffered: %v", err)
	}
	if offered.GetJobId() != "job-1" || offered.GetDriverId() != "driver-1" {
		t.Fatalf("unexpected data: %v", &offered)
	}

	select {
	case m := <-others:
		t.Fatalf("another user received %v", m)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestPushWithoutData(t *testing.T) {
	e := setup(t)
	got := make(chan natsx.PushMessage, 1)
	stop, err := e.client.SubscribePush("p-1", func(_ context.Context, m natsx.PushMessage) { got <- m })
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	if err := e.client.Push(t.Context(), "p-1", "offer.expired", nil); err != nil {
		t.Fatal(err)
	}
	if msg := receive(t, got); msg.Type != "offer.expired" || len(msg.Data) != 0 {
		t.Fatalf("unexpected message: %+v", msg)
	}
}

func TestPushValidatesInput(t *testing.T) {
	e := setup(t)
	if err := e.client.Push(t.Context(), "a.b", "x", nil); err == nil {
		t.Error("want error for a user id with a dot")
	}
	if err := e.client.Push(t.Context(), "user-1", "", nil); err == nil {
		t.Error("want error for an empty type")
	}
	if _, err := e.client.SubscribePush(">", func(context.Context, natsx.PushMessage) {}); err == nil {
		t.Error("want error when subscribing with a wildcard user id")
	}
}

func TestPushStopsAfterUnsubscribe(t *testing.T) {
	e := setup(t)
	got := make(chan natsx.PushMessage, 2)
	stop, err := e.client.SubscribePush("gone", func(_ context.Context, m natsx.PushMessage) { got <- m })
	if err != nil {
		t.Fatal(err)
	}
	stop()
	stop()

	if err := e.client.Push(t.Context(), "gone", "late", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		t.Fatalf("received %v after unsubscribe", m)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestPushContinuesTrace(t *testing.T) {
	installTracing(t)
	e := setup(t)
	got := make(chan trace.TraceID, 1)
	stop, err := e.client.SubscribePush("tracer", func(ctx context.Context, _ natsx.PushMessage) {
		got <- trace.SpanContextFromContext(ctx).TraceID()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	ctx, span := otel.Tracer("test").Start(t.Context(), "request")
	defer span.End()
	if err := e.client.Push(ctx, "tracer", "trip.status", nil); err != nil {
		t.Fatal(err)
	}
	if want := span.SpanContext().TraceID(); receive(t, got) != want {
		t.Fatal("push receiver is not in the sender's trace")
	}
}
