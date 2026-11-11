package natsx_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	dispatchv1 "github.com/zova-platform/proto/gen/go/zova/dispatch/v1"

	"github.com/zova-platform/platform-go/natsx"
)

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the handler")
		var zero T
		return zero
	}
}

func subscribeConfig(e env, durable string) natsx.SubscribeConfig {
	return natsx.SubscribeConfig{
		Stream:     e.stream,
		Durable:    durable,
		Subject:    e.subject,
		RetryDelay: 20 * time.Millisecond,
		AckWait:    time.Second,
	}
}

func consumerInfo(t *testing.T, e env, durable string) *jetstream.ConsumerInfo {
	t.Helper()
	js, err := jetstream.New(rawConn(t))
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := js.Consumer(t.Context(), e.stream, durable)
	if err != nil {
		t.Fatal(err)
	}
	info, err := consumer.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestSubscribeDeliversTypedEvent(t *testing.T) {
	e := setup(t)
	got := make(chan *dispatchv1.JobRequested, 1)
	stop, err := natsx.Subscribe(t.Context(), e.client, subscribeConfig(e, "typed"), func(_ context.Context, ev *dispatchv1.JobRequested) error {
		got <- ev
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	sent := jobRequested()
	if err := e.client.Publish(t.Context(), e.subject, sent); err != nil {
		t.Fatal(err)
	}
	if ev := receive(t, got); !proto.Equal(ev, sent) {
		t.Fatalf("received %v, want %v", ev, sent)
	}
}

func TestSubscribeSkipsDuplicateEventID(t *testing.T) {
	e := setup(t)
	var mu sync.Mutex
	calls := map[string]int{}
	done := make(chan string, 4)
	stop, err := natsx.Subscribe(t.Context(), e.client, subscribeConfig(e, "dedupe"), func(_ context.Context, ev *dispatchv1.JobRequested) error {
		mu.Lock()
		calls[ev.GetMeta().GetEventId()]++
		mu.Unlock()
		done <- ev.GetRefId()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	first := jobRequested()
	first.RefId = "first"
	payload, err := natsx.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, msgID := range []string{"transport-1", "transport-2"} {
		if err := e.client.PublishRaw(t.Context(), e.subject, msgID, payload, nil); err != nil {
			t.Fatal(err)
		}
	}
	second := jobRequested()
	second.RefId = "second"
	if err := e.client.Publish(t.Context(), e.subject, second); err != nil {
		t.Fatal(err)
	}

	if receive(t, done) != "first" || receive(t, done) != "second" {
		t.Fatal("events were not delivered in order")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls[first.GetMeta().GetEventId()] != 1 || calls[second.GetMeta().GetEventId()] != 1 {
		t.Fatalf("handler calls per event_id = %v, want 1 each", calls)
	}
}

func TestSubscribeRetriesUntilHandlerSucceeds(t *testing.T) {
	e := setup(t)
	var attempts atomic.Int32
	done := make(chan struct{}, 1)
	stop, err := natsx.Subscribe(t.Context(), e.client, subscribeConfig(e, "retry"), func(context.Context, *dispatchv1.JobRequested) error {
		if attempts.Add(1) < 3 {
			return errors.New("temporary failure")
		}
		done <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	if err := e.client.Publish(t.Context(), e.subject, jobRequested()); err != nil {
		t.Fatal(err)
	}
	receive(t, done)
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
	time.Sleep(200 * time.Millisecond)
	if got := attempts.Load(); got != 3 {
		t.Fatalf("event handled again after success: attempts = %d", got)
	}
}

func TestSubscribeGivesUpAfterMaxDeliver(t *testing.T) {
	e := setup(t)
	var attempts atomic.Int32
	cfg := subscribeConfig(e, "giveup")
	cfg.MaxDeliver = 3
	stop, err := natsx.Subscribe(t.Context(), e.client, cfg, func(context.Context, *dispatchv1.JobRequested) error {
		attempts.Add(1)
		return errors.New("always failing")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	if err := e.client.Publish(t.Context(), e.subject, jobRequested()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want exactly MaxDeliver (3)", got)
	}
}

func TestSubscribeDropsUndecodableMessage(t *testing.T) {
	e := setup(t)
	got := make(chan string, 2)
	stop, err := natsx.Subscribe(t.Context(), e.client, subscribeConfig(e, "poison"), func(_ context.Context, ev *dispatchv1.JobRequested) error {
		got <- ev.GetRefId()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	if err := e.client.PublishRaw(t.Context(), e.subject, "garbage-1", []byte("{not json"), nil); err != nil {
		t.Fatal(err)
	}
	good := jobRequested()
	good.RefId = "good"
	if err := e.client.Publish(t.Context(), e.subject, good); err != nil {
		t.Fatal(err)
	}

	if receive(t, got) != "good" {
		t.Fatal("handler saw something other than the valid event")
	}
	time.Sleep(1500 * time.Millisecond)
	info := consumerInfo(t, e, "poison")
	if info.Delivered.Consumer != 2 || info.NumAckPending != 0 {
		t.Fatalf("delivered=%d ack_pending=%d, want 2 deliveries and nothing pending", info.Delivered.Consumer, info.NumAckPending)
	}
}

func TestSubscribeResumesDurableConsumer(t *testing.T) {
	e := setup(t)
	cfg := subscribeConfig(e, "durable")
	got := make(chan string, 4)
	handle := func(_ context.Context, ev *dispatchv1.JobRequested) error {
		got <- ev.GetRefId()
		return nil
	}

	stop, err := natsx.Subscribe(t.Context(), e.client, cfg, handle)
	if err != nil {
		t.Fatal(err)
	}
	first := jobRequested()
	first.RefId = "before"
	if err := e.client.Publish(t.Context(), e.subject, first); err != nil {
		t.Fatal(err)
	}
	if receive(t, got) != "before" {
		t.Fatal("first event lost")
	}
	stop()

	missed := jobRequested()
	missed.RefId = "while-stopped"
	if err := e.client.Publish(t.Context(), e.subject, missed); err != nil {
		t.Fatal(err)
	}

	stop, err = natsx.Subscribe(t.Context(), e.client, cfg, handle)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if receive(t, got) != "while-stopped" {
		t.Fatal("durable consumer did not deliver the event published while it was stopped")
	}
}

func TestSubscribeContinuesTrace(t *testing.T) {
	installTracing(t)
	e := setup(t)
	got := make(chan trace.TraceID, 1)
	stop, err := natsx.Subscribe(t.Context(), e.client, subscribeConfig(e, "trace"), func(ctx context.Context, _ *dispatchv1.JobRequested) error {
		got <- trace.SpanContextFromContext(ctx).TraceID()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	ctx, span := otel.Tracer("test").Start(t.Context(), "request")
	defer span.End()
	if err := e.client.Publish(ctx, e.subject, jobRequested()); err != nil {
		t.Fatal(err)
	}
	if want := span.SpanContext().TraceID(); receive(t, got) != want {
		t.Fatal("handler is not in the publisher's trace")
	}
}

func TestSubscribeRequiresNames(t *testing.T) {
	e := setup(t)
	_, err := natsx.Subscribe(t.Context(), e.client, natsx.SubscribeConfig{Stream: e.stream}, func(context.Context, *dispatchv1.JobRequested) error { return nil })
	if err == nil {
		t.Fatal("want error without Durable and Subject")
	}
}

func TestMemoryDeduperEvictsOldest(t *testing.T) {
	d := natsx.NewMemoryDeduper(2)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if err := d.Mark(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	for id, want := range map[string]bool{"a": false, "b": true, "c": true} {
		got, err := d.Seen(ctx, id)
		if err != nil || got != want {
			t.Errorf("Seen(%q) = %v, %v; want %v", id, got, err, want)
		}
	}
}
