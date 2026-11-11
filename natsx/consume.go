package natsx

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultMaxDeliver = 5
	defaultAckWait    = 30 * time.Second
	defaultRetryDelay = 2 * time.Second
	defaultDedupeSize = 10000
)

type Deduper interface {
	Seen(ctx context.Context, eventID string) (bool, error)
	Mark(ctx context.Context, eventID string) error
}

type SubscribeConfig struct {
	Stream     string
	Durable    string
	Subject    string
	MaxDeliver int
	AckWait    time.Duration
	RetryDelay time.Duration
	Deduper    Deduper
}

func Subscribe[T Event](ctx context.Context, c *Client, cfg SubscribeConfig, handle func(context.Context, T) error) (stop func(), err error) {
	if cfg.Stream == "" || cfg.Durable == "" || cfg.Subject == "" {
		return nil, errors.New("natsx: Stream, Durable and Subject are required")
	}
	if cfg.MaxDeliver == 0 {
		cfg.MaxDeliver = defaultMaxDeliver
	}
	if cfg.AckWait == 0 {
		cfg.AckWait = defaultAckWait
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = defaultRetryDelay
	}
	if cfg.Deduper == nil {
		cfg.Deduper = NewMemoryDeduper(defaultDedupeSize)
	}

	consumer, err := c.js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:       cfg.Durable,
		FilterSubject: cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		MaxDeliver:    cfg.MaxDeliver,
		AckWait:       cfg.AckWait,
	})
	if err != nil {
		return nil, fmt.Errorf("natsx: consumer %s: %w", cfg.Durable, err)
	}

	consume, err := consumer.Consume(func(msg jetstream.Msg) {
		process(cfg, msg, handle)
	})
	if err != nil {
		return nil, fmt.Errorf("natsx: consume %s: %w", cfg.Durable, err)
	}

	var once sync.Once
	stop = func() { once.Do(consume.Stop) }
	context.AfterFunc(ctx, stop)
	return stop, nil
}

func process[T Event](cfg SubscribeConfig, msg jetstream.Msg, handle func(context.Context, T) error) {
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(http.Header(msg.Headers())))
	ctx, span := otel.Tracer(tracerName).Start(ctx, "process "+msg.Subject(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", msg.Subject()),
			attribute.String("messaging.consumer.group.name", cfg.Durable),
		),
	)
	defer span.End()

	event := newEvent[T]()
	if err := Unmarshal(msg.Data(), event); err != nil {
		reject(ctx, span, msg, "undecodable event", err)
		return
	}
	id := event.GetMeta().GetEventId()
	if id == "" {
		reject(ctx, span, msg, "event without meta.event_id", errors.New("missing event_id"))
		return
	}
	span.SetAttributes(attribute.String("messaging.message.id", id))

	seen, err := cfg.Deduper.Seen(ctx, id)
	if err != nil {
		retry(ctx, span, cfg, msg, "dedupe check failed", err)
		return
	}
	if seen {
		span.SetAttributes(attribute.Bool("messaging.duplicate", true))
		_ = msg.Ack()
		return
	}

	if err := handle(ctx, event); err != nil {
		retry(ctx, span, cfg, msg, "handler failed", err)
		return
	}
	if err := cfg.Deduper.Mark(ctx, id); err != nil {
		slog.ErrorContext(ctx, "natsx: mark event processed", "event_id", id, "error", err)
	}
	_ = msg.Ack()
}

func reject(ctx context.Context, span trace.Span, msg jetstream.Msg, reason string, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, reason)
	slog.ErrorContext(ctx, "natsx: dropping message", "subject", msg.Subject(), "reason", reason, "error", err)
	_ = msg.Term()
}

func retry(ctx context.Context, span trace.Span, cfg SubscribeConfig, msg jetstream.Msg, reason string, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, reason)

	if meta, metaErr := msg.Metadata(); metaErr == nil && int(meta.NumDelivered) >= cfg.MaxDeliver {
		slog.ErrorContext(ctx, "natsx: giving up on message", "subject", msg.Subject(), "deliveries", meta.NumDelivered, "reason", reason, "error", err)
		_ = msg.Term()
		return
	}
	slog.WarnContext(ctx, "natsx: will retry message", "subject", msg.Subject(), "reason", reason, "error", err)
	_ = msg.NakWithDelay(cfg.RetryDelay)
}

func newEvent[T Event]() T {
	var zero T
	return zero.ProtoReflect().New().Interface().(T)
}

type MemoryDeduper struct {
	mu       sync.Mutex
	capacity int
	order    *list.List
	items    map[string]*list.Element
}

func NewMemoryDeduper(capacity int) *MemoryDeduper {
	if capacity < 1 {
		capacity = 1
	}
	return &MemoryDeduper{
		capacity: capacity,
		order:    list.New(),
		items:    make(map[string]*list.Element, capacity),
	}
}

func (d *MemoryDeduper) Seen(_ context.Context, eventID string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	el, ok := d.items[eventID]
	if ok {
		d.order.MoveToFront(el)
	}
	return ok, nil
}

func (d *MemoryDeduper) Mark(_ context.Context, eventID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if el, ok := d.items[eventID]; ok {
		d.order.MoveToFront(el)
		return nil
	}
	d.items[eventID] = d.order.PushFront(eventID)
	if d.order.Len() > d.capacity {
		oldest := d.order.Back()
		d.order.Remove(oldest)
		delete(d.items, oldest.Value.(string))
	}
	return nil
}
