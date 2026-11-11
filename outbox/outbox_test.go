package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	commonv1 "github.com/zova-platform/proto/gen/go/zova/common/v1"
	dispatchv1 "github.com/zova-platform/proto/gen/go/zova/dispatch/v1"

	"github.com/zova-platform/platform-go/natsx"
	"github.com/zova-platform/platform-go/outbox"
)

var (
	pool    *pgxpool.Pool
	natsURL string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	pg, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("outbox"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres container:", err)
		os.Exit(1)
	}
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres connection string:", err)
		os.Exit(1)
	}
	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect postgres:", err)
		os.Exit(1)
	}

	nc, err := tcnats.Run(ctx, "nats:2.15-alpine")
	if err != nil {
		fmt.Fprintln(os.Stderr, "start nats container:", err)
		os.Exit(1)
	}
	natsURL, err = nc.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "nats connection string:", err)
		os.Exit(1)
	}

	code := m.Run()

	pool.Close()
	_ = testcontainers.TerminateContainer(pg)
	_ = testcontainers.TerminateContainer(nc)
	os.Exit(code)
}

func splitMigration(t *testing.T) (up, down string) {
	t.Helper()
	parts := strings.SplitN(outbox.MigrationSQL, "-- +goose Down", 2)
	if len(parts) != 2 {
		t.Fatal("migration has no goose Down section")
	}
	return strings.TrimPrefix(strings.TrimSpace(parts[0]), "-- +goose Up"), parts[1]
}

func freshTable(t *testing.T) {
	t.Helper()
	up, _ := splitMigration(t)
	if _, err := pool.Exec(t.Context(), `DROP TABLE IF EXISTS outbox`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), up); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
}

func event(ref string) *dispatchv1.JobRequested {
	return &dispatchv1.JobRequested{
		Meta:        natsx.NewMeta(),
		JobType:     dispatchv1.JobType_JOB_TYPE_RIDE,
		RefId:       ref,
		Pickup:      &commonv1.LatLng{Lat: 13.7456, Lng: 100.5340},
		Dropoff:     &commonv1.LatLng{Lat: 13.7650, Lng: 100.5370},
		VehicleType: commonv1.VehicleType_VEHICLE_TYPE_CAR,
	}
}

func insertCommitted(ctx context.Context, t *testing.T, ev *dispatchv1.JobRequested) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Insert(ctx, tx, "dispatch.job.requested", ev); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func unpublishedCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

type published struct {
	subject, id string
	payload     []byte
	headers     map[string]string
}

type fakePublisher struct {
	mu      sync.Mutex
	failOn  func(n int) bool
	calls   int
	sent    []published
	perID   map[string]int
	latency time.Duration
}

func newFake() *fakePublisher { return &fakePublisher{perID: map[string]int{}} }

func (f *fakePublisher) PublishRaw(_ context.Context, subject, msgID string, payload []byte, headers map[string]string) error {
	if f.latency > 0 {
		time.Sleep(f.latency)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failOn != nil && f.failOn(f.calls) {
		return errors.New("broker unavailable")
	}
	f.sent = append(f.sent, published{subject: subject, id: msgID, payload: payload, headers: headers})
	f.perID[msgID]++
	return nil
}

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestMigrationUpAndDown(t *testing.T) {
	freshTable(t)
	_, down := splitMigration(t)
	if _, err := pool.Exec(t.Context(), down); err != nil {
		t.Fatalf("apply down: %v", err)
	}
	var exists bool
	if err := pool.QueryRow(t.Context(), `SELECT to_regclass('outbox') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("outbox table still exists after the Down migration")
	}
}

func TestInsertRequiresEventID(t *testing.T) {
	freshTable(t)
	ev := event("x")
	ev.Meta = nil
	if err := outbox.Insert(t.Context(), pool, "dispatch.job.requested", ev); err == nil {
		t.Fatal("want error without event_id")
	}
}

func TestCommittedEventIsPublishedOnce(t *testing.T) {
	freshTable(t)
	pub := newFake()
	worker := outbox.NewWorker(pool, pub, outbox.Config{})

	ev := event("trip-1")
	insertCommitted(t.Context(), t, ev)

	n, err := worker.PublishPending(t.Context())
	if err != nil || n != 1 {
		t.Fatalf("PublishPending = %d, %v; want 1, nil", n, err)
	}
	if n, err := worker.PublishPending(t.Context()); err != nil || n != 0 {
		t.Fatalf("second PublishPending = %d, %v; want 0, nil", n, err)
	}
	got := pub.sent[0]
	if got.subject != "dispatch.job.requested" || got.id != ev.GetMeta().GetEventId() {
		t.Fatalf("published %+v", got)
	}
	if !strings.Contains(string(got.payload), `"ref_id":"trip-1"`) && !strings.Contains(string(got.payload), `"ref_id": "trip-1"`) {
		t.Fatalf("payload does not carry the event: %s", got.payload)
	}
	if unpublishedCount(t) != 0 {
		t.Fatal("row is still unpublished")
	}
}

func TestRolledBackEventIsNeverPublished(t *testing.T) {
	freshTable(t)
	pub := newFake()
	worker := outbox.NewWorker(pool, pub, outbox.Config{})

	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Insert(t.Context(), tx, "dispatch.job.requested", event("rolled-back")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}

	if n, err := worker.PublishPending(t.Context()); err != nil || n != 0 || pub.count() != 0 {
		t.Fatalf("PublishPending = %d, %v with %d sent; want nothing", n, err, pub.count())
	}
}

func TestBrokerFailureKeepsRowsForRetry(t *testing.T) {
	freshTable(t)
	pub := newFake()
	pub.failOn = func(n int) bool { return n == 1 }
	worker := outbox.NewWorker(pool, pub, outbox.Config{})
	insertCommitted(t.Context(), t, event("a"))

	if _, err := worker.PublishPending(t.Context()); err == nil {
		t.Fatal("want the broker error")
	}
	if unpublishedCount(t) != 1 {
		t.Fatal("a failed publish must leave the row pending")
	}
	if n, err := worker.PublishPending(t.Context()); err != nil || n != 1 {
		t.Fatalf("retry = %d, %v; want 1, nil", n, err)
	}
}

func TestPartialFailureMarksOnlyWhatWasPublished(t *testing.T) {
	freshTable(t)
	pub := newFake()
	pub.failOn = func(n int) bool { return n == 2 }
	worker := outbox.NewWorker(pool, pub, outbox.Config{})

	var ids []string
	for _, ref := range []string{"first", "second", "third"} {
		ev := event(ref)
		ids = append(ids, ev.GetMeta().GetEventId())
		insertCommitted(t.Context(), t, ev)
		time.Sleep(5 * time.Millisecond)
	}

	n, err := worker.PublishPending(t.Context())
	if err == nil || n != 1 {
		t.Fatalf("PublishPending = %d, %v; want 1 published and an error", n, err)
	}
	if pub.sent[0].id != ids[0] {
		t.Fatal("events were not published in insertion order")
	}
	if unpublishedCount(t) != 2 {
		t.Fatalf("pending rows = %d, want 2", unpublishedCount(t))
	}
	if n, err := worker.PublishPending(t.Context()); err != nil || n != 2 {
		t.Fatalf("second pass = %d, %v; want 2, nil", n, err)
	}
	if pub.sent[1].id != ids[1] || pub.sent[2].id != ids[2] {
		t.Fatal("remaining events were published out of order")
	}
}

func TestTwoWorkersPublishEachEventOnce(t *testing.T) {
	freshTable(t)
	pub := newFake()
	pub.latency = 2 * time.Millisecond
	cfg := outbox.Config{PollInterval: 10 * time.Millisecond, BatchSize: 5}

	const total = 60
	for i := range total {
		insertCommitted(t.Context(), t, event(fmt.Sprintf("job-%d", i)))
	}

	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = outbox.NewWorker(pool, pub, cfg).Run(ctx)
		}()
	}
	waitUntil(t, "all events published", func() bool { return pub.count() >= total })
	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.perID) != total {
		t.Fatalf("published %d distinct events, want %d", len(pub.perID), total)
	}
	for id, n := range pub.perID {
		if n != 1 {
			t.Errorf("event %s published %d times, want once", id, n)
		}
	}
}

func TestRunStopsWhenContextIsCancelled(t *testing.T) {
	freshTable(t)
	pub := newFake()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- outbox.NewWorker(pool, pub, outbox.Config{PollInterval: 10 * time.Millisecond}).Run(ctx)
	}()

	insertCommitted(t.Context(), t, event("live"))
	waitUntil(t, "the running worker to publish", func() bool { return pub.count() == 1 })

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestPurgeRemovesOnlyOldPublishedRows(t *testing.T) {
	freshTable(t)
	worker := outbox.NewWorker(pool, newFake(), outbox.Config{Retention: time.Hour})

	oldPublished, recentPublished, pending := event("old"), event("recent"), event("pending")
	for _, ev := range []*dispatchv1.JobRequested{oldPublished, recentPublished, pending} {
		insertCommitted(t.Context(), t, ev)
	}
	for id, age := range map[string]string{
		oldPublished.GetMeta().GetEventId():    "3 hours",
		recentPublished.GetMeta().GetEventId(): "1 minute",
	} {
		if _, err := pool.Exec(t.Context(), `UPDATE outbox SET published_at = now() - $2::interval WHERE id = $1::uuid`, id, age); err != nil {
			t.Fatal(err)
		}
	}

	removed, err := worker.Purge(t.Context())
	if err != nil || removed != 1 {
		t.Fatalf("Purge = %d, %v; want 1, nil", removed, err)
	}
	var left int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM outbox`).Scan(&left); err != nil || left != 2 {
		t.Fatalf("rows left = %d, %v; want 2", left, err)
	}
}

func TestWorkerPublishesThroughNATSInTheOriginalTrace(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	freshTable(t)

	client, err := natsx.Connect(natsx.Config{URL: natsURL, Name: "outbox-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.EnsureStream(t.Context(), natsx.StreamConfig{Name: "OUTBOXTEST", Subjects: []string{"dispatch.>"}}); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("dispatch.job.requested")
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx, span := otel.Tracer("test").Start(t.Context(), "request")
	insertCommitted(ctx, t, event("traced"))
	span.End()

	worker := outbox.NewWorker(pool, client, outbox.Config{})
	if n, err := worker.PublishPending(context.Background()); err != nil || n != 1 {
		t.Fatalf("PublishPending = %d, %v", n, err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	got := otel.GetTextMapPropagator().Extract(t.Context(), propagation.HeaderCarrier(msg.Header))
	if got, want := trace.SpanContextFromContext(got).TraceID(), span.SpanContext().TraceID(); !want.IsValid() || got != want {
		t.Fatalf("message trace id = %s, want the request's %s", got, want)
	}
	if !strings.Contains(string(msg.Data), `"ref_id"`) || strings.Contains(string(msg.Data), "refId") {
		t.Fatalf("payload is not snake_case JSON: %s", msg.Data)
	}
}
