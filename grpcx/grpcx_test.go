package grpcx_test

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/zova-platform/platform-go/grpcx"
)

type testService struct {
	healthpb.UnimplementedHealthServer

	mu       sync.Mutex
	traceID  trace.TraceID
	deadline bool
}

func (s *testService) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	s.mu.Lock()
	s.traceID = trace.SpanContextFromContext(ctx).TraceID()
	_, s.deadline = ctx.Deadline()
	s.mu.Unlock()

	switch req.GetService() {
	case "panic":
		panic("boom")
	case "slow":
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	case "notfound":
		return nil, status.Error(codes.NotFound, "no such thing")
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (s *testService) Watch(req *healthpb.HealthCheckRequest, _ grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
	if req.GetService() == "panic" {
		panic("stream boom")
	}
	return nil
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type env struct {
	client healthpb.HealthClient
	svc    *testService
	logs   *syncBuffer
}

func setup(t *testing.T, srvTimeout, cliTimeout time.Duration) env {
	t.Helper()

	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	svc := &testService{}

	lis := bufconn.Listen(1 << 20)
	srv := grpcx.NewServer(grpcx.ServerOptions{Logger: log, Timeout: srvTimeout})
	healthpb.RegisterHealthServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dialer := grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() })
	var conn *grpc.ClientConn
	var err error
	if cliTimeout < 0 {

		conn, err = grpc.NewClient("passthrough:///bufnet", dialer,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		conn, err = grpcx.NewClient("passthrough:///bufnet", grpcx.ClientOptions{Logger: log, Timeout: cliTimeout},
			dialer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return env{client: healthpb.NewHealthClient(conn), svc: svc, logs: logs}
}

func TestUnaryCallSucceedsAndIsLogged(t *testing.T) {
	e := setup(t, time.Second, time.Second)

	resp, err := e.client.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v", resp.GetStatus())
	}
	out := e.logs.String()
	for _, want := range []string{"side=server", "side=client", "method=/grpc.health.v1.Health/Check", "code=OK"} {
		if !strings.Contains(out, want) {
			t.Errorf("log is missing %q:\n%s", want, out)
		}
	}
}

func TestErrorCodeIsLoggedAtWarn(t *testing.T) {
	e := setup(t, time.Second, time.Second)

	_, err := e.client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "notfound"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}
	if out := e.logs.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "code=NotFound") {
		t.Fatalf("want a WARN line with code=NotFound:\n%s", out)
	}
}

func TestUnaryPanicBecomesInternalAndServerSurvives(t *testing.T) {
	e := setup(t, time.Second, time.Second)

	_, err := e.client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "panic"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal", status.Code(err))
	}
	if !strings.Contains(e.logs.String(), "grpc handler panic") {
		t.Fatalf("panic was not logged:\n%s", e.logs.String())
	}

	if _, err := e.client.Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("server did not survive the panic: %v", err)
	}
}

func TestStreamPanicBecomesInternal(t *testing.T) {
	e := setup(t, time.Second, time.Second)

	stream, err := e.client.Watch(context.Background(), &healthpb.HealthCheckRequest{Service: "panic"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal", status.Code(err))
	}
}

func TestServerAddsDefaultTimeout(t *testing.T) {

	e := setup(t, 50*time.Millisecond, -1)

	start := time.Now()
	_, err := e.client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "slow"})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want DeadlineExceeded", status.Code(err))
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("call was not cut off by the timeout")
	}
}

func TestClientAddsDefaultTimeout(t *testing.T) {
	e := setup(t, time.Minute, 50*time.Millisecond)

	_, err := e.client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "slow"})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want DeadlineExceeded", status.Code(err))
	}
}

func TestCallerDeadlineIsKept(t *testing.T) {
	e := setup(t, 10*time.Millisecond, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := e.client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	e.svc.mu.Lock()
	defer e.svc.mu.Unlock()
	if !e.svc.deadline {
		t.Fatal("handler context has no deadline")
	}
}

func TestTraceContextPropagatesToServer(t *testing.T) {
	e := setup(t, time.Second, time.Second)

	ctx, span := otel.Tracer("test").Start(context.Background(), "client-op")
	defer span.End()
	if _, err := e.client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}

	want := span.SpanContext().TraceID()
	e.svc.mu.Lock()
	defer e.svc.mu.Unlock()
	if !want.IsValid() || e.svc.traceID != want {
		t.Fatalf("server trace id = %s, want %s", e.svc.traceID, want)
	}
}
