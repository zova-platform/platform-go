package run_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/zova-platform/platform-go/health"
	"github.com/zova-platform/platform-go/run"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func listen(t *testing.T) net.Listener {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return lis
}

func accepting(t *testing.T, addr string) bool {
	t.Helper()
	d := net.Dialer{Timeout: 200 * time.Millisecond}
	conn, err := d.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func httpGet(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

func readyz(t *testing.T, h *health.Health) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/readyz", nil))
	return rec.Code
}

func start(t *testing.T, r *run.Runner) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not return")
			return nil
		}
	}
}

func TestCleanShutdownStopsServersAndRunsHooksInReverse(t *testing.T) {
	httpLis, grpcLis := listen(t), listen(t)
	r := run.New(run.Options{Logger: quiet()})
	r.AddHTTP("http", &http.Server{Handler: http.NewServeMux()}, httpLis)
	g := grpc.NewServer()
	healthpb.RegisterHealthServer(g, healthgrpc.NewServer())
	r.AddGRPC("grpc", g, grpcLis)

	var mu sync.Mutex
	var order []string
	for _, name := range []string{"first", "second", "third"} {
		r.OnShutdown(name, func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, name)
			return nil
		})
	}

	stop := start(t, r)
	time.Sleep(50 * time.Millisecond)

	if err := stop(); err != nil {
		t.Fatalf("clean shutdown returned %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "third" || order[1] != "second" || order[2] != "first" {
		t.Fatalf("hooks ran in order %v, want third, second, first", order)
	}
	if accepting(t, httpLis.Addr().String()) {
		t.Fatal("http listener still accepting after shutdown")
	}
	if accepting(t, grpcLis.Addr().String()) {
		t.Fatal("grpc listener still accepting after shutdown")
	}
}

func TestInFlightHTTPRequestCompletes(t *testing.T) {
	lis := listen(t)
	started := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(200 * time.Millisecond)
		_, _ = io.WriteString(w, "done")
	})
	r := run.New(run.Options{Logger: quiet()})
	r.AddHTTP("http", &http.Server{Handler: mux}, lis)
	stop := start(t, r)

	type result struct {
		body string
		err  error
	}
	res := make(chan result, 1)
	go func() {
		body, err := httpGet(context.Background(), "http://"+lis.Addr().String()+"/slow")
		res <- result{body: body, err: err}
	}()

	<-started
	if err := stop(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	got := <-res
	if got.err != nil || got.body != "done" {
		t.Fatalf("in-flight request was cut off: body=%q err=%v", got.body, got.err)
	}
}

func TestReadinessFlipsOnShutdown(t *testing.T) {
	h := health.New(health.Options{})
	r := run.New(run.Options{Logger: quiet(), Health: h})
	stop := start(t, r)

	if code := readyz(t, h); code != http.StatusOK {
		t.Fatalf("readyz before shutdown = %d, want 200", code)
	}

	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if code := readyz(t, h); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz after shutdown = %d, want 503", code)
	}
}

func TestServerFailureStopsEverythingAndReturnsError(t *testing.T) {
	bad := listen(t)
	_ = bad.Close()

	var hookRan bool
	r := run.New(run.Options{Logger: quiet()})
	r.AddHTTP("broken", &http.Server{Handler: http.NewServeMux()}, bad)
	r.OnShutdown("hook", func(context.Context) error { hookRan = true; return nil })

	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want an error naming the failed server")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after a server failed")
	}
	if !hookRan {
		t.Fatal("shutdown hooks must still run after a server failure")
	}
}

func TestHookErrorIsReturned(t *testing.T) {
	boom := errors.New("boom")
	r := run.New(run.Options{Logger: quiet()})
	r.OnShutdown("db", func(context.Context) error { return boom })

	if err := start(t, r)(); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap boom", err)
	}
}

func TestShutdownTimeoutForcesStop(t *testing.T) {
	httpLis, grpcLis := listen(t), listen(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	started := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/hang", func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release
	})
	r := run.New(run.Options{Logger: quiet(), ShutdownTimeout: 100 * time.Millisecond})
	r.AddHTTP("http", &http.Server{Handler: mux}, httpLis)

	g := grpc.NewServer()
	healthpb.RegisterHealthServer(g, &hangingHealth{release: release})
	r.AddGRPC("grpc", g, grpcLis)

	stop := start(t, r)

	go func() { _, _ = httpGet(context.Background(), "http://"+httpLis.Addr().String()+"/hang") }()
	conn, err := grpc.NewClient(grpcLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	go func() {
		_, _ = healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	}()
	<-started
	time.Sleep(100 * time.Millisecond)

	begin := time.Now()
	err = stop()
	if err == nil {
		t.Fatal("want a timeout error when calls do not finish")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if time.Since(begin) > 5*time.Second {
		t.Fatal("shutdown was not forced after the timeout")
	}
}

type hangingHealth struct {
	healthpb.UnimplementedHealthServer
	release chan struct{}
}

func (h *hangingHealth) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	<-h.release
	return &healthpb.HealthCheckResponse{}, nil
}
