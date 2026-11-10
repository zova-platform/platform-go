package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/zova-platform/platform-go/health"
)

func get(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), method, path, nil))
	return rec
}

func TestHealthzAlwaysOK(t *testing.T) {
	h := health.New(health.Options{})
	h.AddReadiness("db", func(context.Context) error { return errors.New("down") })

	rec := get(t, h.Handler(), http.MethodGet, "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", rec.Code)
	}
}

func TestReadyzWithoutChecks(t *testing.T) {
	rec := get(t, health.New(health.Options{}).Handler(), http.MethodGet, "/readyz")
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz = %d, want 200", rec.Code)
	}
}

func TestReadyzAllChecksPass(t *testing.T) {
	h := health.New(health.Options{})
	h.AddReadiness("db", func(context.Context) error { return nil })
	h.AddReadiness("nats", func(context.Context) error { return nil })

	rec := get(t, h.Handler(), http.MethodGet, "/readyz")
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz = %d, want 200", rec.Code)
	}
}

func TestReadyzReportsFailedCheck(t *testing.T) {
	h := health.New(health.Options{})
	h.AddReadiness("db", func(context.Context) error { return nil })
	h.AddReadiness("nats", func(context.Context) error { return errors.New("no connection") })

	rec := get(t, h.Handler(), http.MethodGet, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d, want 503", rec.Code)
	}
	var body struct {
		Status string            `json:"status"`
		Failed map[string]string `json:"failed"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Failed["nats"] != "no connection" || len(body.Failed) != 1 {
		t.Fatalf("unexpected failures: %v", body.Failed)
	}
}

func TestReadyzCheckTimeout(t *testing.T) {
	h := health.New(health.Options{CheckTimeout: 20 * time.Millisecond})
	h.AddReadiness("slow", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	start := time.Now()
	rec := get(t, h.Handler(), http.MethodGet, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d, want 503", rec.Code)
	}
	if time.Since(start) > time.Second {
		t.Fatal("check was not cut off by the timeout")
	}
}

func TestSetNotReady(t *testing.T) {
	h := health.New(health.Options{})
	h.SetNotReady()

	if rec := get(t, h.Handler(), http.MethodGet, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d, want 503", rec.Code)
	}
	if rec := get(t, h.Handler(), http.MethodGet, "/healthz"); rec.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200 while draining", rec.Code)
	}
}

func TestMetricsServesRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "zova_test_total", Help: "test"})
	reg.MustRegister(c)
	c.Inc()

	h := health.New(health.Options{Gatherer: reg})
	rec := get(t, h.Handler(), http.MethodGet, "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "zova_test_total 1") {
		t.Fatalf("metric missing from output:\n%s", rec.Body.String())
	}
}

func TestOnlyGETAllowed(t *testing.T) {
	rec := get(t, health.New(health.Options{}).Handler(), http.MethodPost, "/healthz")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST healthz = %d, want 405", rec.Code)
	}
}
