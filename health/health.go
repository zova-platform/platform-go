package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type CheckFunc func(ctx context.Context) error

type Options struct {
	CheckTimeout time.Duration

	Gatherer prometheus.Gatherer
}

type Health struct {
	timeout  time.Duration
	gatherer prometheus.Gatherer

	mu     sync.RWMutex
	checks map[string]CheckFunc

	notReady atomic.Bool
}

func New(opts Options) *Health {
	if opts.CheckTimeout <= 0 {
		opts.CheckTimeout = 2 * time.Second
	}
	if opts.Gatherer == nil {
		opts.Gatherer = prometheus.DefaultGatherer
	}
	return &Health{
		timeout:  opts.CheckTimeout,
		gatherer: opts.Gatherer,
		checks:   make(map[string]CheckFunc),
	}
}

func (h *Health) AddReadiness(name string, check CheckFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks[name] = check
}

func (h *Health) SetNotReady() { h.notReady.Store(true) }

func (h *Health) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /readyz", h.readyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(h.gatherer, promhttp.HandlerOpts{}))
	return mux
}

type report struct {
	Status string            `json:"status"`
	Failed map[string]string `json:"failed,omitempty"`
}

func (h *Health) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, report{Status: "ok"})
}

func (h *Health) readyz(w http.ResponseWriter, r *http.Request) {
	if h.notReady.Load() {
		writeJSON(w, http.StatusServiceUnavailable, report{Status: "shutting down"})
		return
	}

	h.mu.RLock()
	names := make([]string, 0, len(h.checks))
	for name := range h.checks {
		names = append(names, name)
	}
	checks := make(map[string]CheckFunc, len(h.checks))
	for name, c := range h.checks {
		checks[name] = c
	}
	h.mu.RUnlock()
	sort.Strings(names)

	failed := map[string]string{}
	var fmu sync.Mutex
	var wg sync.WaitGroup
	for _, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
			defer cancel()
			if err := checks[name](ctx); err != nil {
				fmu.Lock()
				failed[name] = err.Error()
				fmu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(failed) > 0 {
		writeJSON(w, http.StatusServiceUnavailable, report{Status: "unavailable", Failed: failed})
		return
	}
	writeJSON(w, http.StatusOK, report{Status: "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
