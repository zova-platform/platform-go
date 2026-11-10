package run

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/zova-platform/platform-go/health"
)

const DefaultShutdownTimeout = 30 * time.Second

type Options struct {
	Logger *slog.Logger

	ShutdownTimeout time.Duration

	Health *health.Health
}

type Runner struct {
	log     *slog.Logger
	timeout time.Duration
	health  *health.Health

	servers []server
	hooks   []hook
}

type server struct {
	name  string
	serve func() error
	stop  func(ctx context.Context) error
}

type hook struct {
	name string
	fn   func(ctx context.Context) error
}

func New(opts Options) *Runner {
	r := &Runner{log: opts.Logger, timeout: opts.ShutdownTimeout, health: opts.Health}
	if r.log == nil {
		r.log = slog.Default()
	}
	if r.timeout <= 0 {
		r.timeout = DefaultShutdownTimeout
	}
	return r
}

func (r *Runner) AddHTTP(name string, srv *http.Server, lis net.Listener) {
	r.servers = append(r.servers, server{
		name: name,
		serve: func() error {
			if err := srv.Serve(lis); !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		},
		stop: func(ctx context.Context) error {
			err := srv.Shutdown(ctx)
			if err != nil {
				_ = srv.Close()
			}
			return err
		},
	})
}

func (r *Runner) AddGRPC(name string, srv *grpc.Server, lis net.Listener) {
	r.servers = append(r.servers, server{
		name:  name,
		serve: func() error { return srv.Serve(lis) },
		stop: func(ctx context.Context) error {
			done := make(chan struct{})
			go func() {
				srv.GracefulStop()
				close(done)
			}()
			select {
			case <-done:
				return nil
			case <-ctx.Done():

				srv.Stop()
				return ctx.Err()
			}
		},
	})
}

func (r *Runner) OnShutdown(name string, fn func(ctx context.Context) error) {
	r.hooks = append(r.hooks, hook{name: name, fn: fn})
}

func (r *Runner) Run(ctx context.Context) error {
	ctx, stopSignals := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	var served sync.WaitGroup
	failed := make(chan error, len(r.servers))
	for _, s := range r.servers {
		served.Add(1)
		go func() {
			defer served.Done()
			r.log.Info("server starting", "server", s.name)
			if err := s.serve(); err != nil {
				failed <- fmt.Errorf("run: server %s: %w", s.name, err)
			}
		}()
	}

	var runErr error
	select {
	case <-ctx.Done():
		r.log.Info("shutdown requested")
	case runErr = <-failed:
		r.log.Error("server failed, shutting down", "error", runErr)
	}

	errs := []error{runErr}
	errs = append(errs, r.shutdown()...)
	served.Wait()
	return errors.Join(errs...)
}

func (r *Runner) shutdown() []error {
	if r.health != nil {
		r.health.SetNotReady()
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)
	add := func(err error) {
		if err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
		}
	}

	for _, s := range r.servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.stop(ctx); err != nil {
				add(fmt.Errorf("run: stop %s: %w", s.name, err))
			}
		}()
	}
	wg.Wait()

	for i := len(r.hooks) - 1; i >= 0; i-- {
		h := r.hooks[i]
		if err := h.fn(ctx); err != nil {
			add(fmt.Errorf("run: hook %s: %w", h.name, err))
		}
	}
	r.log.Info("shutdown complete")
	return errs
}
