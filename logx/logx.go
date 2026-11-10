package logx

import (
	"context"
	"io"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

type Options struct {
	Level slog.Leveler

	Writer io.Writer
}

func New(opts Options) *slog.Logger {
	if opts.Level == nil {
		opts.Level = slog.LevelInfo
	}
	if opts.Writer == nil {
		opts.Writer = os.Stdout
	}
	base := slog.NewJSONHandler(opts.Writer, &slog.HandlerOptions{Level: opts.Level})
	return slog.New(&traceHandler{Handler: base})
}

func ParseLevel(s string) (slog.Level, error) {
	if s == "" {
		return slog.LevelInfo, nil
	}
	var l slog.Level
	err := l.UnmarshalText([]byte(s))
	return l, err
}

type traceHandler struct {
	slog.Handler
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithGroup(name)}
}
