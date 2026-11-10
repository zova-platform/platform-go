package logx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/zova-platform/platform-go/logx"
)

func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("log line is not JSON: %v: %q", err, buf.String())
	}
	return m
}

func TestAddsTraceIDFromContext(t *testing.T) {
	var buf bytes.Buffer
	log := logx.New(logx.Options{Writer: &buf})

	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("1112131415161718")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled,
	}))

	log.InfoContext(ctx, "hello", "k", "v")

	m := decode(t, &buf)
	if m["trace_id"] != tid.String() || m["span_id"] != sid.String() {
		t.Fatalf("missing trace ids: %v", m)
	}
	if m["msg"] != "hello" || m["k"] != "v" {
		t.Fatalf("unexpected record: %v", m)
	}
}

func TestNoTraceIDWithoutSpan(t *testing.T) {
	var buf bytes.Buffer
	log := logx.New(logx.Options{Writer: &buf})

	log.InfoContext(context.Background(), "hello")

	m := decode(t, &buf)
	if _, ok := m["trace_id"]; ok {
		t.Fatalf("trace_id must be absent: %v", m)
	}
}

func TestTraceIDKeptAfterWith(t *testing.T) {
	var buf bytes.Buffer
	log := logx.New(logx.Options{Writer: &buf}).With("svc", "trip").WithGroup("g")

	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("1112131415161718")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid,
	}))
	log.InfoContext(ctx, "hello")

	m := decode(t, &buf)
	g, _ := m["g"].(map[string]any)
	if m["svc"] != "trip" || g["trace_id"] != tid.String() {
		t.Fatalf("unexpected record: %v", m)
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	log := logx.New(logx.Options{Writer: &buf, Level: slog.LevelWarn})

	log.Info("hidden")
	if buf.Len() != 0 {
		t.Fatalf("info must be filtered: %q", buf.String())
	}
	log.Warn("shown")
	if buf.Len() == 0 {
		t.Fatal("warn must be logged")
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"": slog.LevelInfo, "debug": slog.LevelDebug, "INFO": slog.LevelInfo,
		"warn": slog.LevelWarn, "error": slog.LevelError,
	}
	for in, want := range cases {
		got, err := logx.ParseLevel(in)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := logx.ParseLevel("loud"); err == nil {
		t.Error("want error for unknown level")
	}
}
