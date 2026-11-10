package grpcx

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func logCall(ctx context.Context, log *slog.Logger, side, method string, start time.Time, err error) {
	code := status.Code(err)
	level := slog.LevelInfo
	switch code {
	case codes.OK:
	case codes.Unknown, codes.Internal, codes.DataLoss, codes.Unavailable, codes.DeadlineExceeded:
		level = slog.LevelError
	default:
		level = slog.LevelWarn
	}
	attrs := []slog.Attr{
		slog.String("side", side),
		slog.String("method", method),
		slog.String("code", code.String()),
		slog.Duration("duration", time.Since(start)),
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	log.LogAttrs(ctx, level, "grpc call", attrs...)
}

func recovered(ctx context.Context, log *slog.Logger, method string, p any) error {
	log.LogAttrs(ctx, slog.LevelError, "grpc handler panic",
		slog.String("method", method),
		slog.Any("panic", p),
		slog.String("stack", string(debug.Stack())),
	)
	return status.Error(codes.Internal, "internal error")
}

func serverLogUnary(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := h(ctx, req)
		logCall(ctx, log, "server", info.FullMethod, start, err)
		return resp, err
	}
}

func serverLogStream(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		start := time.Now()
		err := h(srv, ss)
		logCall(ss.Context(), log, "server", info.FullMethod, start, err)
		return err
	}
}

func serverRecoverUnary(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if p := recover(); p != nil {
				resp, err = nil, recovered(ctx, log, info.FullMethod, p)
			}
		}()
		return h(ctx, req)
	}
}

func serverRecoverStream(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = recovered(ss.Context(), log, info.FullMethod, p)
			}
		}()
		return h(srv, ss)
	}
}

func serverTimeoutUnary(d time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		ctx, cancel := withDefaultTimeout(ctx, d)
		defer cancel()
		return h(ctx, req)
	}
}

func clientLogUnary(log *slog.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		logCall(ctx, log, "client", method, start, err)
		return err
	}
}

func clientLogStream(log *slog.Logger) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		start := time.Now()
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			logCall(ctx, log, "client", method, start, err)
		}
		return cs, err
	}
}

func clientTimeoutUnary(d time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, cancel := withDefaultTimeout(ctx, d)
		defer cancel()
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func withDefaultTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}
