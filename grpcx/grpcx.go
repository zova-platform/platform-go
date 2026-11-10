package grpcx

import (
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
)

const DefaultTimeout = 5 * time.Second

type ServerOptions struct {
	Logger *slog.Logger

	Timeout time.Duration
}

func NewServer(opts ServerOptions, extra ...grpc.ServerOption) *grpc.Server {
	log := loggerOrDefault(opts.Logger)
	timeout := timeoutOrDefault(opts.Timeout)

	base := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			serverLogUnary(log),
			serverRecoverUnary(log),
			serverTimeoutUnary(timeout),
		),
		grpc.ChainStreamInterceptor(
			serverLogStream(log),
			serverRecoverStream(log),
		),
	}
	return grpc.NewServer(append(base, extra...)...)
}

type ClientOptions struct {
	Logger *slog.Logger

	Timeout time.Duration

	Credentials credentials.TransportCredentials
}

func NewClient(target string, opts ClientOptions, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	log := loggerOrDefault(opts.Logger)
	timeout := timeoutOrDefault(opts.Timeout)
	creds := opts.Credentials
	if creds == nil {
		creds = insecure.NewCredentials()
	}

	base := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(
			clientLogUnary(log),
			clientTimeoutUnary(timeout),
		),
		grpc.WithChainStreamInterceptor(
			clientLogStream(log),
		),
	}
	return grpc.NewClient(target, append(base, extra...)...)
}

func loggerOrDefault(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}

func timeoutOrDefault(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultTimeout
	}
	return d
}
