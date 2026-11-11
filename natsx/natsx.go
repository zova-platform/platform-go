package natsx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/zova-platform/proto/gen/go/zova/common/v1"
)

const EventVersion = 1

type Config struct {
	URL  string
	Name string
}

type StreamConfig struct {
	Name     string
	Subjects []string
	MaxAge   time.Duration
}

type Event interface {
	proto.Message
	GetMeta() *commonv1.EventMeta
}

type Client struct {
	nc *nats.Conn
	js jetstream.JetStream
}

func Connect(cfg Config) (*Client, error) {
	if cfg.URL == "" {
		return nil, errors.New("natsx: URL is required")
	}
	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500 * time.Millisecond),
		nats.Timeout(5 * time.Second),
	}
	if cfg.Name != "" {
		opts = append(opts, nats.Name(cfg.Name))
	}
	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("natsx: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("natsx: jetstream: %w", err)
	}
	return &Client{nc: nc, js: js}, nil
}

func (c *Client) Close() {
	_ = c.nc.Drain()
}

func (c *Client) Ready(ctx context.Context) error {
	if status := c.nc.Status(); status != nats.CONNECTED {
		return fmt.Errorf("natsx: connection is %s", status)
	}
	if _, err := c.js.AccountInfo(ctx); err != nil {
		return fmt.Errorf("natsx: jetstream: %w", err)
	}
	return nil
}

func (c *Client) EnsureStream(ctx context.Context, cfg StreamConfig) error {
	if cfg.Name == "" || len(cfg.Subjects) == 0 {
		return errors.New("natsx: stream Name and Subjects are required")
	}
	_, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     cfg.Name,
		Subjects: cfg.Subjects,
		MaxAge:   cfg.MaxAge,
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("natsx: ensure stream %s: %w", cfg.Name, err)
	}
	return nil
}

func NewMeta() *commonv1.EventMeta {
	return &commonv1.EventMeta{
		EventId:    uuid.Must(uuid.NewV7()).String(),
		OccurredAt: timestamppb.Now(),
		Version:    EventVersion,
	}
}
