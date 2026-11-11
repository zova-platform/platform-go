package natsx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

const pushSubjectPrefix = "push.user."

type PushMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

func PushSubject(userID string) (string, error) {
	if userID == "" || strings.ContainsAny(userID, ".*> \t\r\n") {
		return "", fmt.Errorf("natsx: invalid user id %q for a push subject", userID)
	}
	return pushSubjectPrefix + userID, nil
}

func (c *Client) Push(ctx context.Context, userID, msgType string, data proto.Message) error {
	subject, err := PushSubject(userID)
	if err != nil {
		return err
	}
	if msgType == "" {
		return errors.New("natsx: push message type is required")
	}

	message := PushMessage{Type: msgType}
	if data != nil {
		raw, err := Marshal(data)
		if err != nil {
			return fmt.Errorf("natsx: marshal push data: %w", err)
		}
		message.Data = raw
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("natsx: marshal push message: %w", err)
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "push "+msgType,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", pushSubjectPrefix+"*"),
		),
	)
	defer span.End()

	msg := &nats.Msg{Subject: subject, Data: payload, Header: nats.Header{}}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(http.Header(msg.Header)))
	if err := c.nc.PublishMsg(msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("natsx: push to %s: %w", subject, err)
	}
	return nil
}

func (c *Client) SubscribePush(userID string, handle func(context.Context, PushMessage)) (stop func(), err error) {
	subject, err := PushSubject(userID)
	if err != nil {
		return nil, err
	}
	sub, err := c.nc.Subscribe(subject, func(msg *nats.Msg) {
		var message PushMessage
		if err := json.Unmarshal(msg.Data, &message); err != nil {
			slog.Error("natsx: dropping undecodable push message", "subject", msg.Subject, "error", err)
			return
		}
		ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(http.Header(msg.Header)))
		ctx, span := otel.Tracer(tracerName).Start(ctx, "receive "+message.Type, trace.WithSpanKind(trace.SpanKindConsumer))
		defer span.End()
		handle(ctx, message)
	})
	if err != nil {
		return nil, fmt.Errorf("natsx: subscribe %s: %w", subject, err)
	}

	var once sync.Once
	return func() { once.Do(func() { _ = sub.Unsubscribe() }) }, nil
}
