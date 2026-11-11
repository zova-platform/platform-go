package natsx

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const tracerName = "github.com/zova-platform/platform-go/natsx"

var (
	marshalOptions   = protojson.MarshalOptions{UseProtoNames: true}
	unmarshalOptions = protojson.UnmarshalOptions{DiscardUnknown: true}
)

func Marshal(m proto.Message) ([]byte, error) {
	return marshalOptions.Marshal(m)
}

func Unmarshal(data []byte, m proto.Message) error {
	return unmarshalOptions.Unmarshal(data, m)
}

func InjectHeaders(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier
}

func (c *Client) Publish(ctx context.Context, subject string, event Event) error {
	id := event.GetMeta().GetEventId()
	if id == "" {
		return errors.New("natsx: event meta.event_id is required, use natsx.NewMeta()")
	}
	payload, err := Marshal(event)
	if err != nil {
		return fmt.Errorf("natsx: marshal %s: %w", subject, err)
	}
	return c.PublishRaw(ctx, subject, id, payload, nil)
}

func (c *Client) PublishRaw(ctx context.Context, subject, msgID string, payload []byte, headers map[string]string) error {
	if len(headers) > 0 {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(headers))
	}
	ctx, span := otel.Tracer(tracerName).Start(ctx, "publish "+subject,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", subject),
			attribute.String("messaging.message.id", msgID),
		),
	)
	defer span.End()

	msg := &nats.Msg{Subject: subject, Data: payload, Header: nats.Header{}}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(http.Header(msg.Header)))

	if _, err := c.js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID)); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("natsx: publish %s: %w", subject, err)
	}
	return nil
}
