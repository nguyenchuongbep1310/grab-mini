// Package kafka is the event side of order-service: it decodes the three
// topics it consumes, maps each event to a use case, and encodes the two
// events it announces. The wire types (services/pkg/events) stop here.
package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"grab-mini/order-service/internal/app"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

// Decoder reads an event's Avro wire bytes (*eventcodec.Codec).
type Decoder interface {
	Decode(ctx context.Context, topic string, data []byte, v any) error
}

var _ Decoder = (*eventcodec.Codec)(nil)

// Handler maps the outcomes Matching and Trip Service report to use cases.
type Handler struct {
	svc   *app.Service
	codec Decoder
}

func NewHandler(svc *app.Service, codec Decoder) *Handler { return &Handler{svc: svc, codec: codec} }

// Handle applies one message. A trip status other than CANCELLED is not ours.
func (h *Handler) Handle(ctx context.Context, msg kafkago.Message) error {
	switch msg.Topic {
	case events.TopicOrderMatched:
		var evt events.OrderMatched
		if err := h.codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.svc.SetMatched(ctx, evt.OrderID, evt.DriverID)
	case events.TopicOrderNoDriverFound:
		var evt events.OrderNoDriverFound
		if err := h.codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.svc.SetNoDriver(ctx, evt.OrderID)
	case events.TopicTripStatusChanged:
		var evt events.TripStatusChanged
		if err := h.codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		if evt.Status != events.TripStatusCancelled {
			return nil
		}
		return h.svc.CancelMatchedOrder(ctx, evt.OrderID, evt.CancelledBy, evt.Reason)
	}
	return fmt.Errorf("unexpected topic %q", msg.Topic)
}

// Run keeps orders.status honest. Order Service publishes order.created and
// then knows nothing, so without this an order would read PENDING forever, and
// a rider who reloads couldn't tell "still searching" from "matched" or
// "nobody available".
func (h *Handler) Run(ctx context.Context, brokers []string) {
	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:    brokers,
		GroupID:    "order-service-group", // the literal is read by deploy/k8s/check.py
		Topics:     []string{events.TopicOrderMatched, events.TopicOrderNoDriverFound, events.TopicTripStatusChanged},
		LogLabel:   "[order-service]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}, h.Handle)
}
