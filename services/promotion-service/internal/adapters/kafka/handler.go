// Package kafka is promotion-service's Kafka adapter: it decodes what the
// service consumes into use case commands, and encodes what it publishes.
package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
	"grab-mini/promotion-service/internal/app"
)

// ConsumedTopics: order.cancelled, trip.status.changed (CANCELLED) and
// order.no_driver_found are the three ways an order ends without a ride; each
// releases its redemption.
var ConsumedTopics = []string{
	events.TopicOrderCreated, events.TopicOrderCancelled,
	events.TopicTripStatusChanged, events.TopicOrderNoDriverFound,
}

// Decoder reads an event's Avro wire bytes (*eventcodec.Codec).
type Decoder interface {
	Decode(ctx context.Context, topic string, data []byte, v any) error
}

var _ Decoder = (*eventcodec.Codec)(nil)

// Handler feeds consumed events to the use cases.
type Handler struct {
	svc *app.Service
	dec Decoder
}

// NewHandler returns a Handler over svc.
func NewHandler(svc *app.Service, dec Decoder) *Handler { return &Handler{svc: svc, dec: dec} }

// Handle decodes the message by topic and passes it on. A database blip is
// retried through the retry tiers; only an event that can never be applied
// (domain.InvalidEvent) or cannot be decoded skips to the DLQ.
func (h *Handler) Handle(ctx context.Context, msg kafkago.Message) error {
	switch msg.Topic {
	case events.TopicOrderCreated:
		var evt events.OrderCreated
		if err := h.dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.svc.RedeemOrder(ctx, app.RedeemOrderCommand{
			EventID: evt.EventID, OrderID: evt.OrderID, RiderID: evt.RiderID,
			PromoCode: evt.PromoCode, Discount: evt.Discount,
		})
	case events.TopicOrderCancelled:
		var evt events.OrderCancelled
		if err := h.dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.svc.OnOrderCancelled(ctx, app.ReleaseCommand{EventID: evt.EventID, OrderID: evt.OrderID})
	case events.TopicTripStatusChanged:
		var evt events.TripStatusChanged
		if err := h.dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.svc.OnTripStatusChanged(ctx, app.ReleaseCommand{
			EventID: evt.EventID, OrderID: evt.OrderID, Status: string(evt.Status),
		})
	case events.TopicOrderNoDriverFound:
		var evt events.OrderNoDriverFound
		if err := h.dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.svc.OnNoDriverFound(ctx, app.ReleaseCommand{EventID: evt.EventID, OrderID: evt.OrderID})
	}
	return fmt.Errorf("unexpected topic %q", msg.Topic)
}

// Run reads all the topics under one group; KEDA scales on it and
// scripts/kafka-acls.sh grants it. groupOverride is for tests, which need a
// group of their own.
func (h *Handler) Run(ctx context.Context, brokers []string, groupOverride string) {
	cfg := kafkaconsumer.Config{
		Brokers:    brokers,
		GroupID:    "promotion-service-group", // the literal is read by deploy/k8s/check.py
		Topics:     ConsumedTopics,
		LogLabel:   "[promotion-service]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}
	if groupOverride != "" {
		cfg.GroupID = groupOverride
	}
	kafkaconsumer.Run(ctx, cfg, h.Handle)
}
