// Package kafka is the event side of payment-service: it decodes the four
// topics it consumes, maps each event to an app command, and encodes the
// payment.recorded announcement. The wire types (services/pkg/events) stop here;
// the use cases never see them.
package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"grab-mini/payment-service/internal/app"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

// ConsumedTopics are the topics read under one group.
var ConsumedTopics = []string{events.TopicOrderCreated, events.TopicOrderMatched, events.TopicTripStatusChanged, events.TopicTripRated}

// Decoder reads an event's Avro wire bytes (*eventcodec.Codec).
type Decoder interface {
	Decode(ctx context.Context, topic string, data []byte, v any) error
}

var _ Decoder = (*eventcodec.Codec)(nil)

// Handler maps events to use cases.
type Handler struct {
	svc *app.Service
}

func NewHandler(svc *app.Service) *Handler { return &Handler{svc: svc} }

func (h *Handler) OnOrderCreated(ctx context.Context, evt events.OrderCreated) error {
	return h.svc.RecordOrder(ctx, app.OrderCreated{
		OrderID: evt.OrderID, RiderID: evt.RiderID, Price: evt.Price, Discount: evt.Discount,
	})
}

func (h *Handler) OnOrderMatched(ctx context.Context, evt events.OrderMatched) error {
	return h.svc.RecordAssignment(ctx, app.OrderMatched{OrderID: evt.OrderID, OccurredAt: evt.OccurredAt})
}

func (h *Handler) OnTripStatus(ctx context.Context, evt events.TripStatusChanged) error {
	return h.svc.SettleTrip(ctx, app.TripStatusChanged{
		EventID: evt.EventID, TripID: evt.TripID, OrderID: evt.OrderID, DriverID: evt.DriverID,
		RiderID: evt.RiderID, Status: string(evt.Status), CancelledBy: evt.CancelledBy, OccurredAt: evt.OccurredAt,
	})
}

func (h *Handler) OnTripRated(ctx context.Context, evt events.TripRated) error {
	return h.svc.SettleTip(ctx, app.TripRated{
		EventID: evt.EventID, TripID: evt.TripID, OrderID: evt.OrderID, DriverID: evt.DriverID,
		RiderID: evt.RiderID, TipAmount: evt.TipAmount, Currency: evt.Currency,
	})
}

// Handle is the consumer's handler: it decodes the message by topic and passes
// it on. Failures are retried through the retry tiers (a database blip, a
// provider timeout, an order.created that has not been processed yet) and only
// permanent ones (app.InvalidEventError, domain.AmountError) skip straight to the DLQ.
func (h *Handler) Handle(ctx context.Context, dec Decoder, msg kafkago.Message) error {
	switch msg.Topic {
	case events.TopicOrderCreated:
		var evt events.OrderCreated
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.OnOrderCreated(ctx, evt)
	case events.TopicOrderMatched:
		var evt events.OrderMatched
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.OnOrderMatched(ctx, evt)
	case events.TopicTripStatusChanged:
		var evt events.TripStatusChanged
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.OnTripStatus(ctx, evt)
	case events.TopicTripRated:
		var evt events.TripRated
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.OnTripRated(ctx, evt)
	}
	return fmt.Errorf("unexpected topic %q", msg.Topic)
}

// Config says where and under which group to consume.
type Config struct {
	Brokers []string
	// GroupOverride is for tests, which need a group of their own.
	GroupOverride string
}

// Run reads the four topics under one group. A trip event can reach
// this service before the order.created it needs; it is then retried a few
// seconds later (the first retry tier), by which time the order has been
// recorded.
//
// One group reads all four topics; KEDA scales on it and scripts/kafka-acls.sh
// grants it.
func (h *Handler) Run(ctx context.Context, cfg Config, dec Decoder) {
	kc := kafkaconsumer.Config{
		Brokers:    cfg.Brokers,
		GroupID:    "payment-service-group", // the literal is read by deploy/k8s/check.py
		Topics:     ConsumedTopics,
		LogLabel:   "[payment-service]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}
	if cfg.GroupOverride != "" {
		kc.GroupID = cfg.GroupOverride
	}
	kafkaconsumer.Run(ctx, kc, func(ctx context.Context, msg kafkago.Message) error {
		return h.Handle(ctx, dec, msg)
	})
}
