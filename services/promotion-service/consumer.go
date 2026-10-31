package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

// order.cancelled, trip.status.changed (CANCELLED) and order.no_driver_found are
// the three ways an order ends without a ride; each releases its redemption.
var consumedTopics = []string{
	events.TopicOrderCreated, events.TopicOrderCancelled,
	events.TopicTripStatusChanged, events.TopicOrderNoDriverFound,
}

// decoder reads an event's Avro wire bytes (*eventcodec.Codec).
type decoder interface {
	Decode(ctx context.Context, topic string, data []byte, v any) error
}

var _ decoder = (*eventcodec.Codec)(nil)

// handle is the consumer's handler: it decodes the message by topic and passes
// it on. A database blip is retried through the retry tiers; only an event that
// can never be applied (invalidEvent) or cannot be decoded skips to the DLQ.
func (s *service) handle(ctx context.Context, dec decoder, msg kafka.Message) error {
	switch msg.Topic {
	case events.TopicOrderCreated:
		var evt events.OrderCreated
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return s.onOrderCreated(ctx, evt)
	case events.TopicOrderCancelled:
		var evt events.OrderCancelled
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return s.onOrderCancelled(ctx, evt)
	case events.TopicTripStatusChanged:
		var evt events.TripStatusChanged
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return s.onTripStatusChanged(ctx, evt)
	case events.TopicOrderNoDriverFound:
		var evt events.OrderNoDriverFound
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return s.onNoDriverFound(ctx, evt)
	}
	return fmt.Errorf("unexpected topic %q", msg.Topic)
}

// runConsumer reads all the topics under one group; KEDA scales on it and
// scripts/kafka-acls.sh grants it. groupOverride is for tests, which need a
// group of their own.
func (s *service) runConsumer(ctx context.Context, env Env, groupOverride string, dec decoder) {
	cfg := kafkaconsumer.Config{
		Brokers:    env.KafkaBrokers,
		GroupID:    "promotion-service-group",
		Topics:     consumedTopics,
		LogLabel:   "[promotion-service]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}
	if groupOverride != "" {
		cfg.GroupID = groupOverride
	}
	kafkaconsumer.Run(ctx, cfg, func(ctx context.Context, msg kafka.Message) error {
		return s.handle(ctx, dec, msg)
	})
}
