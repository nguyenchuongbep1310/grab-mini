package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

var consumedTopics = []string{events.TopicOrderCreated, events.TopicTripStatusChanged, events.TopicTripRated}

// decoder reads an event's Avro wire bytes (*eventcodec.Codec).
type decoder interface {
	Decode(ctx context.Context, topic string, data []byte, v any) error
}

var _ decoder = (*eventcodec.Codec)(nil)

// handle is the consumer's handler: it decodes the message by topic and passes
// it on. Failures are retried through the retry tiers (a database blip, a
// provider timeout, an order.created that has not been processed yet) and only
// permanent ones (invalidEvent, amountError) skip straight to the DLQ.
func (s *service) handle(ctx context.Context, dec decoder, msg kafka.Message) error {
	switch msg.Topic {
	case events.TopicOrderCreated:
		var evt events.OrderCreated
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return s.recordOrder(ctx, evt)
	case events.TopicTripStatusChanged:
		var evt events.TripStatusChanged
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return s.onTripStatus(ctx, evt)
	case events.TopicTripRated:
		var evt events.TripRated
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return s.onTripRated(ctx, evt)
	}
	return fmt.Errorf("unexpected topic %q", msg.Topic)
}

// runConsumer reads the three topics under one group. A trip event can reach
// this service before the order.created it needs; it is then retried a few
// seconds later (the first retry tier), by which time the order has been
// recorded.
//
// One group reads all three topics; KEDA scales on it and scripts/kafka-acls.sh
// grants it. groupOverride is for tests, which need a group of their own.
func (s *service) runConsumer(ctx context.Context, env Env, groupOverride string, dec decoder) {
	cfg := kafkaconsumer.Config{
		Brokers:    env.KafkaBrokers,
		GroupID:    "payment-service-group",
		Topics:     consumedTopics,
		LogLabel:   "[payment-service]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}
	if groupOverride != "" {
		cfg.GroupID = groupOverride
	}
	kafkaconsumer.Run(ctx, cfg, func(ctx context.Context, msg kafka.Message) error {
		return s.handle(ctx, dec, msg)
	})
}
