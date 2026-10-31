package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

// trip.status.changed is the only topic this service reads: it is how it learns
// who is on a trip and when the trip ended.
var consumedTopics = []string{events.TopicTripStatusChanged}

// decoder reads an event's Avro wire bytes (*eventcodec.Codec).
type decoder interface {
	Decode(ctx context.Context, topic string, data []byte, v any) error
}

var _ decoder = (*eventcodec.Codec)(nil)

// handle is the consumer's handler. A database blip is retried through the
// retry tiers; only an event that can never be applied (invalidEvent) or cannot
// be decoded skips to the DLQ.
func (s *service) handle(ctx context.Context, dec decoder, msg kafka.Message) error {
	switch msg.Topic {
	case events.TopicTripStatusChanged:
		var evt events.TripStatusChanged
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return s.onTripStatusChanged(ctx, evt)
	}
	return fmt.Errorf("unexpected topic %q", msg.Topic)
}

// runConsumer reads the topics under one group; KEDA scales on it and
// scripts/kafka-acls.sh grants it. groupOverride is for tests, which need a
// group of their own.
func (s *service) runConsumer(ctx context.Context, env Env, groupOverride string, dec decoder) {
	cfg := kafkaconsumer.Config{
		Brokers:    env.KafkaBrokers,
		GroupID:    "chat-service-group",
		Topics:     consumedTopics,
		LogLabel:   "[chat-service]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}
	if groupOverride != "" {
		cfg.GroupID = groupOverride
	}
	kafkaconsumer.Run(ctx, cfg, func(ctx context.Context, msg kafka.Message) error {
		return s.handle(ctx, dec, msg)
	})
}
