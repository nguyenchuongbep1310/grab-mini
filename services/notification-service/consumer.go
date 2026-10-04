package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

// decoder is the part of *eventcodec.Codec the handlers need.
type decoder interface {
	Decode(ctx context.Context, topic string, data []byte, v any) error
}

// runConsumer implements tasks 4.2/4.3: one consumer group subscribed to
// every topic the Notification Service cares about, dispatching each
// event to the hub so it reaches the right rider/driver socket, on
// whichever instance holds it.
//
// Notifications are best-effort and the apps recover over REST, so a failure
// is retried once quickly and then dead-lettered: a notification retried
// minutes later would only show a stale status.
func runConsumer(ctx context.Context, env Env, h *hub) {
	codec := eventcodec.New(env.SchemaRegistryURL)

	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers: env.KafkaBrokers,
		GroupID: "notification-service-group",
		Topics: []string{
			events.TopicOrderCreated,
			events.TopicOrderMatched,
			events.TopicOrderNoDriverFound,
			events.TopicTripStatusChanged,
			events.TopicDriverLocationUpdate,
			events.TopicTripRated,
		},
		MaxRetries: 1,
		RetryDelay: 500 * time.Millisecond,
		LogLabel:   "[notification-service]",
	}, func(ctx context.Context, msg kafka.Message) error {
		return handleMessage(ctx, codec, h, msg)
	})
}

// riderOf is the rider an event is for: the one it names, or else the one
// remembered from order.created (events from before they carried it).
func riderOf(ctx context.Context, h *hub, riderID, orderID string) (string, error) {
	if riderID != "" {
		return riderID, nil
	}
	return h.riderForOrder(ctx, orderID)
}

func handleMessage(ctx context.Context, codec decoder, h *hub, msg kafka.Message) error {
	switch msg.Topic {
	case events.TopicOrderCreated:
		var evt events.OrderCreated
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.rememberOrderRider(ctx, evt.OrderID, evt.RiderID)

	case events.TopicOrderMatched:
		var evt events.OrderMatched
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		riderID, err := riderOf(ctx, h, evt.RiderID, evt.OrderID)
		if err != nil {
			return err
		}
		if riderID != "" {
			if err := h.rememberDriverRider(ctx, evt.DriverID, riderID); err != nil {
				return err
			}
			if err := h.pushToRider(ctx, riderID, message{Type: events.TopicOrderMatched, Payload: evt}); err != nil {
				return err
			}
		}
		return h.pushToDriver(ctx, evt.DriverID, message{Type: events.TopicOrderMatched, Payload: evt})

	case events.TopicOrderNoDriverFound:
		var evt events.OrderNoDriverFound
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		riderID, err := riderOf(ctx, h, evt.RiderID, evt.OrderID)
		if err != nil || riderID == "" {
			return err
		}
		return h.pushToRider(ctx, riderID, message{Type: events.TopicOrderNoDriverFound, Payload: evt})

	case events.TopicTripStatusChanged:
		var evt events.TripStatusChanged
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		riderID, err := riderOf(ctx, h, evt.RiderID, evt.OrderID)
		if err != nil {
			return err
		}
		if riderID != "" {
			if err := h.pushToRider(ctx, riderID, message{Type: events.TopicTripStatusChanged, Payload: evt}); err != nil {
				return err
			}
		}
		if err := h.pushToDriver(ctx, evt.DriverID, message{Type: events.TopicTripStatusChanged, Payload: evt}); err != nil {
			return err
		}
		if evt.Status == events.TripStatusCompleted || evt.Status == events.TripStatusCancelled {
			return h.forgetDriverRider(ctx, evt.DriverID)
		}

	case events.TopicDriverLocationUpdate:
		var evt events.DriverLocationUpdated
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		// task 4.3: push to the rider watching this driver's current trip.
		riderID, err := h.riderForDriver(ctx, evt.DriverID)
		if err != nil {
			return err
		}
		if riderID != "" {
			if err := h.pushToRider(ctx, riderID, message{Type: events.TopicDriverLocationUpdate, Payload: evt}); err != nil {
				return err
			}
		}
		// Echo to the driver's own app so it can show where it is.
		return h.pushToDriver(ctx, evt.DriverID, message{Type: events.TopicDriverLocationUpdate, Payload: evt})

	case events.TopicTripRated:
		var evt events.TripRated
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.pushToDriver(ctx, evt.DriverID, message{Type: events.TopicTripRated, Payload: evt})

	default:
		return fmt.Errorf("unhandled topic %q", msg.Topic)
	}

	return nil
}
