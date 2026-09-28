package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
)

// runConsumer implements tasks 4.2/4.3: one consumer group subscribed to
// every topic the Notification Service cares about, dispatching each
// event to the hub so it reaches the right rider/driver socket.
func runConsumer(ctx context.Context, env Env, h *hub) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: env.KafkaBrokers,
		GroupID: "notification-service-group",
		GroupTopics: []string{
			events.TopicOrderCreated,
			events.TopicOrderMatched,
			events.TopicOrderNoDriverFound,
			events.TopicTripStatusChanged,
			events.TopicDriverLocationUpdate,
		},
	})
	defer reader.Close()

	log.Println("notification-service consumer running")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatalf("failed to fetch message: %v", err)
		}

		handleMessage(h, msg)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("failed to commit offset: %v", err)
		}
	}
}

func handleMessage(h *hub, msg kafka.Message) {
	switch msg.Topic {
	case events.TopicOrderCreated:
		var evt events.OrderCreated
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("failed to unmarshal order.created: %v", err)
			return
		}
		// US-5.1 needs to route order.matched/no_driver_found (which
		// don't carry riderId) back to the right rider.
		h.rememberOrderRider(evt.OrderID, evt.RiderID)

	case events.TopicOrderMatched:
		var evt events.OrderMatched
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("failed to unmarshal order.matched: %v", err)
			return
		}
		if riderID, ok := h.riderForOrder(evt.OrderID); ok {
			h.rememberDriverRider(evt.DriverID, riderID)
			h.pushToRider(riderID, message{Type: events.TopicOrderMatched, Payload: evt})
		}
		h.pushToDriver(evt.DriverID, message{Type: events.TopicOrderMatched, Payload: evt})

	case events.TopicOrderNoDriverFound:
		var evt events.OrderNoDriverFound
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("failed to unmarshal order.no_driver_found: %v", err)
			return
		}
		if riderID, ok := h.riderForOrder(evt.OrderID); ok {
			h.pushToRider(riderID, message{Type: events.TopicOrderNoDriverFound, Payload: evt})
		}

	case events.TopicTripStatusChanged:
		var evt events.TripStatusChanged
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("failed to unmarshal trip.status.changed: %v", err)
			return
		}
		if riderID, ok := h.riderForOrder(evt.OrderID); ok {
			h.pushToRider(riderID, message{Type: events.TopicTripStatusChanged, Payload: evt})
		}
		h.pushToDriver(evt.DriverID, message{Type: events.TopicTripStatusChanged, Payload: evt})

		if evt.Status == events.TripStatusCompleted || evt.Status == events.TripStatusCancelled {
			h.forgetDriverRider(evt.DriverID)
		}

	case events.TopicDriverLocationUpdate:
		var evt events.DriverLocationUpdated
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("failed to unmarshal driver.location.updated: %v", err)
			return
		}
		// task 4.3: push to the rider watching this driver's current trip.
		if riderID, ok := h.riderForDriver(evt.DriverID); ok {
			h.pushToRider(riderID, message{Type: events.TopicDriverLocationUpdate, Payload: evt})
		}

	default:
		log.Printf("unhandled topic %q", msg.Topic)
	}
}
