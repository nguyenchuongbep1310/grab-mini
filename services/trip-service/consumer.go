package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
)

// runOrderMatchedConsumer implements task 2.1: for every order.matched
// event, create a trip in ASSIGNED status. Deduping on eventId (US-6.1)
// means a redelivered event is a safe no-op rather than a second trip.
func runOrderMatchedConsumer(ctx context.Context, env Env, db *pgxpool.Pool) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: env.KafkaBrokers,
		GroupID: "trip-service-group",
		Topic:   events.TopicOrderMatched,
	})
	defer reader.Close()

	log.Println("trip-service order.matched consumer running")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatalf("failed to fetch message: %v", err)
		}

		var evt events.OrderMatched
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("failed to unmarshal order.matched: %v", err)
			continue
		}

		log.Printf("received order.matched partition=%d offset=%d orderId=%s driverId=%s",
			msg.Partition, msg.Offset, evt.OrderID, evt.DriverID)

		if err := handleOrderMatched(ctx, db, evt); err != nil {
			log.Printf("failed to handle order.matched for order %s: %v", evt.OrderID, err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("failed to commit offset: %v", err)
		}
	}
}

func handleOrderMatched(ctx context.Context, db *pgxpool.Pool, evt events.OrderMatched) error {
	firstTime, err := markEventProcessed(ctx, db, evt.EventID)
	if err != nil {
		return err
	}
	if !firstTime {
		log.Printf("duplicate order.matched event %s, skipping", evt.EventID)
		return nil
	}

	tripID := uuid.NewString()
	tag, err := db.Exec(ctx,
		`INSERT INTO trips (id, order_id, driver_id, status) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (order_id) DO NOTHING`,
		tripID, evt.OrderID, evt.DriverID, events.TripStatusAssigned,
	)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		log.Printf("trip already exists for order=%s, skipping (order_id safety net)", evt.OrderID)
		return nil
	}

	log.Printf("created trip=%s for order=%s driver=%s", tripID, evt.OrderID, evt.DriverID)
	return nil
}
