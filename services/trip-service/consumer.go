package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

// runOrderMatchedConsumer implements task 2.1: for every order.matched
// event, create a trip in ASSIGNED status. Deduping on eventId (US-6.1)
// means a redelivered event is a safe no-op rather than a second trip.
func runOrderMatchedConsumer(ctx context.Context, env Env, db *pgxpool.Pool) {
	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:  env.KafkaBrokers,
		GroupID:  "trip-service-group",
		Topic:    events.TopicOrderMatched,
		LogLabel: "[trip-service]",
	}, func(ctx context.Context, msg kafka.Message) error {
		var evt events.OrderMatched
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			return err
		}
		return handleOrderMatched(ctx, db, evt)
	})
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
