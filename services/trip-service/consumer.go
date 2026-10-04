package main

import (
	"context"
	"errors"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

// runOrderMatchedConsumer implements task 2.1: for every order.matched
// event, create a trip in ASSIGNED status. A redelivered event is a safe
// no-op rather than a second trip (US-6.1).
func runOrderMatchedConsumer(ctx context.Context, env Env, db *pgxpool.Pool, rdb *redis.Client, codec *eventcodec.Codec) {
	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:    env.KafkaBrokers,
		GroupID:    "trip-service-group",
		Topic:      events.TopicOrderMatched,
		LogLabel:   "[trip-service]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}, func(ctx context.Context, msg kafka.Message) error {
		var evt events.OrderMatched
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return handleOrderMatched(ctx, db, rdb, evt)
	})
}

// handleOrderMatched creates the trip for a matched order and confirms the
// driver as ON_TRIP, which ends the lease Matching put on them (without it
// the driver would become claimable again after a couple of minutes).
//
// It can be retried or redelivered at any point and converges:
//   - an order that already has a trip is left alone, so a replay never
//     flips a finished trip's driver back to ON_TRIP;
//   - the driver is confirmed before the trip is saved, so a failure there
//     leaves nothing recorded and the retry tries again;
//   - the eventId dedupe row and the trip are saved in one transaction, so
//     "seen" never outlives a trip that failed to save.
func handleOrderMatched(ctx context.Context, db *pgxpool.Pool, rdb *redis.Client, evt events.OrderMatched) error {
	if _, err := getTripByOrderID(ctx, db, evt.OrderID); err == nil {
		log.Printf("trip already exists for order=%s, skipping order.matched event %s", evt.OrderID, evt.EventID)
		return nil
	} else if !errors.Is(err, errTripNotFound) {
		return err
	}

	if err := driverstatus.Set(ctx, rdb, evt.DriverID, driverstatus.OnTrip); err != nil {
		return err
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	firstTime, err := markEventProcessed(ctx, tx, evt.EventID)
	if err != nil {
		return err
	}
	if !firstTime {
		log.Printf("duplicate order.matched event %s, skipping", evt.EventID)
		return nil
	}

	tripID := uuid.NewString()
	tag, err := tx.Exec(ctx,
		`INSERT INTO trips (id, order_id, driver_id, rider_id, status) VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (order_id) DO NOTHING`,
		tripID, evt.OrderID, evt.DriverID, evt.RiderID, events.TripStatusAssigned,
	)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		log.Printf("trip already exists for order=%s, skipping (order_id safety net)", evt.OrderID)
		return nil
	}
	log.Printf("created trip=%s for order=%s driver=%s", tripID, evt.OrderID, evt.DriverID)
	return nil
}

// runDriverReleaseConsumer frees the driver when a trip ends. handleComplete
// and handleCancel already do it in the request, but that step sits outside
// the transaction that commits the trip: a crash between the two used to
// leave the driver ON_TRIP forever. The trip.status.changed event is queued
// in the same transaction as the status, so it always arrives, and this is
// the step that cannot be lost.
func runDriverReleaseConsumer(ctx context.Context, env Env, db *pgxpool.Pool, rdb *redis.Client, codec *eventcodec.Codec) {
	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:    env.KafkaBrokers,
		GroupID:    "trip-driver-release-group",
		Topic:      events.TopicTripStatusChanged,
		LogLabel:   "[trip-driver-release]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}, func(ctx context.Context, msg kafka.Message) error {
		var evt events.TripStatusChanged
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return releaseDriverAfterTrip(ctx, db, rdb, evt)
	})
}

// releaseDriverAfterTrip sets the driver of a COMPLETED or CANCELLED trip back
// to AVAILABLE, unless that would be wrong. Events arrive late and again, so it
// checks the present rather than trusting the event:
//   - the driver has another trip in flight in the database: leave them;
//   - the driver isn't ON_TRIP in Redis (already released, or Matching has
//     since claimed them for a new order): leave them. The change is a
//     compare-and-set, so a claim that lands in between is never overwritten.
func releaseDriverAfterTrip(ctx context.Context, db *pgxpool.Pool, rdb *redis.Client, evt events.TripStatusChanged) error {
	if evt.Status != events.TripStatusCompleted && evt.Status != events.TripStatusCancelled {
		return nil
	}

	if _, err := getActiveTripByDriver(ctx, db, evt.DriverID); err == nil {
		return nil
	} else if !errors.Is(err, errTripNotFound) {
		return err
	}

	released, err := driverstatus.Transition(ctx, rdb, evt.DriverID, driverstatus.OnTrip, driverstatus.Available)
	if err != nil {
		return err
	}
	if released {
		log.Printf("driver=%s released after trip=%s %s", evt.DriverID, evt.TripID, evt.Status)
	}
	return nil
}
