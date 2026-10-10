package main

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
)

// badOutcome is an event that can never be applied (its order id isn't a
// UUID). Permanent, so it goes straight to the dead-letter topic.
type badOutcome struct{ reason string }

func (e *badOutcome) Error() string   { return "unusable matching outcome: " + e.reason }
func (e *badOutcome) Permanent() bool { return true }

// runOutcomeConsumer keeps orders.status honest. Order Service publishes
// order.created and then knows nothing, so without this an order would read
// PENDING forever, and a rider who reloads couldn't tell "still searching"
// from "matched" or "nobody available".
func runOutcomeConsumer(ctx context.Context, env Env, db *pgxpool.Pool, codec *eventcodec.Codec) {
	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:    env.KafkaBrokers,
		GroupID:    "order-service-group",
		Topics:     []string{events.TopicOrderMatched, events.TopicOrderNoDriverFound, events.TopicTripStatusChanged},
		LogLabel:   "[order-service]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}, func(ctx context.Context, msg kafka.Message) error {
		switch msg.Topic {
		case events.TopicOrderMatched:
			var evt events.OrderMatched
			if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
				return err
			}
			return setOutcome(ctx, db, evt.OrderID, "MATCHED")
		case events.TopicOrderNoDriverFound:
			var evt events.OrderNoDriverFound
			if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
				return err
			}
			return setOutcome(ctx, db, evt.OrderID, "NO_DRIVER")
		case events.TopicTripStatusChanged:
			var evt events.TripStatusChanged
			if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
				return err
			}
			if evt.Status != events.TripStatusCancelled {
				return nil
			}
			return cancelMatchedOrder(ctx, db, evt.OrderID, evt.CancelledBy, evt.Reason)
		}
		return fmt.Errorf("unexpected topic %q", msg.Topic)
	})
}

// setOutcome moves a PENDING order to its outcome. Only PENDING orders move,
// so a redelivered or out-of-order event is a harmless no-op.
func setOutcome(ctx context.Context, db *pgxpool.Pool, orderID, status string) error {
	if _, err := uuid.Parse(orderID); err != nil {
		return &badOutcome{"order id " + orderID + " is not a UUID"}
	}
	tag, err := db.Exec(ctx, `UPDATE orders SET status = $2 WHERE id = $1 AND status = 'PENDING'`, orderID, status)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		log.Printf("order %s -> %s", orderID, status)
	}
	return nil
}

// cancelMatchedOrder records that the trip of a MATCHED order was cancelled
// (by the rider or the driver), so the order reads CANCELLED. It is separate
// from setOutcome on purpose: that one only ever moves PENDING orders. Only
// MATCHED orders move here, so a redelivered event, or one for an order the
// rider already cancelled while PENDING, is a harmless no-op.
func cancelMatchedOrder(ctx context.Context, db *pgxpool.Pool, orderID, cancelledBy, reason string) error {
	if _, err := uuid.Parse(orderID); err != nil {
		return &badOutcome{"order id " + orderID + " is not a UUID"}
	}
	tag, err := db.Exec(ctx, `
		UPDATE orders SET status = 'CANCELLED', cancelled_at = now(), cancelled_by = $2, cancel_reason = $3
		WHERE id = $1 AND status = 'MATCHED'`, orderID, cancelledBy, reason)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		log.Printf("order %s -> CANCELLED (trip cancelled by %q)", orderID, cancelledBy)
	}
	return nil
}
