package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
	"grab-mini/pkg/outbox"
)

const maxCancelReasonLength = 200

// readCancelInfo works out who is cancelling (from the token's role) and the
// optional reason in the body. A bad body is answered here (and false returned).
func readCancelInfo(w http.ResponseWriter, r *http.Request, c auth.Identity) (cancelInfo, bool) {
	var body struct {
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return cancelInfo{}, false
	}
	reason := strings.TrimSpace(body.Reason)
	if utf8.RuneCountInString(reason) > maxCancelReasonLength {
		http.Error(w, `{"error":"reason is at most 200 characters"}`, http.StatusBadRequest)
		return cancelInfo{}, false
	}
	by := events.CancelledByRider
	if c.Role == auth.Driver {
		by = events.CancelledByDriver
	}
	return cancelInfo{By: by, Reason: reason}, true
}

// runOrderCancelledConsumer cancels the trip of an order the rider cancelled
// (order.cancelled), for the case where a driver was matched just before the
// rider tapped cancel.
func runOrderCancelledConsumer(ctx context.Context, env Env, s *server, codec *eventcodec.Codec) {
	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:    env.KafkaBrokers,
		GroupID:    "trip-order-cancelled-group",
		Topic:      events.TopicOrderCancelled,
		LogLabel:   "[trip-order-cancelled]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}, func(ctx context.Context, msg kafka.Message) error {
		var evt events.OrderCancelled
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return s.handleOrderCancelled(ctx, evt)
	})
}

// errTripAppeared asks for a retry: the trip for the order was created after
// we looked for it, so the status event we prepared doesn't name it.
var errTripAppeared = errors.New("the trip appeared while its order was being cancelled; retrying")

// handleOrderCancelled remembers that the order is cancelled and cancels the
// trip already created for it, if any. It converges when redelivered:
//   - a trip that is already CANCELLED (or COMPLETED) is left alone, and no
//     second trip.status.changed is queued;
//   - no trip yet: the order is remembered, so that when order.matched
//     arrives late handleOrderMatched doesn't create one.
//
// Both sides take the same per-order advisory lock, so "the trip is created"
// and "the order is cancelled" can't pass each other unnoticed.
func (s *server) handleOrderCancelled(ctx context.Context, evt events.OrderCancelled) error {
	t, err := getTripByOrderID(ctx, s.db, evt.OrderID)
	if errors.Is(err, errTripNotFound) {
		t = nil
	} else if err != nil {
		return err
	}

	by := evt.CancelledBy
	if by == "" {
		by = events.CancelledByRider // only riders cancel orders
	}
	info := cancelInfo{By: by, Reason: evt.Reason}

	// Prepared outside the transaction (it may call the schema registry).
	var payload []byte
	if t != nil && (t.Status == events.TripStatusAssigned || t.Status == events.TripStatusInProgress) {
		if payload, err = s.encodeStatusChanged(ctx, t, events.TripStatusCancelled, info); err != nil {
			return err
		}
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := lockOrder(ctx, tx, evt.OrderID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO cancelled_orders (order_id) VALUES ($1) ON CONFLICT DO NOTHING`, evt.OrderID); err != nil {
		return err
	}

	var tripID string
	err = tx.QueryRow(ctx, `SELECT id FROM trips WHERE order_id = $1`, evt.OrderID).Scan(&tripID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return tx.Commit(ctx) // nothing to cancel; the order is remembered
	case err != nil:
		return err
	}
	if t == nil || t.ID != tripID {
		// Remember the order, then try again with the trip in hand.
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return errTripAppeared
	}
	if payload == nil {
		return tx.Commit(ctx) // already CANCELLED or COMPLETED: nothing to do
	}

	moved, err := transitionTrip(ctx, tx, tripID,
		[]events.TripStatus{events.TripStatusAssigned, events.TripStatusInProgress},
		events.TripStatusCancelled, cancelledAt, info)
	if err != nil {
		return err
	}
	if moved {
		if err := outbox.Add(ctx, tx, outboxSource, events.TopicTripStatusChanged, []byte(tripID), payload); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if moved {
		s.relay.Wake()
		tripTransitions.WithLabelValues(string(events.TripStatusCancelled)).Inc()
		log.Printf("trip=%s cancelled because order=%s was cancelled by the %s", tripID, evt.OrderID, info.By)
	}
	return nil
}

// lockOrder serializes work on one order for the rest of the transaction.
func lockOrder(ctx context.Context, db execer, orderID string) error {
	_, err := db.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "order:"+orderID)
	return err
}

// orderIsCancelled says whether order.cancelled was seen for the order.
func orderIsCancelled(ctx context.Context, db queryRower, orderID string) (bool, error) {
	var cancelled bool
	err := db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM cancelled_orders WHERE order_id = $1)`, orderID).Scan(&cancelled)
	if err != nil {
		return false, fmt.Errorf("check cancelled order %s: %w", orderID, err)
	}
	return cancelled, nil
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var _ queryRower = (*pgxpool.Pool)(nil)
