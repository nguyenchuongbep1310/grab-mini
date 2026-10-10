package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
)

var ordersCancelled = promauto.NewCounter(prometheus.CounterOpts{
	Name: "orders_cancelled_total",
	Help: "Orders cancelled by the rider while still PENDING (saved with their order.cancelled event).",
})

const maxCancelReasonLength = 200

type cancelOrderRequest struct {
	Reason string `json:"reason"`
}

// handleCancelOrder lets a rider cancel an order that has no driver yet.
//
//   - PENDING moves to CANCELLED and order.cancelled is queued in the same
//     transaction, so Matching stops looking and any trip that raced in is cancelled.
//   - Already CANCELLED is a no-op success (a retried tap), with no second event.
//   - MATCHED is a 409: a driver is on the way, so the rider cancels the trip
//     (POST /trips/{id}/cancel) instead and Trip Service tells us.
//   - NO_DRIVER is a 409: there is nothing left to cancel.
func (s *server) handleCancelOrder(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	id := r.PathValue("id")
	// orders.id is a UUID column: a malformed id would otherwise be a Postgres error.
	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, `{"error":"order not found"}`, http.StatusNotFound)
		return
	}

	var req cancelOrderRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}
	req.Reason = strings.TrimSpace(req.Reason)
	if utf8.RuneCountInString(req.Reason) > maxCancelReasonLength {
		http.Error(w, `{"error":"reason is at most 200 characters"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	o, err := queryOrder(ctx, s.db, "id = $1", id)
	// To anyone but its rider the order doesn't exist.
	if err == nil && o.RiderID != caller.Subject {
		err = pgx.ErrNoRows
	}
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, `{"error":"order not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("failed to load order: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	if o.Status == string(events.OrderStatusPending) {
		// Encode before touching the database, like order.created.
		payload, err := s.codec.Encode(ctx, events.TopicOrderCancelled, events.OrderCancelled{
			Envelope:    events.Envelope{EventID: uuid.NewString(), OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)},
			OrderID:     o.ID,
			RiderID:     o.RiderID,
			CancelledBy: events.CancelledByRider,
			Reason:      req.Reason,
		})
		if err != nil {
			log.Printf("failed to encode order.cancelled: %v", err)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}
		cancelled, err := s.cancelPending(ctx, o.ID, req.Reason, payload)
		if err != nil {
			log.Printf("failed to cancel order: %v", err)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}
		if cancelled {
			s.relay.Wake()
			ordersCancelled.Inc()
		}
		// Not cancelled means we lost a race (matched, or cancelled by a
		// concurrent request); the status read below says which.
		if o, err = queryOrder(ctx, s.db, "id = $1", id); err != nil {
			log.Printf("failed to reload order: %v", err)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}
	}

	switch o.Status {
	case string(events.OrderStatusCancelled):
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(o)
	case string(events.OrderStatusMatched):
		writeCancelConflict(w, o.Status, "a driver is already assigned: cancel the trip instead (POST /trips/{id}/cancel)")
	default:
		writeCancelConflict(w, o.Status, "this order can no longer be cancelled")
	}
}

func writeCancelConflict(w http.ResponseWriter, status, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)
	json.NewEncoder(w).Encode(map[string]string{"error": message, "status": status})
}

// cancelPending moves a PENDING order to CANCELLED and queues order.cancelled
// in one transaction. The status check lives in the UPDATE, so against a
// concurrent order.matched exactly one of the two wins; it reports false
// (and queues nothing) if the order was no longer PENDING.
func (s *server) cancelPending(ctx context.Context, orderID, reason string, payload []byte) (bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE orders SET status = 'CANCELLED', cancelled_at = now(), cancelled_by = $2, cancel_reason = $3
		WHERE id = $1 AND status = 'PENDING'`,
		orderID, events.CancelledByRider, reason)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	if err := outbox.Add(ctx, tx, outboxSource, events.TopicOrderCancelled, []byte(orderID), payload); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
