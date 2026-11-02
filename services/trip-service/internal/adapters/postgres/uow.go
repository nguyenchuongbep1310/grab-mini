package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
	"grab-mini/trip-service/internal/app"
	"grab-mini/trip-service/internal/domain"
)

// OutboxSource names this service in the shared outbox table.
const OutboxSource = "trip-service"

// Waker is woken after a commit that queued an outbox row (*outbox.Relay).
type Waker interface{ Wake() }

// UnitOfWork implements app.UnitOfWork: one pgx transaction, with the outbox
// rows written in it (ADR 0004).
type UnitOfWork struct {
	db    *pgxpool.Pool
	waker Waker // may be nil in tests that don't publish
}

func NewUnitOfWork(db *pgxpool.Pool, waker Waker) *UnitOfWork {
	return &UnitOfWork{db: db, waker: waker}
}

var _ app.UnitOfWork = (*UnitOfWork)(nil)

func (u *UnitOfWork) Do(ctx context.Context, fn func(app.Tx) error) error {
	tx, err := u.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	t := &txn{tx: tx}
	if err := fn(t); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if t.queued && u.waker != nil {
		u.waker.Wake()
	}
	return nil
}

type txn struct {
	tx     pgx.Tx
	queued bool
}

var _ app.Tx = (*txn)(nil)

// LockOrder serializes work on one order for the rest of the transaction.
func (t *txn) LockOrder(ctx context.Context, orderID string) error {
	_, err := t.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "order:"+orderID)
	return err
}

func (t *txn) IsOrderCancelled(ctx context.Context, orderID string) (bool, error) {
	return orderIsCancelled(ctx, t.tx, orderID)
}

func (t *txn) MarkOrderCancelled(ctx context.Context, orderID string) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO cancelled_orders (order_id) VALUES ($1) ON CONFLICT DO NOTHING`, orderID)
	return err
}

func (t *txn) TripIDForOrder(ctx context.Context, orderID string) (string, bool, error) {
	var tripID string
	err := t.tx.QueryRow(ctx, `SELECT id FROM trips WHERE order_id = $1`, orderID).Scan(&tripID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, err
	}
	return tripID, true, nil
}

// MarkEventProcessed reports whether this is the first time eventID has
// been seen, atomically claiming it.
func (t *txn) MarkEventProcessed(ctx context.Context, eventID string) (bool, error) {
	tag, err := t.tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (t *txn) CreateTrip(ctx context.Context, tr domain.Trip, route domain.Route) (bool, error) {
	var pickupLat, pickupLng, dropoffLat, dropoffLng *float64 // NULL when the event carries no route
	if route.Pickup != nil {
		pickupLat, pickupLng = &route.Pickup.Lat, &route.Pickup.Lng
	}
	if route.Dropoff != nil {
		dropoffLat, dropoffLng = &route.Dropoff.Lat, &route.Dropoff.Lng
	}
	tag, err := t.tx.Exec(ctx,
		`INSERT INTO trips (id, order_id, driver_id, rider_id, status, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (order_id) DO NOTHING`,
		tr.ID, tr.OrderID, tr.DriverID, tr.RiderID, tr.Status,
		pickupLat, pickupLng, dropoffLat, dropoffLng,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// The statement that moves a trip, by the status it moves to: each also stamps
// its own timestamp column.
var transitionSQL = map[domain.Status]string{
	domain.InProgress: `UPDATE trips SET status = $1, started_at = now() WHERE id = $2 AND status = ANY($3)`,
	domain.Completed:  `UPDATE trips SET status = $1, completed_at = now() WHERE id = $2 AND status = ANY($3)`,
	domain.Cancelled:  `UPDATE trips SET status = $1, cancelled_at = now(), cancelled_by = $4, cancel_reason = $5 WHERE id = $2 AND status = ANY($3)`,
}

func (t *txn) Transition(ctx context.Context, tripID string, a domain.Action, cancel domain.CancelInfo) (bool, error) {
	from := a.From()
	fromStrings := make([]string, len(from))
	for i, s := range from {
		fromStrings[i] = string(s)
	}
	args := []any{a.To(), tripID, fromStrings}
	if a.To() == domain.Cancelled {
		args = append(args, cancel.By, cancel.Reason)
	}
	tag, err := t.tx.Exec(ctx, transitionSQL[a.To()], args...)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// SaveRating records a rating on a completed, not-yet-rated trip and reports
// whether this call did it. The conditions live in the UPDATE, so two
// concurrent submissions can't both succeed.
func (t *txn) SaveRating(ctx context.Context, tripID string, r domain.Rating) (bool, error) {
	tag, err := t.tx.Exec(ctx, `
		UPDATE trips SET rating = $1, rating_comment = $2, tip_amount = $3, rated_at = now(),
		    receipt_closed_at = COALESCE(receipt_closed_at, now())
		WHERE id = $4 AND status = 'COMPLETED' AND rating IS NULL`,
		r.Stars, r.Comment, r.Tip, tripID,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (t *txn) EnqueueStatusChanged(ctx context.Context, tripID string, payload []byte) error {
	return t.enqueue(ctx, events.TopicTripStatusChanged, []byte(tripID), payload)
}

func (t *txn) EnqueueTripRated(ctx context.Context, driverID string, payload []byte) error {
	return t.enqueue(ctx, events.TopicTripRated, []byte(driverID), payload)
}

func (t *txn) enqueue(ctx context.Context, topic string, key, payload []byte) error {
	if err := outbox.Add(ctx, t.tx, OutboxSource, topic, key, payload); err != nil {
		return err
	}
	t.queued = true
	return nil
}
