package main

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
)

var errTripNotFound = errors.New("trip not found")

func connectDB(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS trips (
			id UUID PRIMARY KEY,
			order_id TEXT NOT NULL UNIQUE,
			driver_id TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			started_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			cancelled_at TIMESTAMPTZ
		);

		-- US-6.1: dedupe table so a redelivered Kafka event never creates a
		-- second trip for the same order.matched event.
		-- Ratings arrived after the first release, hence ALTER rather than CREATE.
		ALTER TABLE trips ADD COLUMN IF NOT EXISTS rating INT;
		ALTER TABLE trips ADD COLUMN IF NOT EXISTS rating_comment TEXT;
		ALTER TABLE trips ADD COLUMN IF NOT EXISTS rated_at TIMESTAMPTZ;
		-- Who to tell about the trip; copied from order.matched (empty for older trips).
		ALTER TABLE trips ADD COLUMN IF NOT EXISTS rider_id TEXT NOT NULL DEFAULT '';

		CREATE TABLE IF NOT EXISTS processed_events (
			event_id UUID PRIMARY KEY,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	if err != nil {
		return nil, err
	}

	if err := outbox.Migrate(ctx, pool); err != nil {
		return nil, err
	}

	return pool, nil
}

// execer is what the write helpers need, so they run on the pool or inside a
// transaction (see transitionWithEvent).
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// markEventProcessed reports whether this is the first time eventID has
// been seen, atomically claiming it.
func markEventProcessed(ctx context.Context, db execer, eventID string) (firstTime bool, err error) {
	tag, err := db.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

type trip struct {
	ID       string            `json:"id"`
	OrderID  string            `json:"orderId"`
	DriverID string            `json:"driverId"`
	RiderID  string            `json:"riderId"`
	Status   events.TripStatus `json:"status"`
	// Set once the rider has rated the trip.
	Rating *int `json:"rating,omitempty"`
}

func getTripByID(ctx context.Context, db *pgxpool.Pool, id string) (*trip, error) {
	return queryTrip(ctx, db, `SELECT id, order_id, driver_id, rider_id, status, rating FROM trips WHERE id = $1`, id)
}

// getTripByOrderID lets a client that only knows the orderId (e.g. from an
// order.matched push) find the trip Trip Service created for it.
func getTripByOrderID(ctx context.Context, db *pgxpool.Pool, orderID string) (*trip, error) {
	return queryTrip(ctx, db, `SELECT id, order_id, driver_id, rider_id, status, rating FROM trips WHERE order_id = $1`, orderID)
}

// getActiveTripByDriver is the driver's trip still in flight (ASSIGNED or
// IN_PROGRESS), so a driver app that was closed or reloaded can pick up where
// it left off. A driver only holds one at a time; if data ever says otherwise,
// the newest wins.
func getActiveTripByDriver(ctx context.Context, db *pgxpool.Pool, driverID string) (*trip, error) {
	return queryTrip(ctx, db, `
		SELECT id, order_id, driver_id, rider_id, status, rating FROM trips
		WHERE driver_id = $1 AND status IN ('ASSIGNED', 'IN_PROGRESS')
		ORDER BY created_at DESC LIMIT 1`, driverID)
}

func queryTrip(ctx context.Context, db *pgxpool.Pool, query string, arg string) (*trip, error) {
	var t trip
	err := db.QueryRow(ctx, query, arg).Scan(&t.ID, &t.OrderID, &t.DriverID, &t.RiderID, &t.Status, &t.Rating)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errTripNotFound
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

type timestampField int

const (
	startedAt timestampField = iota
	completedAt
	cancelledAt
)

var transitionSQL = map[timestampField]string{
	startedAt:   `UPDATE trips SET status = $1, started_at = now() WHERE id = $2 AND status = ANY($3)`,
	completedAt: `UPDATE trips SET status = $1, completed_at = now() WHERE id = $2 AND status = ANY($3)`,
	cancelledAt: `UPDATE trips SET status = $1, cancelled_at = now() WHERE id = $2 AND status = ANY($3)`,
}

// transitionTrip moves a trip to newStatus only if it's currently in one
// of fromStatuses, and reports whether this call performed the move.
// This is what makes trip actions idempotent (US-6.1): calling pickup
// twice, for example, only actually transitions once.
func transitionTrip(ctx context.Context, db execer, id string, fromStatuses []events.TripStatus, newStatus events.TripStatus, field timestampField) (bool, error) {
	tag, err := db.Exec(ctx, transitionSQL[field], newStatus, id, statusesToStrings(fromStatuses))
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func statusesToStrings(statuses []events.TripStatus) []string {
	out := make([]string, len(statuses))
	for i, s := range statuses {
		out[i] = string(s)
	}
	return out
}

// saveRating records a rating on a completed, not-yet-rated trip and reports
// whether this call did it. The conditions live in the UPDATE, so two
// concurrent submissions can't both succeed.
func saveRating(ctx context.Context, db execer, tripID string, stars int, comment string) (bool, error) {
	tag, err := db.Exec(ctx, `
		UPDATE trips SET rating = $1, rating_comment = $2, rated_at = now()
		WHERE id = $3 AND status = 'COMPLETED' AND rating IS NULL`,
		stars, comment, tripID,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
