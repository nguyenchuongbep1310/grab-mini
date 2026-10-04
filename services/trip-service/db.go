package main

import (
	"context"
	"embed"
	"errors"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/events"
	"grab-mini/pkg/migrate"
)

var errTripNotFound = errors.New("trip not found")

//go:embed migrations/*.sql
var migrationFiles embed.FS

const serviceName = "trip-service"

// openPool connects without touching the schema.
func openPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}

// connectDB connects and brings the schema up to date, unless
// RUN_MIGRATIONS=false (then a deploy step runs `trip-service migrate`).
func connectDB(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := openPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := migrate.AtStartup(ctx, pool, serviceName, migrationFiles); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// runMigrate is the `trip-service migrate` command: apply pending migrations and exit.
func runMigrate(env Env) {
	ctx := context.Background()
	pool, err := openPool(ctx, env.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()
	n, err := migrate.Up(ctx, pool, serviceName, migrationFiles)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s: %d migration(s) applied", serviceName, n)
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
