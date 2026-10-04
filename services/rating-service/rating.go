package main

import (
	"context"
	"embed"
	"fmt"
	"log"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/events"
	"grab-mini/pkg/migrate"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const serviceName = "rating-service"

// openPool connects without touching the schema.
func openPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}

// connectDB connects and brings the schema up to date, unless
// RUN_MIGRATIONS=false (then a deploy step runs `rating-service migrate`).
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

// runMigrate is the `rating-service migrate` command: apply pending migrations and exit.
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

// invalidEvent is a trip.rated that can never be counted (a star count
// outside 1 to 5). It's permanent, so it goes straight to the dead-letter
// topic instead of being retried, and never skews an average.
type invalidEvent struct{ reason string }

func (e *invalidEvent) Error() string   { return "invalid trip.rated: " + e.reason }
func (e *invalidEvent) Permanent() bool { return true }

func validate(evt events.TripRated) error {
	if evt.Stars < 1 || evt.Stars > 5 {
		return &invalidEvent{fmt.Sprintf("stars=%d, want 1 to 5", evt.Stars)}
	}
	if evt.DriverID == "" {
		return &invalidEvent{"no driverId"}
	}
	return nil
}

// record adds a rating to its driver's totals, at most once per event: the
// dedupe insert and the update share a transaction, so a crash between them
// can't count a rating without remembering it, or the reverse.
func record(ctx context.Context, db *pgxpool.Pool, evt events.TripRated) (counted bool, err error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `INSERT INTO rating_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, evt.EventID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO driver_ratings (driver_id, rating_count, rating_sum) VALUES ($1, 1, $2)
		ON CONFLICT (driver_id) DO UPDATE SET
			rating_count = driver_ratings.rating_count + 1,
			rating_sum = driver_ratings.rating_sum + EXCLUDED.rating_sum`,
		evt.DriverID, evt.Stars,
	)
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

type driverRating struct {
	DriverID string  `json:"driverId"`
	Average  float64 `json:"average"`
	Count    int     `json:"count"`
}

// average is the mean rounded to two decimals, or 0 when there are no ratings.
func average(sum, count int) float64 {
	if count == 0 {
		return 0
	}
	return math.Round(float64(sum)/float64(count)*100) / 100
}

func getRating(ctx context.Context, db *pgxpool.Pool, driverID string) (driverRating, error) {
	var sum, count int
	err := db.QueryRow(ctx, `SELECT rating_sum, rating_count FROM driver_ratings WHERE driver_id = $1`, driverID).Scan(&sum, &count)
	if err != nil && err != pgx.ErrNoRows {
		return driverRating{}, err
	}
	return driverRating{DriverID: driverID, Average: average(sum, count), Count: count}, nil
}
