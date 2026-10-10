package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/auth"
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
	if evt.TipAmount < 0 {
		return &invalidEvent{fmt.Sprintf("tipAmount=%d, want 0 or more", evt.TipAmount)}
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

	// The review row shares the transaction (and so the dedupe) with the
	// totals. A second event for an already-reviewed trip is not counted.
	tag, err = tx.Exec(ctx, `
		INSERT INTO driver_reviews (event_id, trip_id, driver_id, rider_id, stars, comment, tip_amount)
		VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (trip_id) DO NOTHING`,
		evt.EventID, evt.TripID, evt.DriverID, evt.RiderID, evt.Stars, evt.Comment, evt.TipAmount,
	)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, tx.Commit(ctx)
	}

	tipCount := 0
	if evt.TipAmount > 0 {
		tipCount = 1
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO driver_ratings (driver_id, rating_count, rating_sum, tip_sum, tip_count) VALUES ($1, 1, $2, $3, $4)
		ON CONFLICT (driver_id) DO UPDATE SET
			rating_count = driver_ratings.rating_count + 1,
			rating_sum = driver_ratings.rating_sum + EXCLUDED.rating_sum,
			tip_sum = driver_ratings.tip_sum + EXCLUDED.tip_sum,
			tip_count = driver_ratings.tip_count + EXCLUDED.tip_count`,
		evt.DriverID, evt.Stars, evt.TipAmount, tipCount,
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
	// TotalTips is the sum of recorded tips in VND; TipCount how many ratings carried one.
	TotalTips int64 `json:"totalTips"`
	TipCount  int   `json:"tipCount"`
}

// average is the mean rounded to two decimals, or 0 when there are no ratings.
func average(sum, count int) float64 {
	if count == 0 {
		return 0
	}
	return math.Round(float64(sum)/float64(count)*100) / 100
}

func getRating(ctx context.Context, db *pgxpool.Pool, driverID string) (driverRating, error) {
	var sum, count, tipCount int
	var tipSum int64
	err := db.QueryRow(ctx, `SELECT rating_sum, rating_count, tip_sum, tip_count FROM driver_ratings WHERE driver_id = $1`, driverID).Scan(&sum, &count, &tipSum, &tipCount)
	if err != nil && err != pgx.ErrNoRows {
		return driverRating{}, err
	}
	return driverRating{DriverID: driverID, Average: average(sum, count), Count: count, TotalTips: tipSum, TipCount: tipCount}, nil
}

type driverReview struct {
	TripID    string    `json:"tripId"`
	Stars     int       `json:"stars"`
	Comment   string    `json:"comment"`
	TipAmount int64     `json:"tipAmount"`
	CreatedAt time.Time `json:"createdAt"`
}

const (
	defaultReviewLimit = 20
	maxReviewLimit     = 50
)

// getReviews returns a driver's reviews newest first. before (exclusive) is
// the createdAt of the last review of the previous page, or zero for the first.
func getReviews(ctx context.Context, db *pgxpool.Pool, driverID string, limit int, before time.Time) ([]driverReview, error) {
	if before.IsZero() {
		before = time.Now().Add(24 * time.Hour)
	}
	rows, err := db.Query(ctx, `
		SELECT trip_id, stars, comment, tip_amount, created_at FROM driver_reviews
		WHERE driver_id = $1 AND created_at < $2
		ORDER BY created_at DESC LIMIT $3`, driverID, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []driverReview{}
	for rows.Next() {
		var r driverReview
		if err := rows.Scan(&r.TripID, &r.Stars, &r.Comment, &r.TipAmount, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// parseReviewQuery reads ?limit= (1 to 50, default 20) and ?before= (RFC 3339).
func parseReviewQuery(q url.Values) (limit int, before time.Time, err error) {
	limit = defaultReviewLimit
	if v := q.Get("limit"); v != "" {
		n, convErr := strconv.Atoi(v)
		if convErr != nil || n < 1 {
			return 0, time.Time{}, errors.New("limit must be a positive integer")
		}
		limit = min(n, maxReviewLimit)
	}
	if v := q.Get("before"); v != "" {
		before, err = time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return 0, time.Time{}, errors.New("before must be an RFC 3339 time")
		}
	}
	return limit, before, nil
}

// reviewsHandler serves GET /drivers/{id}/reviews. Comments are private to the
// driver they are about, so only that driver may read them.
func reviewsHandler(db *pgxpool.Pool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := auth.From(r.Context())
		if !ok || id.Role != auth.Driver || id.Subject != r.PathValue("id") {
			auth.Deny(w, http.StatusForbidden, "drivers can read only their own reviews")
			return
		}
		limit, before, err := parseReviewQuery(r.URL.Query())
		if err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		reviews, err := getReviews(r.Context(), db, id.Subject, limit, before)
		if err != nil {
			log.Printf("failed to load reviews: %v", err)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"driverId": id.Subject, "reviews": reviews})
	})
}
