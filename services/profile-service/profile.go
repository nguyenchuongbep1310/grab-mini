package main

import (
	"context"
	"embed"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/migrate"
	"grab-mini/pkg/outbox"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const (
	serviceName  = "profile-service"
	outboxSource = "profile-service"
)

var (
	profileUpdates = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_updates_total",
		Help: "PUT /riders/{id} and PUT /drivers/{id} that were accepted, by kind (rider, driver).",
	}, []string{"kind"})

	applications = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_applications_total",
		Help: "POST /drivers/{id}/application, by result: submitted (now PENDING), replayed (already PENDING or APPROVED: nothing changed).",
	}, []string{"result"})

	reviews = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_reviews_total",
		Help: "driver.application.reviewed events queued, by decision: approved, rejected, suspended, vehicle_changed (an approved driver changed plate or vehicle type and is out of service until approved again).",
	}, []string{"decision"})

	requestsRefused = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_requests_rejected_total",
		Help: "Requests refused, by reason: invalid_request, invalid_profile, forbidden, not_found, incomplete_profile, suspended, invalid_transition, rate_limited.",
	}, []string{"reason"})

	publicLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_public_lookups_total",
		Help: "GET /drivers/{id}/public, by result: found, not_found (no such driver, or not approved: the two look the same).",
	}, []string{"result"})
)

// openPool connects without touching the schema.
func openPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}

// connectDB connects and brings the schema up to date, unless
// RUN_MIGRATIONS=false (then a deploy step runs `profile-service migrate`).
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

// runMigrate is the `profile-service migrate` command: apply pending migrations and exit.
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

// encoder turns an event into its Avro wire bytes (*eventcodec.Codec).
type encoder interface {
	Encode(ctx context.Context, topic string, v any) ([]byte, error)
}

// service holds what the handlers share.
type service struct {
	db    *pgxpool.Pool
	enc   encoder
	relay *outbox.Relay // may be nil in tests that don't publish
	now   func() time.Time

	// publicLimiter limits GET /drivers/{id}/public per caller, in memory: with
	// several instances a caller's real allowance is the limit times the instances.
	// nil lets everything through.
	publicLimiter *auth.Limiter
}

func newService(db *pgxpool.Pool, enc encoder, relay *outbox.Relay, env Env) *service {
	return &service{
		db: db, enc: enc, relay: relay, now: time.Now,
		publicLimiter: newLimiter(env.PublicRatePerMinute),
	}
}

// newLimiter allows a burst of a quarter of a minute's allowance and then perMinute
// lookups a minute. RATE_LIMIT_SCALE scales it, 0 turns it off (load tests), as for
// every other service.
func newLimiter(perMinute int) *auth.Limiter {
	return auth.LimiterFromEnv(float64(perMinute)/60, max(1, perMinute/4))
}
