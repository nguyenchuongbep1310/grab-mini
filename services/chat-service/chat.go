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
	serviceName  = "chat-service"
	outboxSource = "chat-service"
)

var (
	messagesSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "chat_messages_total",
		Help: "POST /trips/{id}/messages that were accepted, by sender role and result: sent, or replayed (an Idempotency-Key seen before: nothing new was saved).",
	}, []string{"role", "result"})

	messagesRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "chat_messages_rejected_total",
		Help: "POST /trips/{id}/messages that were refused, by reason: unknown_trip, not_participant, invalid_text, chat_closed, rate_limited, key_mismatch, no_recipient, invalid_request.",
	}, []string{"reason"})

	tripEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "chat_trip_events_total",
		Help: "trip.status.changed events handled, by result: applied (the trip row changed), stale (an older status than the one held), duplicate (eventId seen before), ignored (a status this service does not know).",
	}, []string{"result"})

	pruned = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "chat_pruned_total",
		Help: "Rows deleted by the retention job, by table (chat_messages, chat_trips).",
	}, []string{"table"})
)

// openPool connects without touching the schema.
func openPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}

// connectDB connects and brings the schema up to date, unless
// RUN_MIGRATIONS=false (then a deploy step runs `chat-service migrate`).
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

// runMigrate is the `chat-service migrate` command: apply pending migrations and exit.
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

	// limiter is per sender and in memory: with several instances a sender's real
	// allowance is the limit times the instances (see auth.Limiter). nil lets everything through.
	limiter *auth.Limiter
	// grace is how long a finished trip's chat stays writable.
	grace time.Duration
}

func newService(db *pgxpool.Pool, enc encoder, relay *outbox.Relay, env Env) *service {
	return &service{
		db: db, enc: enc, relay: relay, now: time.Now,
		limiter: newLimiter(env.RatePerMinute),
		grace:   env.Grace,
	}
}

// newLimiter allows a burst of a quarter of a minute's allowance (so a few quick
// lines in a row are fine) and then perMinute messages a minute. RATE_LIMIT_SCALE
// scales it, 0 turns it off (load tests), as for every other service.
func newLimiter(perMinute int) *auth.Limiter {
	return auth.LimiterFromEnv(float64(perMinute)/60, max(1, perMinute/4))
}
