package main

import (
	"context"
	"embed"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/pkg/migrate"
	"grab-mini/pkg/outbox"
	"grab-mini/pkg/promo"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const (
	serviceName  = "promotion-service"
	outboxSource = "promotion-service"
)

var (
	validations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "promotion_validations_total",
		Help: "GET /promotions/validate answers, by result: ok or the rejection code (not_found, expired, not_started, min_fare, wrong_vehicle, limit_reached, already_used).",
	}, []string{"result"})

	redemptions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "promotion_redemptions_total",
		Help: "Redemptions recorded from order.created, by status (REDEEMED, OVER_LIMIT) and, for OVER_LIMIT, the limit reached (usage_limit, per_rider_limit).",
	}, []string{"status", "reason"})

	redemptionsReleased = promauto.NewCounter(prometheus.CounterOpts{
		Name: "promotion_redemptions_released_total",
		Help: "Redemptions given back because the order ended without a ride (order.cancelled, a cancelled trip, order.no_driver_found).",
	})

	redemptionsDuplicate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "promotion_redemptions_duplicate_total",
		Help: "Events for a redemption or release that was already applied (redelivery or replay): nothing changed.",
	})
)

// openPool connects without touching the schema.
func openPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}

// connectDB connects and brings the schema up to date, unless
// RUN_MIGRATIONS=false (then a deploy step runs `promotion-service migrate`).
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

// runMigrate is the `promotion-service migrate` command: apply pending migrations and exit.
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
	db     *pgxpool.Pool
	signer *promo.Signer
	enc    encoder
	relay  *outbox.Relay // may be nil in tests that don't publish
	now    func() time.Time
}

func newService(db *pgxpool.Pool, signer *promo.Signer, enc encoder, relay *outbox.Relay) *service {
	return &service{db: db, signer: signer, enc: enc, relay: relay, now: time.Now}
}

// ------------------------------------------------------------------ store ----

const promotionColumns = `code, kind, value, max_discount, min_fare, vehicle_type, valid_from, valid_to, usage_limit, per_rider_limit, disabled_at`

type scanner interface{ Scan(dest ...any) error }

func scanPromotion(row scanner, p *Promotion) error {
	return row.Scan(&p.Code, &p.Kind, &p.Value, &p.MaxDiscount, &p.MinFare, &p.VehicleType, &p.ValidFrom, &p.ValidTo, &p.UsageLimit, &p.PerRider, &p.DisabledAt)
}

// queryer is a pool or a transaction.
type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var errNoPromotion = errors.New("no such promotion")

// findPromotion reads a promotion; lock takes its row lock (FOR UPDATE), which
// is how redemptions of one code are serialised so the limits are counted exactly.
func findPromotion(ctx context.Context, q queryer, code string, lock bool) (Promotion, error) {
	sql := `SELECT ` + promotionColumns + ` FROM promotions WHERE code = $1`
	if lock {
		sql += ` FOR UPDATE`
	}
	var p Promotion
	err := scanPromotion(q.QueryRow(ctx, sql, code), &p)
	if errors.Is(err, pgx.ErrNoRows) {
		return Promotion{}, errNoPromotion
	}
	return p, err
}

// usage counts the redemptions that count against a promotion's limits.
func usage(ctx context.Context, q queryer, code, riderID string) (Usage, error) {
	var u Usage
	err := q.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE rider_id = $2)
		FROM redemptions WHERE code = $1 AND status = 'REDEEMED'`, code, riderID).Scan(&u.Total, &u.ByRider)
	return u, err
}
