package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/order-service/internal/app"
	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
)

// UnitOfWork runs use cases in one database transaction.
type UnitOfWork struct {
	db     *pgxpool.Pool
	source string // names this service in the shared outbox table
}

// NewUnitOfWork returns a UnitOfWork over db whose outbox rows carry source.
func NewUnitOfWork(db *pgxpool.Pool, source string) *UnitOfWork {
	return &UnitOfWork{db: db, source: source}
}

// Do commits when fn returns nil and rolls back otherwise. It returns fn's
// error as it is, so callers can use errors.Is and errors.As on it.
func (u *UnitOfWork) Do(ctx context.Context, fn func(app.Tx) error) error {
	pt, err := u.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer pt.Rollback(ctx)
	if err := fn(&tx{tx: pt, source: u.source}); err != nil {
		return err
	}
	return pt.Commit(ctx)
}

type tx struct {
	tx     pgx.Tx
	source string
}

// queryer is what lookupKey needs, satisfied by a pool and by a transaction.
type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// LockKey serializes requests that share a rider and key until the
// transaction ends, so a duplicate waits for the first to commit and then
// finds its row.
func (t *tx) LockKey(ctx context.Context, riderID, key string) error {
	_, err := t.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, fmt.Sprintf("%d:%s:%s", len(riderID), riderID, key))
	return err
}

func (t *tx) Lookup(ctx context.Context, riderID, key string) (*domain.KeyRecord, error) {
	return lookupKey(ctx, t.tx, riderID, key)
}

// lookupKey returns the live record for (riderID, key), or nil. A record
// older than domain.IdempotencyTTL is removed and reported as absent.
func lookupKey(ctx context.Context, q queryer, riderID, key string) (*domain.KeyRecord, error) {
	var rec domain.KeyRecord
	var expired bool
	err := q.QueryRow(ctx, `
		SELECT order_id::text, fingerprint, status_code, response, created_at < now() - make_interval(secs => $3)
		FROM idempotency_keys WHERE rider_id = $1 AND key = $2`,
		riderID, key, domain.IdempotencyTTL.Seconds(),
	).Scan(&rec.OrderID, &rec.Fingerprint, &rec.Status, &rec.Body, &expired)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if expired {
		if _, err := q.Exec(ctx, `DELETE FROM idempotency_keys WHERE rider_id = $1 AND key = $2`, riderID, key); err != nil {
			return nil, err
		}
		return nil, nil
	}
	rec.Key = key
	return &rec, nil
}

// SaveKey maps the primary-key violation on idempotency_keys (another request
// with this rider and key committed first) to app.ErrDuplicateKey.
func (t *tx) SaveKey(ctx context.Context, riderID string, rec domain.KeyRecord) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO idempotency_keys (rider_id, key, order_id, fingerprint, status_code, response)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		riderID, rec.Key, rec.OrderID, rec.Fingerprint, rec.Status, rec.Body)
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg.Code == "23505" && pg.ConstraintName == "idempotency_keys_pkey" {
		return fmt.Errorf("%w: %v", app.ErrDuplicateKey, err)
	}
	return err
}

func (t *tx) InsertOrder(ctx context.Context, o app.NewOrder) error {
	multiplier := o.Multiplier
	if multiplier == 0 {
		multiplier = 1.0 // an order built without pricing
	}
	_, err := t.tx.Exec(ctx, `
		INSERT INTO orders (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type, status, surge_multiplier, price, promo_code, discount)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', $8, $9, $10, $11)`,
		o.ID, o.RiderID, o.Pickup.Lat, o.Pickup.Lng, o.Dropoff.Lat, o.Dropoff.Lng, string(o.VehicleType),
		multiplier, o.Price, o.PromoCode, o.Discount,
	)
	return err
}

// CancelPending moves a PENDING order to CANCELLED. The status check lives in
// the UPDATE, so against a concurrent order.matched exactly one of the two wins.
func (t *tx) CancelPending(ctx context.Context, orderID, reason string) (bool, error) {
	tag, err := t.tx.Exec(ctx, `
		UPDATE orders SET status = 'CANCELLED', cancelled_at = now(), cancelled_by = $2, cancel_reason = $3
		WHERE id = $1 AND status = 'PENDING'`,
		orderID, domain.CancelledByRider, reason)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (t *tx) QueueOrderCreated(ctx context.Context, orderID string, payload []byte) error {
	return outbox.Add(ctx, t.tx, t.source, events.TopicOrderCreated, []byte(orderID), payload)
}

func (t *tx) QueueOrderCancelled(ctx context.Context, orderID string, payload []byte) error {
	return outbox.Add(ctx, t.tx, t.source, events.TopicOrderCancelled, []byte(orderID), payload)
}
