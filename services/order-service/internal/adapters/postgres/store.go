package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/order-service/internal/domain"
)

// Store reads orders, reads and purges idempotency keys, and applies the
// outcomes reported by Matching and Trip Service. It implements app.OrderReader,
// app.KeyStore and app.Outcomes.
type Store struct{ db *pgxpool.Pool }

// NewStore returns a Store over db.
func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

const orderColumns = `id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type, status, surge_multiplier, price, promo_code, discount, cancelled_at, cancelled_by, cancel_reason, driver_id`

func (s *Store) queryOrder(ctx context.Context, where string, arg string) (domain.Order, error) {
	var o domain.Order
	err := s.db.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE `+where+` ORDER BY created_at DESC LIMIT 1`, arg).
		Scan(&o.ID, &o.RiderID, &o.Pickup.Lat, &o.Pickup.Lng, &o.Dropoff.Lat, &o.Dropoff.Lng, &o.VehicleType, &o.Status, &o.SurgeMultiplier, &o.Price, &o.PromoCode, &o.Discount, &o.CancelledAt, &o.CancelledBy, &o.CancelReason, &o.DriverID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Order{}, domain.ErrOrderNotFound
	}
	return o, err
}

func (s *Store) ByID(ctx context.Context, id string) (domain.Order, error) {
	return s.queryOrder(ctx, "id = $1", id)
}

func (s *Store) LatestByRider(ctx context.Context, riderID string) (domain.Order, error) {
	return s.queryOrder(ctx, "rider_id = $1", riderID)
}

func (s *Store) Lookup(ctx context.Context, riderID, key string) (*domain.KeyRecord, error) {
	return lookupKey(ctx, s.db, riderID, key)
}

// Purge deletes one batch (5000) of expired keys. Expired keys are ignored by
// Lookup either way; this only keeps the table small.
func (s *Store) Purge(ctx context.Context) (int64, error) {
	tag, err := s.db.Exec(ctx, `
		DELETE FROM idempotency_keys WHERE ctid IN (
			SELECT ctid FROM idempotency_keys WHERE created_at < now() - make_interval(secs => $1) LIMIT 5000)`,
		domain.IdempotencyTTL.Seconds())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Set moves a PENDING order to status; only PENDING orders move, so a
// redelivered or out-of-order event is a harmless no-op.
func (s *Store) Set(ctx context.Context, orderID string, status domain.Status, driverID string) (bool, error) {
	tag, err := s.db.Exec(ctx, `UPDATE orders SET status = $2, driver_id = $3 WHERE id = $1 AND status = 'PENDING'`, orderID, string(status), driverID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// CancelMatched moves a MATCHED order to CANCELLED.
func (s *Store) CancelMatched(ctx context.Context, orderID, by, reason string) (bool, error) {
	tag, err := s.db.Exec(ctx, `
		UPDATE orders SET status = 'CANCELLED', cancelled_at = now(), cancelled_by = $2, cancel_reason = $3
		WHERE id = $1 AND status = 'MATCHED'`, orderID, by, reason)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
