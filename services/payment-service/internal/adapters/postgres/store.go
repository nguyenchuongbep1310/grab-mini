// Package postgres implements the app's store ports with SQL: orders and
// assignments, the payment lookup, the read side, and the unit of work that
// commits a settlement and its outbox row together.
package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/payment-service/internal/app"
	"grab-mini/payment-service/internal/domain"
)

// Store implements app.OrderStore, app.AssignmentStore, app.PaymentLookup and app.Reader.
type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

var (
	_ app.OrderStore      = (*Store)(nil)
	_ app.AssignmentStore = (*Store)(nil)
	_ app.PaymentLookup   = (*Store)(nil)
	_ app.Reader          = (*Store)(nil)
)

// ----------------------------------------------------------- order prices ----

func (s *Store) SaveOrder(ctx context.Context, orderID, riderID string, price *int64, discount int64) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO order_prices (order_id, rider_id, price, discount) VALUES ($1, $2, $3, $4) ON CONFLICT (order_id) DO NOTHING`,
		orderID, riderID, price, discount)
	return err
}

func (s *Store) FindOrder(ctx context.Context, orderID string) (domain.OrderInfo, bool, error) {
	var o domain.OrderInfo
	err := s.db.QueryRow(ctx, `SELECT rider_id, price, discount FROM order_prices WHERE order_id = $1`, orderID).Scan(&o.RiderID, &o.Price, &o.Discount)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.OrderInfo{}, false, nil
	}
	if err != nil {
		return domain.OrderInfo{}, false, err
	}
	return o, true, nil
}

// ------------------------------------------------------------ assignments ----

func (s *Store) SaveAssignment(ctx context.Context, orderID string, at time.Time) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO order_assignments (order_id, assigned_at) VALUES ($1, $2) ON CONFLICT (order_id) DO NOTHING`,
		orderID, at)
	return err
}

func (s *Store) AssignedAt(ctx context.Context, orderID string) (time.Time, bool, error) {
	var assignedAt time.Time
	err := s.db.QueryRow(ctx, `SELECT assigned_at FROM order_assignments WHERE order_id = $1`, orderID).Scan(&assignedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return assignedAt, true, nil
}

// --------------------------------------------------------------- payments ----

func (s *Store) Exists(ctx context.Context, tripID, kind string) (bool, error) {
	var exists bool
	err := s.db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM payments WHERE trip_id = $1 AND kind = $2)`, tripID, kind).Scan(&exists)
	return exists, err
}

// ------------------------------------------------------------- read side ----

func (s *Store) RiderPayments(ctx context.Context, tripID, riderID string) ([]app.RiderPayment, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, kind, status, amount, currency, created_at FROM payments
		WHERE trip_id = $1 AND rider_id = $2 ORDER BY created_at, kind`, tripID, riderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []app.RiderPayment{}
	for rows.Next() {
		var p app.RiderPayment
		if err := rows.Scan(&p.ID, &p.Kind, &p.Status, &p.Amount, &p.Currency, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) DriverPayouts(ctx context.Context, tripID, driverID string) ([]app.DriverPayout, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, kind, status, driver_amount, amount, platform_amount, currency, created_at FROM payments
		WHERE trip_id = $1 AND driver_id = $2 AND status = 'CAPTURED' ORDER BY created_at, kind`, tripID, driverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []app.DriverPayout{}
	for rows.Next() {
		var p app.DriverPayout
		if err := rows.Scan(&p.ID, &p.Kind, &p.Status, &p.Amount, &p.Gross, &p.Commission, &p.Currency, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) EarningsByKind(ctx context.Context, driverID string, from, to *time.Time) ([]app.KindTotal, error) {
	rows, err := s.db.Query(ctx, `
		SELECT kind, COALESCE(SUM(driver_amount), 0)::bigint, COALESCE(SUM(platform_amount), 0)::bigint, COUNT(*)
		FROM payments
		WHERE driver_id = $1 AND status = 'CAPTURED'
		  AND ($2::timestamptz IS NULL OR created_at >= $2) AND ($3::timestamptz IS NULL OR created_at < $3)
		GROUP BY kind`, driverID, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []app.KindTotal
	for rows.Next() {
		var k app.KindTotal
		if err := rows.Scan(&k.Kind, &k.Driver, &k.Platform, &k.Count); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (s *Store) PayableBalance(ctx context.Context, driverID string) (int64, error) {
	var balance int64
	err := s.db.QueryRow(ctx, `SELECT COALESCE(SUM(amount), 0)::bigint FROM ledger_entries WHERE account_id = $1`,
		domain.DriverPayableAccount(driverID).ID).Scan(&balance)
	return balance, err
}
