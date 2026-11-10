package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/promotion-service/internal/app"
	"grab-mini/promotion-service/internal/domain"
)

// Store reads and writes promotions: it implements app.PromotionReader,
// app.PromotionWriter and app.PromotionViews.
type Store struct{ db *pgxpool.Pool }

// NewStore returns a Store over db.
func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

const promotionColumns = `code, kind, value, max_discount, min_fare, vehicle_type, valid_from, valid_to, usage_limit, per_rider_limit, disabled_at`

type scanner interface{ Scan(dest ...any) error }

func scanPromotion(row scanner, p *domain.Promotion) error {
	return row.Scan(&p.Code, &p.Kind, &p.Value, &p.MaxDiscount, &p.MinFare, &p.VehicleType, &p.ValidFrom, &p.ValidTo, &p.UsageLimit, &p.PerRider, &p.DisabledAt)
}

// queryer is a pool or a transaction.
type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// findPromotion reads a promotion; lock takes its row lock (FOR UPDATE), which
// is how redemptions of one code are serialised so the limits are counted exactly.
func findPromotion(ctx context.Context, q queryer, code string, lock bool) (domain.Promotion, error) {
	sql := `SELECT ` + promotionColumns + ` FROM promotions WHERE code = $1`
	if lock {
		sql += ` FOR UPDATE`
	}
	var p domain.Promotion
	err := scanPromotion(q.QueryRow(ctx, sql, code), &p)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Promotion{}, domain.ErrNotFound
	}
	return p, err
}

// usage counts the redemptions that count against a promotion's limits.
func usage(ctx context.Context, q queryer, code, riderID string) (domain.Usage, error) {
	var u domain.Usage
	err := q.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE rider_id = $2)
		FROM redemptions WHERE code = $1 AND status = 'REDEEMED'`, code, riderID).Scan(&u.Total, &u.ByRider)
	return u, err
}

// Find returns the promotion with the code, or domain.ErrNotFound.
func (s *Store) Find(ctx context.Context, code string) (domain.Promotion, error) {
	return findPromotion(ctx, s.db, code, false)
}

// Usage counts the redemptions against the code: by anyone, and by the rider.
func (s *Store) Usage(ctx context.Context, code, riderID string) (domain.Usage, error) {
	return usage(ctx, s.db, code, riderID)
}

// Create stores a promotion; domain.ErrCodeExists on a unique violation.
func (s *Store) Create(ctx context.Context, p domain.Promotion, createdBy string) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO promotions (code, kind, value, max_discount, min_fare, vehicle_type, valid_from, valid_to, usage_limit, per_rider_limit, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		p.Code, p.Kind, p.Value, p.MaxDiscount, p.MinFare, p.VehicleType, p.ValidFrom, p.ValidTo, p.UsageLimit, p.PerRider, createdBy)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return domain.ErrCodeExists
	}
	return err
}

// Disable sets disabled_at to at unless it is already set; found is false
// when no promotion has the code.
func (s *Store) Disable(ctx context.Context, code string, at time.Time) (bool, error) {
	tag, err := s.db.Exec(ctx, `UPDATE promotions SET disabled_at = COALESCE(disabled_at, $2) WHERE code = $1`, code, at)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

const viewColumns = `p.code, p.kind, p.value, p.max_discount, p.min_fare, p.vehicle_type, p.valid_from, p.valid_to,
	p.usage_limit, p.per_rider_limit, p.disabled_at, p.created_by, p.created_at,
	count(r.*) FILTER (WHERE r.status = 'REDEEMED'), count(r.*) FILTER (WHERE r.status = 'OVER_LIMIT')`

func scanView(row scanner) (domain.Summary, error) {
	var v domain.Summary
	err := row.Scan(&v.Code, &v.Kind, &v.Value, &v.MaxDiscount, &v.MinFare, &v.VehicleType, &v.ValidFrom, &v.ValidTo,
		&v.UsageLimit, &v.PerRider, &v.DisabledAt, &v.CreatedBy, &v.CreatedAt, &v.Redeemed, &v.OverLimit)
	return v, err
}

// View returns one promotion with its usage, or domain.ErrNotFound. A scan
// failure is returned as it is: the use case labels it "load promotion", as
// the flat handlers did.
func (s *Store) View(ctx context.Context, code string) (domain.Summary, error) {
	v, err := scanView(s.db.QueryRow(ctx, `
		SELECT `+viewColumns+` FROM promotions p LEFT JOIN redemptions r ON r.code = p.code
		WHERE p.code = $1 GROUP BY p.code`, code))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Summary{}, domain.ErrNotFound
	}
	return v, err
}

// List returns every promotion, newest first (at most 500). The flat handler
// logged a row that failed to scan as "read promotion", so that is its step
// here; the query and rows.Err failures are plain, for the use case's
// "list promotions".
func (s *Store) List(ctx context.Context) ([]domain.Summary, error) {
	rows, err := s.db.Query(ctx, `
		SELECT `+viewColumns+` FROM promotions p LEFT JOIN redemptions r ON r.code = p.code
		GROUP BY p.code ORDER BY p.created_at DESC, p.code LIMIT 500`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	views := []domain.Summary{}
	for rows.Next() {
		v, err := scanView(rows)
		if err != nil {
			return nil, &app.StepError{Step: "read promotion", Err: err}
		}
		views = append(views, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return views, nil
}
