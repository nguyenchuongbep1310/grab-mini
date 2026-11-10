package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
	"grab-mini/profile-service/internal/app"
	"grab-mini/profile-service/internal/domain"
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

// Do commits when fn returns nil and rolls back otherwise. It returns fn's error as it is,
// so callers can use errors.Is and errors.As on it.
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

// LockOrCreateDriver creates the (empty) profile if there is none and locks it.
func (t *tx) LockOrCreateDriver(ctx context.Context, id string, now time.Time) (domain.Driver, error) {
	if _, err := t.tx.Exec(ctx, `
		INSERT INTO driver_profiles (driver_id, status_at, updated_at) VALUES ($1, $2, $2) ON CONFLICT DO NOTHING`, id, now); err != nil {
		return domain.Driver{}, err
	}
	return loadDriver(ctx, t.tx, id, true)
}

// LockDriver locks an existing profile; domain.ErrNoDriver when there is none.
func (t *tx) LockDriver(ctx context.Context, id string) (domain.Driver, error) {
	return loadDriver(ctx, t.tx, id, true)
}

// UpdateProfile replaces the editable fields and the status columns.
func (t *tx) UpdateProfile(ctx context.Context, d domain.Driver) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE driver_profiles
		SET name = $2, phone = $3, vehicle_type = $4, plate = $5, model = $6, color = $7,
		    status = $8, reason = $9, reviewed_by = $10, reviewed_at = $11, status_at = $12, updated_at = $13
		WHERE driver_id = $1`,
		d.ID, d.Name, d.Phone, string(d.Vehicle), d.Plate, d.Model, d.Color,
		d.Status, d.Reason, d.ReviewedBy, d.ReviewedAt, d.StatusAt, d.UpdatedAt)
	return err
}

// MarkPending writes an application: PENDING, no reason, status and update time now.
func (t *tx) MarkPending(ctx context.Context, id string, now time.Time) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE driver_profiles SET status = $2, reason = '', status_at = $3, updated_at = $3 WHERE driver_id = $1`,
		id, domain.StatusPending, now)
	return err
}

// RecordReview writes a decision: reviewed_at = at, status_at = updated_at = d.StatusAt.
func (t *tx) RecordReview(ctx context.Context, d domain.Driver, at time.Time) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE driver_profiles
		SET status = $2, reason = $3, reviewed_by = $4, reviewed_at = $5, status_at = $6, updated_at = $6
		WHERE driver_id = $1`, d.ID, d.Status, d.Reason, d.ReviewedBy, at, d.StatusAt)
	return err
}

// QueueReview adds driver.application.reviewed to the outbox in the transaction.
func (t *tx) QueueReview(ctx context.Context, driverID string, payload []byte) error {
	return outbox.Add(ctx, t.tx, t.source, events.TopicDriverApplicationReviewed, []byte(driverID), payload)
}
