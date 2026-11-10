package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
	"grab-mini/promotion-service/internal/app"
	"grab-mini/promotion-service/internal/domain"
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

// MarkEventProcessed records that eventID was applied, and reports whether
// this is the first time. The use case checks the id's shape.
func (t *tx) MarkEventProcessed(ctx context.Context, eventID string) (bool, error) {
	tag, err := t.tx.Exec(ctx, `INSERT INTO promotion_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// LockOrder serialises a redemption and a release of one order. order.created
// and the events that release it are on different topics, so they can be
// handled at the same moment by different workers; whichever takes the lock
// second sees what the first did.
func (t *tx) LockOrder(ctx context.Context, orderID string) error {
	_, err := t.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "promotion-order:"+orderID)
	return err
}

func (t *tx) FindPromotionForUpdate(ctx context.Context, code string) (domain.Promotion, error) {
	return findPromotion(ctx, t.tx, code, true)
}

func (t *tx) OrderWasCancelled(ctx context.Context, orderID string) (bool, error) {
	var cancelled bool
	err := t.tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM promotion_cancelled_orders WHERE order_id = $1)`, orderID).Scan(&cancelled)
	return cancelled, err
}

func (t *tx) Usage(ctx context.Context, code, riderID string) (domain.Usage, error) {
	return usage(ctx, t.tx, code, riderID)
}

func (t *tx) InsertRedemption(ctx context.Context, r app.NewRedemption) (bool, error) {
	tag, err := t.tx.Exec(ctx, `
		INSERT INTO redemptions (order_id, code, rider_id, discount, status, over_limit_reason, source_event_id, released_at)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8)
		ON CONFLICT (code, order_id) DO NOTHING`,
		r.OrderID, r.Code, r.RiderID, r.Discount, r.StoredStatus, r.OverLimitReason, r.SourceEventID, r.ReleasedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (t *tx) QueueRedeemed(ctx context.Context, orderID string, payload []byte) error {
	return outbox.Add(ctx, t.tx, t.source, events.TopicPromotionRedeemed, []byte(orderID), payload)
}

// ReleaseRedemptions: only REDEEMED and OVER_LIMIT rows move, so a RELEASED
// one keeps its original released_at.
func (t *tx) ReleaseRedemptions(ctx context.Context, orderID string) (int64, error) {
	tag, err := t.tx.Exec(ctx, `
		UPDATE redemptions SET status = 'RELEASED', released_at = now()
		WHERE order_id = $1 AND status IN ('REDEEMED', 'OVER_LIMIT')`, orderID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (t *tx) RememberCancelled(ctx context.Context, orderID string) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO promotion_cancelled_orders (order_id) VALUES ($1) ON CONFLICT DO NOTHING`, orderID)
	return err
}
