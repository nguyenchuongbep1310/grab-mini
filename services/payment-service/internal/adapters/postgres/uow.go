package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/payment-service/internal/app"
	"grab-mini/payment-service/internal/domain"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
)

// OutboxSource names this service's rows in the shared outbox table.
const OutboxSource = "payment-service"

// Waker is woken after a commit that queued an outbox row (*outbox.Relay).
type Waker interface{ Wake() }

// UnitOfWork implements app.UnitOfWork: one pgx transaction, with the outbox
// row written in it (ADR 0004).
type UnitOfWork struct {
	db    *pgxpool.Pool
	waker Waker // may be nil in tests that don't publish
}

func NewUnitOfWork(db *pgxpool.Pool, waker Waker) *UnitOfWork {
	return &UnitOfWork{db: db, waker: waker}
}

var _ app.UnitOfWork = (*UnitOfWork)(nil)

func (u *UnitOfWork) Do(ctx context.Context, fn func(app.Tx) error) error {
	tx, err := u.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	t := &txn{tx: tx}
	if err := fn(t); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if t.queued && u.waker != nil {
		u.waker.Wake()
	}
	return nil
}

type txn struct {
	tx     pgx.Tx
	queued bool
}

func (t *txn) MarkEventProcessed(ctx context.Context, eventID string) (bool, error) {
	tag, err := t.tx.Exec(ctx, `INSERT INTO payment_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (t *txn) InsertPayment(ctx context.Context, p domain.Payment) (bool, error) {
	tag, err := t.tx.Exec(ctx, `
		INSERT INTO payments (id, trip_id, order_id, rider_id, driver_id, kind, status, amount, driver_amount, platform_amount,
		                      currency, provider, provider_ref, decline_reason, ledger_txn_id, source_event_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (trip_id, kind) DO NOTHING`,
		p.ID, p.TripID, p.OrderID, p.RiderID, p.DriverID, p.Kind, p.Status, p.Amount, p.DriverAmount, p.PlatformAmount,
		p.Currency, p.Provider, p.ProviderRef, p.DeclineReason, p.LedgerTxnID, p.SourceEventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (t *txn) PostLedger(ctx context.Context, x domain.Txn) error { return post(ctx, t.tx, x) }

func (t *txn) EnqueuePaymentRecorded(ctx context.Context, tripID string, payload []byte) error {
	if err := outbox.Add(ctx, t.tx, OutboxSource, events.TopicPaymentRecorded, []byte(tripID), payload); err != nil {
		return err
	}
	t.queued = true
	return nil
}
