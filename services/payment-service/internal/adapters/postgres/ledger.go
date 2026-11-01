package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"grab-mini/payment-service/internal/domain"
)

// post writes the transaction and its entries inside tx, so a ledger write is
// always part of the caller's transaction. It validates first, so an
// unbalanced transaction never reaches the database (the use case has already
// validated and counted it; this is the second look).
func post(ctx context.Context, tx pgx.Tx, t domain.Txn) error {
	if err := t.Validate(); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO ledger_transactions (id, idempotency_key, kind, currency) VALUES ($1, $2, $3, $4)`,
		t.ID, t.Key, t.Kind, t.Currency); err != nil {
		return fmt.Errorf("insert ledger transaction %s: %w", t.Key, err)
	}
	for _, e := range t.Entries {
		if _, err := tx.Exec(ctx,
			`INSERT INTO ledger_accounts (id, kind, owner_id, currency) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
			e.Acct.ID, e.Acct.Kind, e.Acct.Owner, t.Currency); err != nil {
			return fmt.Errorf("ensure account %s: %w", e.Acct.ID, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO ledger_entries (txn_id, account_id, amount) VALUES ($1, $2, $3)`,
			t.ID, e.Acct.ID, e.Amount); err != nil {
			return fmt.Errorf("insert ledger entry on %s: %w", e.Acct.ID, err)
		}
	}
	return nil
}
