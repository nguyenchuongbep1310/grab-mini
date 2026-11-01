package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/payment-service/internal/domain"
)

var unbalancedRejected = promauto.NewCounter(prometheus.CounterOpts{
	Name: "ledger_unbalanced_rejected_total",
	Help: "Ledger transactions refused because their entries did not sum to zero. Should always be 0: a non-zero value is a bug in the code that built the transaction.",
})

// validateTxn is Txn.Validate plus the count of a rejected unbalanced transaction.
func validateTxn(t domain.Txn) error {
	err := t.Validate()
	var ub *domain.UnbalancedError
	if errors.As(err, &ub) {
		unbalancedRejected.Inc()
	}
	return err
}

// post writes the transaction and its entries inside tx, so a ledger write is
// always part of the caller's transaction. It validates first, so an
// unbalanced transaction never reaches the database.
func post(ctx context.Context, tx pgx.Tx, t domain.Txn) error {
	if err := validateTxn(t); err != nil {
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
