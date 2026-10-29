package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The ledger is double-entry. A transaction is two or more entries on
// accounts whose amounts sum to exactly zero. An entry's amount is signed:
// negative takes value out of the account (the rider paying), positive puts it
// in (the driver's payable balance, the platform's revenue). An account's
// balance is the sum of its entries; nothing is stored that could drift from
// them.
//
// A transaction that does not balance is refused in code (Txn.Validate, before
// anything is written) and, as a second line of defense, by a deferred
// constraint trigger in the database (migrations/00001) that rejects a commit
// holding an unbalanced transaction.
//
// The card network is outside this ledger: the rider account is debited when
// the PaymentProvider reports the charge captured, and the provider's own
// reference is kept on the payments row for reconciliation.

// Account kinds.
const (
	accountRider           = "RIDER"
	accountDriverPayable   = "DRIVER_PAYABLE"
	accountPlatformRevenue = "PLATFORM_REVENUE"
)

// account names a ledger account; id is its primary key.
type account struct {
	id, kind, owner string
}

func riderAccount(riderID string) account {
	return account{id: "rider:" + riderID, kind: accountRider, owner: riderID}
}

func driverPayableAccount(driverID string) account {
	return account{id: "driver_payable:" + driverID, kind: accountDriverPayable, owner: driverID}
}

// platformRevenueAccount is the single account the platform's commission accrues to.
func platformRevenueAccount() account {
	return account{id: "platform_revenue", kind: accountPlatformRevenue}
}

type entry struct {
	acct   account
	amount int64
}

// Txn is one balanced set of entries. Key is its idempotency key: posting the
// same key twice is refused by a unique index, so a retry can't book it twice.
type Txn struct {
	ID       string
	Key      string
	Kind     string
	Currency string
	Entries  []entry
}

var unbalancedRejected = promauto.NewCounter(prometheus.CounterOpts{
	Name: "ledger_unbalanced_rejected_total",
	Help: "Ledger transactions refused because their entries did not sum to zero. Should always be 0: a non-zero value is a bug in the code that built the transaction.",
})

// errUnbalanced is a ledger transaction that does not sum to zero. It is a
// programming error, never a property of an event, so it is not retried either
// (permanent): the event goes to the dead-letter topic for a person to look at.
type errUnbalanced struct{ detail string }

func (e *errUnbalanced) Error() string   { return "ledger transaction is not balanced: " + e.detail }
func (e *errUnbalanced) Permanent() bool { return true }

// Validate checks the transaction can be booked: at least two entries, each
// non-zero and within bounds, and a sum of exactly zero. It counts a rejected
// unbalanced transaction.
func (t Txn) Validate() error {
	if t.ID == "" || t.Key == "" || t.Currency == "" {
		return errors.New("ledger transaction needs an id, an idempotency key and a currency")
	}
	if len(t.Entries) < 2 {
		return fmt.Errorf("ledger transaction %s has %d entries, want at least 2", t.Key, len(t.Entries))
	}
	var sum int64
	for _, e := range t.Entries {
		if e.amount == 0 {
			return fmt.Errorf("ledger transaction %s has a zero entry on %s", t.Key, e.acct.id)
		}
		if e.amount > maxAmount || e.amount < -maxAmount {
			return &amountError{fmt.Sprintf("entry %d on %s is out of bounds", e.amount, e.acct.id)}
		}
		sum += e.amount
	}
	if sum != 0 {
		unbalancedRejected.Inc()
		return &errUnbalanced{fmt.Sprintf("%s sums to %d", t.Key, sum)}
	}
	return nil
}

// post writes the transaction and its entries inside tx, so a ledger write is
// always part of the caller's transaction. It validates first, so an
// unbalanced transaction never reaches the database.
func post(ctx context.Context, tx pgx.Tx, t Txn) error {
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
			e.acct.id, e.acct.kind, e.acct.owner, t.Currency); err != nil {
			return fmt.Errorf("ensure account %s: %w", e.acct.id, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO ledger_entries (txn_id, account_id, amount) VALUES ($1, $2, $3)`,
			t.ID, e.acct.id, e.amount); err != nil {
			return fmt.Errorf("insert ledger entry on %s: %w", e.acct.id, err)
		}
	}
	return nil
}

// chargeTxn is the ledger transaction for a captured charge of amount from the
// rider: the driver's share is credited to their payable account and the
// platform's commission to platform revenue. A zero share gets no entry (a
// ledger entry is never zero).
func chargeTxn(id, key, kind, riderID, driverID string, driverAmount, platformAmount int64) Txn {
	t := Txn{ID: id, Key: key, Kind: kind, Currency: currencyVND}
	t.Entries = append(t.Entries, entry{riderAccount(riderID), -(driverAmount + platformAmount)})
	if driverAmount != 0 {
		t.Entries = append(t.Entries, entry{driverPayableAccount(driverID), driverAmount})
	}
	if platformAmount != 0 {
		t.Entries = append(t.Entries, entry{platformRevenueAccount(), platformAmount})
	}
	return t
}
