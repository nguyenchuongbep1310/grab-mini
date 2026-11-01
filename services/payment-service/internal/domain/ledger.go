package domain

import (
	"errors"
	"fmt"
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
// holding an unbalanced transaction. Counting a refusal (the metric
// ledger_unbalanced_rejected_total) is the use case's job, not the domain's.
//
// The card network is outside this ledger: the rider account is debited when
// the payment provider reports the charge captured, and the provider's own
// reference is kept on the payments row for reconciliation.

// Account kinds.
const (
	AccountRider           = "RIDER"
	AccountDriverPayable   = "DRIVER_PAYABLE"
	AccountPlatformRevenue = "PLATFORM_REVENUE"
)

// Account names a ledger account; ID is its primary key.
type Account struct {
	ID, Kind, Owner string
}

func RiderAccount(riderID string) Account {
	return Account{ID: "rider:" + riderID, Kind: AccountRider, Owner: riderID}
}

func DriverPayableAccount(driverID string) Account {
	return Account{ID: "driver_payable:" + driverID, Kind: AccountDriverPayable, Owner: driverID}
}

// PlatformRevenueAccount is the single account the platform's commission accrues to.
func PlatformRevenueAccount() Account {
	return Account{ID: "platform_revenue", Kind: AccountPlatformRevenue}
}

// Entry is one signed amount on one account.
type Entry struct {
	Acct   Account
	Amount int64
}

// Txn is one balanced set of entries. Key is its idempotency key: posting the
// same key twice is refused by a unique index, so a retry can't book it twice.
type Txn struct {
	ID       string
	Key      string
	Kind     string
	Currency string
	Entries  []Entry
}

// UnbalancedError is a ledger transaction that does not sum to zero. It is a
// programming error, never a property of an event, so it is not retried either
// (permanent): the event goes to the dead-letter topic for a person to look at.
type UnbalancedError struct{ Detail string }

func (e *UnbalancedError) Error() string   { return "ledger transaction is not balanced: " + e.Detail }
func (e *UnbalancedError) Permanent() bool { return true }

// Validate checks the transaction can be booked: at least two entries, each
// non-zero and within bounds, and a sum of exactly zero. One that does not sum
// to zero is an *UnbalancedError.
func (t Txn) Validate() error {
	if t.ID == "" || t.Key == "" || t.Currency == "" {
		return errors.New("ledger transaction needs an id, an idempotency key and a currency")
	}
	if len(t.Entries) < 2 {
		return fmt.Errorf("ledger transaction %s has %d entries, want at least 2", t.Key, len(t.Entries))
	}
	var sum int64
	for _, e := range t.Entries {
		if e.Amount == 0 {
			return fmt.Errorf("ledger transaction %s has a zero entry on %s", t.Key, e.Acct.ID)
		}
		if e.Amount > MaxAmount || e.Amount < -MaxAmount {
			return &AmountError{fmt.Sprintf("entry %d on %s is out of bounds", e.Amount, e.Acct.ID)}
		}
		sum += e.Amount
	}
	if sum != 0 {
		return &UnbalancedError{fmt.Sprintf("%s sums to %d", t.Key, sum)}
	}
	return nil
}

// ChargeTxn is the ledger transaction for a captured charge of amount from the
// rider: the driver's share is credited to their payable account and the
// platform's commission to platform revenue. A zero share gets no entry (a
// ledger entry is never zero).
func ChargeTxn(id, key, kind, riderID, driverID string, driverAmount, platformAmount int64) Txn {
	t := Txn{ID: id, Key: key, Kind: kind, Currency: CurrencyVND}
	t.Entries = append(t.Entries, Entry{RiderAccount(riderID), -(driverAmount + platformAmount)})
	if driverAmount != 0 {
		t.Entries = append(t.Entries, Entry{DriverPayableAccount(driverID), driverAmount})
	}
	if platformAmount != 0 {
		t.Entries = append(t.Entries, Entry{PlatformRevenueAccount(), platformAmount})
	}
	return t
}
