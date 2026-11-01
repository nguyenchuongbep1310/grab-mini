// Package app holds payment-service's use cases and the ports they need. It
// depends on internal/domain and the standard library; the database, Kafka,
// the payment provider and Prometheus are reached only through the interfaces
// declared here, which adapters implement (internal/arch_test.go enforces that).
package app

import (
	"context"
	"time"

	"grab-mini/payment-service/internal/domain"
)

// ---------------------------------------------------------------- provider ----

// PaymentProvider is the seam to a card processor or wallet. The service never sees a
// card number, expiry or CVV: the rider's payment method lives with the
// provider (a tokenised method saved when the rider signed up there), and a
// charge names only the rider. A real implementation would map RiderID to that
// token, call the processor with IdempotencyKey, and map its answer to a
// ChargeResult.
//
// Contract:
//   - Charge is idempotent on IdempotencyKey: asking again with the same key
//     returns the original result and never charges twice.
//   - A decline (insufficient funds, a blocked method) is a normal answer, not
//     an error: return a ChargeResult with Status ChargeDeclined.
//   - An error means "unknown, try again" (timeout, 5xx): the event is retried
//     through the retry topics, and the same key makes the retry safe.
type PaymentProvider interface {
	Name() string
	Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error)
}

type ChargeRequest struct {
	// IdempotencyKey is stable for one logical charge ("fare:<tripId>").
	IdempotencyKey string
	RiderID        string
	// Amount is in minor units of Currency (VND: whole dong), always positive.
	Amount      int64
	Currency    string
	Description string
}

type ChargeStatus string

const (
	ChargeCaptured ChargeStatus = "CAPTURED"
	ChargeDeclined ChargeStatus = "DECLINED"
)

type ChargeResult struct {
	Status ChargeStatus
	// Ref is the provider's own id for the charge, kept for reconciliation.
	Ref           string
	DeclineReason string
}

// ------------------------------------------------------------------- store ----

// OrderStore keeps the price (and promo discount) locked at booking, so a trip
// event never needs a call to Order Service.
type OrderStore interface {
	// SaveOrder stores the order; the first copy wins, a redelivery changes nothing.
	SaveOrder(ctx context.Context, orderID, riderID string, price *int64, discount int64) error
	// FindOrder reports false when no order.created has been recorded for orderID.
	FindOrder(ctx context.Context, orderID string) (domain.OrderInfo, bool, error)
}

// AssignmentStore keeps when each order got its driver (the cancellation grace
// period starts there).
type AssignmentStore interface {
	// SaveAssignment stores the moment; the first one wins.
	SaveAssignment(ctx context.Context, orderID string, at time.Time) error
	// AssignedAt reports false when the assignment was never recorded.
	AssignedAt(ctx context.Context, orderID string) (time.Time, bool, error)
}

// PaymentLookup answers whether a charge was already recorded.
type PaymentLookup interface {
	Exists(ctx context.Context, tripID, kind string) (bool, error)
}

// UnitOfWork runs fn in one database transaction: it commits when fn returns
// nil and rolls back when it returns an error. Everything a settlement writes
// goes through the Tx, so the dedupe row, the payment, the ledger entries and
// the outbox row commit together or not at all (ADR 0003, 0004, 0010).
type UnitOfWork interface {
	Do(ctx context.Context, fn func(Tx) error) error
}

// Tx is what a settlement may write inside the unit of work.
type Tx interface {
	// MarkEventProcessed records that the event was applied and reports whether
	// this is the first time.
	MarkEventProcessed(ctx context.Context, eventID string) (first bool, err error)
	// InsertPayment stores the payment; false means (trip, kind) already had one.
	InsertPayment(ctx context.Context, p domain.Payment) (inserted bool, err error)
	// PostLedger writes a balanced ledger transaction.
	PostLedger(ctx context.Context, t domain.Txn) error
	// EnqueuePaymentRecorded queues payment.recorded (encoded by PaymentEncoder)
	// for the outbox relay, keyed by the trip.
	EnqueuePaymentRecorded(ctx context.Context, tripID string, payload []byte) error
}

// PaymentEncoder turns the announcement into the bytes to publish. Encoding can
// call Schema Registry, so it runs before the transaction opens.
type PaymentEncoder interface {
	EncodePaymentRecorded(ctx context.Context, rec PaymentRecorded) ([]byte, error)
}

// Reader is the read side behind the payments and earnings queries.
type Reader interface {
	RiderPayments(ctx context.Context, tripID, riderID string) ([]RiderPayment, error)
	DriverPayouts(ctx context.Context, tripID, driverID string) ([]DriverPayout, error)
	// EarningsByKind sums a driver's captured payments per kind in [from, to).
	EarningsByKind(ctx context.Context, driverID string, from, to *time.Time) ([]KindTotal, error)
	// PayableBalance is what the platform owes the driver now, from the ledger.
	PayableBalance(ctx context.Context, driverID string) (int64, error)
}

// --------------------------------------------------------------- reporting ----

// Metrics receives the facts worth counting. The Prometheus implementation keeps
// the exported metric names and labels.
type Metrics interface {
	Recorded(kind, status string)
	CapturedAmount(kind string, amount int64)
	Duplicate(kind string)
	Skipped(reason string)
	ProviderError()
	LedgerUnbalanced()
}
