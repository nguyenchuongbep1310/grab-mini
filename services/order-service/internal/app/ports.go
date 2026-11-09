// Package app holds the order-service use cases and the ports they need.
package app

import (
	"context"
	"time"

	"grab-mini/order-service/internal/domain"
)

// MultiplierSource gives the current surge multiplier of a cell. It cannot
// fail: when it can't know, the answer is 1.0 (an adapter fails open, so a
// booking is never blocked by pricing).
type MultiplierSource interface {
	Multiplier(ctx context.Context, vehicle, cell string) float64
}

// ClaimStore makes a quote or a promo token single-use. The ids it receives
// are the raw quote id and "promo:"+token; an adapter may hash them.
type ClaimStore interface {
	// Claim marks id used and reports whether this call was the first to. It
	// keeps the mark for ttl (the id's remaining life).
	Claim(ctx context.Context, id string, ttl time.Duration) (first bool, err error)
	// Release takes the mark back, for a booking that failed after claiming.
	Release(ctx context.Context, id string)
}

// Metrics counts what happened. The result strings are the label values of
// the order-service counters.
type Metrics interface {
	OrderCreated(vehicle string)
	OrderCancelled()
	// Pricing: quote, repriced, requote, quote_reused, quote_unchecked, invalid_quote.
	Pricing(result string)
	// SurgeFailOpen counts a multiplier read that failed and answered 1.0.
	SurgeFailOpen()
	// Promo: applied, unchecked, used, invalid, expired, wrong_rider, wrong_price, wrong_vehicle.
	Promo(result string)
	// Idempotent: the result of an Idempotency-Key lookup.
	Idempotent(result string)
}

// OrderReader loads orders. Both methods return domain.ErrOrderNotFound when
// there is none.
type OrderReader interface {
	ByID(ctx context.Context, id string) (domain.Order, error)
	// LatestByRider is the rider's most recently created order.
	LatestByRider(ctx context.Context, riderID string) (domain.Order, error)
}

// KeyStore reads and purges Idempotency-Key records outside a transaction.
type KeyStore interface {
	// Lookup returns the live record for (riderID, key), or nil, nil when
	// there is none. A record older than domain.IdempotencyTTL is deleted and
	// reported as absent.
	Lookup(ctx context.Context, riderID, key string) (*domain.KeyRecord, error)
	// Purge deletes one batch of expired records and returns how many went.
	Purge(ctx context.Context) (int64, error)
}

// Outcomes applies what Matching and Trip Service report. Each update is
// conditional on the current status, so a redelivered or out-of-order event
// changes nothing; changed reports whether a row moved.
type Outcomes interface {
	// Set moves a PENDING order to status and records driverID ("" for none).
	Set(ctx context.Context, orderID string, status domain.Status, driverID string) (changed bool, err error)
	// CancelMatched moves a MATCHED order to CANCELLED, by whom and why.
	CancelMatched(ctx context.Context, orderID, by, reason string) (changed bool, err error)
}

// UnitOfWork runs fn in one database transaction: it commits when fn returns
// nil and rolls back when it returns an error. Do returns fn's error as it is,
// or the error of beginning or committing the transaction.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(Tx) error) error
}

// Tx is what a use case may do inside the unit of work.
type Tx interface {
	// LockKey serializes requests that share a rider and Idempotency-Key until
	// the transaction ends, so a duplicate waits and then finds the first's record.
	LockKey(ctx context.Context, riderID, key string) error
	// Lookup is KeyStore.Lookup inside the transaction.
	Lookup(ctx context.Context, riderID, key string) (*domain.KeyRecord, error)
	// SaveKey stores rec under (riderID, rec.Key); ErrDuplicateKey when another
	// request with that rider and key committed first.
	SaveKey(ctx context.Context, riderID string, rec domain.KeyRecord) error
	// InsertOrder adds a PENDING order.
	InsertOrder(ctx context.Context, o NewOrder) error
	// CancelPending moves the order to CANCELLED by the rider if, and only if,
	// it is still PENDING; cancelled is false when it was not.
	CancelPending(ctx context.Context, orderID, reason string) (cancelled bool, err error)
	// QueueOrderCreated and QueueOrderCancelled add the encoded event to the
	// outbox, keyed by the order id.
	QueueOrderCreated(ctx context.Context, orderID string, payload []byte) error
	QueueOrderCancelled(ctx context.Context, orderID string, payload []byte) error
}

// Encoder encodes the events this service publishes, ready for the outbox.
type Encoder interface {
	OrderCreated(ctx context.Context, e OrderCreated) ([]byte, error)
	OrderCancelled(ctx context.Context, e OrderCancelled) ([]byte, error)
}

// ReceiptEncoder renders the response to a new booking. The bytes are stored
// with an Idempotency-Key and replayed as they are, so the encoding must not
// change (the HTTP adapter owns it).
type ReceiptEncoder func(Receipt) []byte

// Waker is the outbox relay: woken after a commit that queued an event.
type Waker interface {
	Wake()
}
