// Package app holds the order-service use cases and the ports they need.
package app

import (
	"context"
	"time"
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
