package app

import (
	"errors"

	"grab-mini/pkg/surge"
)

// ErrInvalidQuote is a quote that is forged, damaged or for another trip.
var ErrInvalidQuote = errors.New("invalid quote")

// ReasonQuoteUsed is why a rider is asked to confirm a fresh quote after
// presenting one that already priced another booking.
const ReasonQuoteUsed = "this quote was already used for another booking"

// QuoteConflict is returned when the multiplier moved past the tolerance since
// the rider's (expired) quote, or the quote was already used; NewQuote is what
// they can confirm instead.
type QuoteConflict struct {
	NewQuote surge.Quote
	// Why the rider is asked to confirm; empty means the price moved.
	Reason string
}

func (c *QuoteConflict) Error() string {
	if c.Reason != "" {
		return c.Reason
	}
	return "the price has changed"
}

// PromoError is a promo token that cannot be applied. Code is the stable,
// machine-readable reason in the response body; Conflict is true for a token
// already used for another booking (the HTTP adapter answers 409), false for
// a refusal (400).
type PromoError struct {
	Code     string
	Message  string
	Conflict bool
}

func (e *PromoError) Error() string { return e.Code + ": " + e.Message }

// ErrDuplicateKey is what Tx.SaveKey returns when another request with the same
// rider and Idempotency-Key committed first.
var ErrDuplicateKey = errors.New("idempotency key already saved")

// ErrKeyMismatch is an Idempotency-Key used again with a different request body.
var ErrKeyMismatch = errors.New("this Idempotency-Key was already used with a different request body")

// ErrForbidden is a rider asking for another rider's orders.
var ErrForbidden = errors.New("you can only read your own orders")

// ErrInternal is a failure the caller cannot fix; it has been logged.
var ErrInternal = errors.New("internal error")

// BadOutcomeError is an event that can never be applied (its order id isn't a
// UUID). Permanent, so it goes straight to the dead-letter topic.
type BadOutcomeError struct{ Reason string }

func (e *BadOutcomeError) Error() string { return "unusable matching outcome: " + e.Reason }

// Permanent tells the consumer not to retry.
func (e *BadOutcomeError) Permanent() bool { return true }
