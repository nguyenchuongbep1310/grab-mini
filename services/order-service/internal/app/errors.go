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
