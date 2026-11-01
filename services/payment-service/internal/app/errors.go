package app

// InvalidEventError is an event that can never be charged (no driver, a negative
// tip, a currency we don't handle). Permanent: straight to the dead-letter
// topic instead of being retried.
type InvalidEventError struct{ Reason string }

func (e *InvalidEventError) Error() string   { return "invalid payment event: " + e.Reason }
func (e *InvalidEventError) Permanent() bool { return true }

// OrderUnknownError asks for a retry: the trip event arrived before order.created
// was recorded (they are different topics, so there is no order between them).
// If the order never shows up the event ends in the dead-letter topic, which
// pages: a finished ride without a price is a ride nobody can charge.
type OrderUnknownError struct{ OrderID string }

func (e *OrderUnknownError) Error() string {
	return "no price recorded yet for order " + e.OrderID + " (order.created not seen); retrying"
}
