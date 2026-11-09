package domain

import "errors"

var (
	// ErrNotFound: no promotion has the code asked for.
	ErrNotFound = errors.New("no such promotion")
	// ErrCodeExists: a promotion with this code already exists.
	ErrCodeExists = errors.New("a promotion with this code already exists")
)

// InvalidEvent is an event that can never be applied (no order id, a code that
// does not exist here). Permanent: straight to the dead-letter topic instead of
// being retried, where it pages: a discount that was given and not recorded.
type InvalidEvent struct{ Reason string }

func (e *InvalidEvent) Error() string   { return "invalid promotion event: " + e.Reason }
func (e *InvalidEvent) Permanent() bool { return true }

// InvalidPromotionError is a promotion an admin asked for that cannot exist.
type InvalidPromotionError struct{ Message string }

func (e *InvalidPromotionError) Error() string { return e.Message }

// InvalidRequestError is a malformed validate request (its price or vehicle).
type InvalidRequestError struct{ Message string }

func (e *InvalidRequestError) Error() string { return e.Message }
