package app

import (
	"github.com/google/uuid"

	"grab-mini/promotion-service/internal/domain"
)

// StepError is a port failure that is not a business refusal: Step names what
// was being done ("load promotion", "count redemptions", ...). A transport
// logs it and answers 500.
type StepError struct {
	Step string
	Err  error
}

func (e *StepError) Error() string { return e.Step + ": " + e.Err.Error() }
func (e *StepError) Unwrap() error { return e.Err }

func step(name string, err error) error { return &StepError{Step: name, Err: err} }

// invalidEvent is an event that can never be applied: permanent, so the
// consumer sends it to the dead-letter topic instead of retrying it.
func invalidEvent(reason string) error { return &domain.InvalidEvent{Reason: reason} }

// checkEventID refuses an eventId that is not a UUID. It runs first inside
// the transaction, before the marker is written.
func checkEventID(eventID string) error {
	if _, err := uuid.Parse(eventID); err != nil {
		return invalidEvent("eventId " + eventID + " is not a UUID")
	}
	return nil
}
