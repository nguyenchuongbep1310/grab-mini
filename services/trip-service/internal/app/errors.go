package app

import (
	"errors"

	"grab-mini/trip-service/internal/domain"
)

// What a use case can answer besides success. The HTTP adapter maps each to a
// status; a use case has already logged the cause of ErrInternal.
var (
	// ErrTripNotFound covers "no such trip" and "not yours to know about".
	ErrTripNotFound = domain.ErrTripNotFound
	// ErrInternal means something failed and was logged; answer with a generic error.
	ErrInternal = errors.New("internal error")
	// ErrForbidden: only a driver may read their own active trip.
	ErrForbidden = errors.New("you can only read your own active trip")
	// ErrRiderMismatch: the rating names a rider other than the caller.
	ErrRiderMismatch = errors.New("riderId doesn't match the token")
	// ErrNotCompleted: the action needs a COMPLETED trip.
	ErrNotCompleted = errors.New("the trip is not completed")
	// ErrAlreadyRated: a trip is rated once.
	ErrAlreadyRated = errors.New("this trip was already rated")
)

// CannotTransitionError: the trip is not in a status the action may start from.
type CannotTransitionError struct{ To domain.Status }

func (e *CannotTransitionError) Error() string {
	return "trip cannot transition to " + string(e.To) + " from its current status"
}

// errTripAppeared asks the consumer's retry: the trip for the order was created
// after we looked for it, so the status event we prepared doesn't name it.
var errTripAppeared = errors.New("the trip appeared while its order was being cancelled; retrying")

// errRollback ends a unit of work without committing and without an error.
var errRollback = errors.New("roll back")
