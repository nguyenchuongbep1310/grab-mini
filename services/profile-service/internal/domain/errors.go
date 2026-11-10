package domain

import (
	"errors"
	"fmt"
)

var (
	ErrNoDriver = errors.New("no such driver")
	// ErrNoProfile: apply before saving a profile.
	ErrNoProfile = errors.New("no driver profile yet")
	ErrSuspended = errors.New("the driver is suspended")
)

// TransitionError is a review that the driver's status does not allow.
type TransitionError struct{ From, Action string }

func (e *TransitionError) Error() string {
	return fmt.Sprintf("a driver who is %s cannot be %s", e.From, e.Action)
}

// IncompleteError lists what a driver must fill in before applying.
type IncompleteError struct{ Missing []string }

func (e *IncompleteError) Error() string { return fmt.Sprintf("profile is missing %v", e.Missing) }

// InvalidRequestError is a list query the caller got wrong; Message is shown as it is.
type InvalidRequestError struct{ Message string }

func (e *InvalidRequestError) Error() string { return e.Message }
