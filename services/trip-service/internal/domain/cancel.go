package domain

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// ValidationError is a client mistake: Msg is what the client is told.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

// Who cancelled a trip (the published values: events.CancelledByRider/Driver).
const (
	CancelledByRider  = "RIDER"
	CancelledByDriver = "DRIVER"
)

const maxCancelReasonLength = 200

// CancelInfo says who cancelled a trip and why. It is recorded with the
// transition to CANCELLED and goes out on the event.
type CancelInfo struct{ By, Reason string }

// NewCancelInfo works out who is cancelling (from the side they act as) and
// checks the optional reason: trimmed, at most 200 characters.
func NewCancelInfo(who Actor, reason string) (CancelInfo, error) {
	reason = strings.TrimSpace(reason)
	if utf8.RuneCountInString(reason) > maxCancelReasonLength {
		return CancelInfo{}, &ValidationError{"reason is at most 200 characters"}
	}
	by := CancelledByRider
	if who.Role == RoleDriver {
		by = CancelledByDriver
	}
	return CancelInfo{By: by, Reason: reason}, nil
}

// OrderCancelledInfo is the cancellation recorded when order.cancelled arrives.
// Only riders cancel orders, so an event that names nobody (older messages) is the rider's.
func OrderCancelledInfo(by, reason string) CancelInfo {
	if by == "" {
		by = CancelledByRider
	}
	return CancelInfo{By: by, Reason: reason}
}

// Rating limits.
const (
	MaxCommentLength = 500
	MaxTipAmount     = 1_000_000 // VND
)

// Rating is a validated rating: 1 to 5 stars, a trimmed comment, and an
// optional tip in whole VND (recorded only; Payment Service charges it).
type Rating struct {
	Stars   int
	Comment string
	Tip     int64
}

// NewRating validates a rider's rating. The comment is trimmed first and its
// length counted in characters; control characters other than newline and tab
// are refused. The error is a *ValidationError whose message the client sees.
func NewRating(stars int, comment string, tip int64) (Rating, error) {
	comment = strings.TrimSpace(comment)
	if stars < 1 || stars > 5 || utf8.RuneCountInString(comment) > MaxCommentLength {
		return Rating{}, &ValidationError{"stars (1 to 5) are required; comment is at most 500 characters"}
	}
	for _, r := range comment {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return Rating{}, &ValidationError{"comment contains control characters"}
		}
	}
	if tip < 0 || tip > MaxTipAmount {
		return Rating{}, &ValidationError{"tipAmount must be a whole number from 0 to 1000000"}
	}
	return Rating{Stars: stars, Comment: comment, Tip: tip}, nil
}
