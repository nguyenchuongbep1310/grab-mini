package domain

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// MaxCancelReasonLength is the longest cancel reason, in characters.
const MaxCancelReasonLength = 200

// CancelledByRider is the published value for a cancel made by the rider.
const CancelledByRider = "RIDER"

// ErrReasonTooLong is returned for a cancel reason over MaxCancelReasonLength.
var ErrReasonTooLong = errors.New("cancel reason is too long")

// NormalizeCancelReason trims the reason and checks its length in runes.
func NormalizeCancelReason(reason string) (string, error) {
	reason = strings.TrimSpace(reason)
	if utf8.RuneCountInString(reason) > MaxCancelReasonLength {
		return "", ErrReasonTooLong
	}
	return reason, nil
}
