package domain

import (
	"slices"
	"time"
)

// Action is a review action and the statuses it may start from. Anything else is a conflict:
// a repeat of an action already taken is one too (the response says which status the driver has).
//
//	approve  PENDING or SUSPENDED -> APPROVED
//	reject   PENDING              -> REJECTED   (REJECTED to PENDING is the driver's new application)
//	suspend  APPROVED             -> SUSPENDED
type Action struct {
	Name string
	From []string
	To   string
	// Decision is the label the decision is counted under.
	Decision string
	// NeedsReason: a reason must be given for this action.
	NeedsReason bool
}

var reviewActions = map[string]Action{
	"approve": {"approve", []string{StatusPending, StatusSuspended}, StatusApproved, "approved", false},
	"reject":  {"reject", []string{StatusPending}, StatusRejected, "rejected", true},
	"suspend": {"suspend", []string{StatusApproved}, StatusSuspended, "suspended", true},
}

// LookupAction returns the action called name.
func LookupAction(name string) (Action, bool) {
	a, ok := reviewActions[name]
	if ok {
		a.From = slices.Clone(a.From)
	}
	return a, ok
}

// ActionNames lists the actions, sorted.
func ActionNames() []string {
	names := make([]string, 0, len(reviewActions))
	for n := range reviewActions {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// Decide applies an admin's decision; the result's time (the second result) is later than the
// driver's previous result.
func Decide(d Driver, a Action, admin, reason string, now time.Time) (Driver, time.Time, error) {
	if !slices.Contains(a.From, d.Status) {
		return d, time.Time{}, &TransitionError{From: d.Status, Action: a.To}
	}
	at := NextReviewTime(d.ReviewedAt, now)
	d.Status, d.Reason, d.ReviewedBy, d.ReviewedAt, d.StatusAt, d.UpdatedAt = a.To, reason, admin, &at, now, now
	return d, at, nil
}

// NextReviewTime is the time of a new review result: now, but always later than the previous
// one of the same driver, so a consumer can tell which is newer by the time alone even if
// the clock stepped back or two results fall in one microsecond.
func NextReviewTime(last *time.Time, now time.Time) time.Time {
	if last != nil && !now.After(*last) {
		return last.Add(time.Microsecond)
	}
	return now
}
