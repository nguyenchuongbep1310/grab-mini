package domain

import "time"

// Resyncable says whether a driver with this status has a result to repeat; PENDING and NONE
// have none.
func Resyncable(status string) bool {
	return status == StatusApproved || status == StatusRejected || status == StatusSuspended
}

// ForResync is the driver as its repeated result carries it, and the result's time: dated
// like a new decision (later than the driver's last one). The stored profile is not changed.
func ForResync(d Driver, now time.Time) (Driver, time.Time) {
	at := NextReviewTime(d.ReviewedAt, now)
	if d.Reason == "" {
		d.Reason = ResyncReviewer
	}
	d.ReviewedBy = ResyncReviewer
	return d, at
}
