package domain

import (
	"testing"
	"time"
)

func TestForResyncDatesAndFillsTheReasonAndReviewer(t *testing.T) {
	now := mustTime("2026-03-04T05:06:07Z")
	last := now.Add(time.Hour)
	d := Driver{ID: "d1", Status: StatusApproved, Reason: "", ReviewedBy: "admin-1", ReviewedAt: &last, StatusAt: last}
	got, at := ForResync(d, now)
	if !at.After(last) || got.Reason != ResyncReviewer || got.ReviewedBy != ResyncReviewer {
		t.Errorf("%+v at %v", got, at)
	}
	if got.Status != StatusApproved || got.ReviewedAt != &last || !got.StatusAt.Equal(last) {
		t.Errorf("the profile moved: %+v", got)
	}
	if d.Reason != "" || d.ReviewedBy != "admin-1" {
		t.Error("the input was changed")
	}

	d = Driver{Status: StatusRejected, Reason: "bad plate", ReviewedBy: "admin-1"}
	got, at = ForResync(d, now)
	if got.Reason != "bad plate" || got.ReviewedBy != ResyncReviewer || !at.Equal(now) {
		t.Errorf("%+v at %v", got, at)
	}
}

func TestResyncableStatuses(t *testing.T) {
	for st, want := range map[string]bool{StatusApproved: true, StatusRejected: true, StatusSuspended: true,
		StatusPending: false, StatusNone: false, "": false, "approved": false} {
		if Resyncable(st) != want {
			t.Errorf("Resyncable(%q) = %v", st, !want)
		}
	}
}
