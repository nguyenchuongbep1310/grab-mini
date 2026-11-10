package domain

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestNextReviewTimeIsAlwaysLater(t *testing.T) {
	now := mustTime("2026-01-02T03:04:05.000100Z")
	if got := NextReviewTime(nil, now); !got.Equal(now) {
		t.Errorf("no previous: %v", got)
	}
	earlier := mustTime("2026-01-02T03:04:04Z")
	if got := NextReviewTime(&earlier, now); !got.Equal(now) {
		t.Errorf("earlier previous: %v", got)
	}
	same := now
	if got := NextReviewTime(&same, now); !got.After(same) {
		t.Errorf("same time: %v is not after %v", got, same)
	}
	later := mustTime("2026-01-02T03:05:00Z")
	if got := NextReviewTime(&later, now); !got.After(later) {
		t.Errorf("clock stepped back: %v is not after %v", got, later)
	}
}

func mustTime(s string) (t time.Time) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestDecideTableAndTheTransitionMessage(t *testing.T) {
	now := mustTime("2026-03-04T05:06:07Z")
	statuses := []string{StatusNone, StatusPending, StatusApproved, StatusRejected, StatusSuspended}
	want := map[string]struct {
		from        []string
		to, dec     string
		needsReason bool
	}{
		"approve": {[]string{StatusPending, StatusSuspended}, StatusApproved, "approved", false},
		"reject":  {[]string{StatusPending}, StatusRejected, "rejected", true},
		"suspend": {[]string{StatusApproved}, StatusSuspended, "suspended", true},
	}
	if got := ActionNames(); !reflect.DeepEqual(got, []string{"approve", "reject", "suspend"}) {
		t.Fatalf("ActionNames = %v", got)
	}
	if _, ok := LookupAction("delete"); ok {
		t.Error("unknown action found")
	}
	for name, w := range want {
		a, ok := LookupAction(name)
		if !ok || a.Name != name || a.To != w.to || a.Decision != w.dec || a.NeedsReason != w.needsReason || !reflect.DeepEqual(a.From, w.from) {
			t.Fatalf("%s: %+v", name, a)
		}
		for _, st := range statuses {
			d := Driver{ID: "d1", Status: st, Name: "An"}
			got, at, err := Decide(d, a, "admin-1", "because", now)
			allowed := false
			for _, f := range w.from {
				allowed = allowed || f == st
			}
			if !allowed {
				var te *TransitionError
				if !errors.As(err, &te) || te.From != st || te.Action != w.to {
					t.Errorf("%s from %s: err = %v", name, st, err)
				}
				if err.Error() != "a driver who is "+st+" cannot be "+w.to {
					t.Errorf("message = %q", err.Error())
				}
				if got.Status != st {
					t.Errorf("%s from %s: driver changed to %s", name, st, got.Status)
				}
				continue
			}
			if err != nil {
				t.Errorf("%s from %s: %v", name, st, err)
				continue
			}
			if got.Status != w.to || got.Reason != "because" || got.ReviewedBy != "admin-1" || !got.StatusAt.Equal(now) || !got.UpdatedAt.Equal(now) ||
				got.ReviewedAt == nil || !got.ReviewedAt.Equal(at) || !at.Equal(now) || got.Name != "An" {
				t.Errorf("%s from %s: %+v at %v", name, st, got, at)
			}
		}
	}
}

func TestDecideDatesTheResultLaterThanThePreviousOne(t *testing.T) {
	now := mustTime("2026-03-04T05:06:07Z")
	last := now.Add(time.Hour)
	a, _ := LookupAction("approve")
	got, at, err := Decide(Driver{Status: StatusPending, ReviewedAt: &last}, a, "x", "", now)
	if err != nil {
		t.Fatal(err)
	}
	if !at.After(last) || !got.ReviewedAt.Equal(at) || !got.StatusAt.Equal(now) {
		t.Errorf("at %v, driver %+v", at, got)
	}
}
