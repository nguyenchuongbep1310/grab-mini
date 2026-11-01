package domain

import (
	"testing"
	"time"
)

func ptr(n int64) *int64 { return &n }

func TestFareIsTheLockedPriceSplitByCommission(t *testing.T) {
	p := Policy{CommissionBps: 2000}
	c, skip := p.Fare(ptr(100000))
	if skip != "" || c.Kind != KindFare || c.Total != 100000 || c.Driver != 80000 || c.Platform != 20000 {
		t.Errorf("Fare(100000) = %+v, %q", c, skip)
	}
}

func TestFareWithoutAPriceChargesNothing(t *testing.T) {
	p := Policy{CommissionBps: 2000}
	if _, skip := p.Fare(nil); skip != SkipNoPrice {
		t.Errorf("Fare(nil) skip = %q, want %q", skip, SkipNoPrice)
	}
	if _, skip := p.Fare(ptr(0)); skip != SkipZeroAmount {
		t.Errorf("Fare(0) skip = %q, want %q", skip, SkipZeroAmount)
	}
}

func TestTipGoesToTheDriverInFull(t *testing.T) {
	p := Policy{CommissionBps: 2000}
	c, skip := p.Tip(20000)
	if skip != "" || c.Kind != KindTip || c.Total != 20000 || c.Driver != 20000 || c.Platform != 0 {
		t.Errorf("Tip(20000) = %+v, %q", c, skip)
	}
	if _, skip := p.Tip(0); skip != SkipZeroAmount {
		t.Errorf("Tip(0) skip = %q, want %q", skip, SkipZeroAmount)
	}
}

func TestCancellationFeeRules(t *testing.T) {
	const grace = 3 * time.Minute
	cases := []struct {
		name        string
		fee         int64
		cancelledBy string
		price       *int64
		assignedFor *time.Duration
		wantTotal   int64
		wantSkip    string
	}{
		{"rider cancels after the grace period", 15000, CancelledByRider, ptr(100000), dur(grace + time.Second), 15000, ""},
		{"rider cancels within the grace period", 15000, CancelledByRider, ptr(100000), dur(grace - time.Second), 0, SkipWithinGrace},
		{"exactly at the grace limit is still free", 15000, CancelledByRider, ptr(100000), dur(grace), 0, SkipWithinGrace},
		{"immediately after the assignment", 15000, CancelledByRider, ptr(100000), dur(0), 0, SkipWithinGrace},
		{"assignment never seen: not held against the rider", 15000, CancelledByRider, ptr(100000), nil, 0, SkipNoAssignment},
		{"rider cancels, fee off", 0, CancelledByRider, ptr(100000), dur(time.Hour), 0, SkipFeeDisabled},
		{"driver cancels late, fee on: never charged", 15000, CancelledByDriver, ptr(100000), dur(time.Hour), 0, SkipDriverCancel},
		{"driver cancels, fee off", 0, CancelledByDriver, ptr(100000), dur(time.Hour), 0, SkipDriverCancel},
		{"unknown canceller: not charged", 15000, "", ptr(100000), dur(time.Hour), 0, SkipUnknownActor},
		{"fee is capped at the price", 15000, CancelledByRider, ptr(10000), dur(time.Hour), 10000, ""},
		{"free ride, nothing to cap to", 15000, CancelledByRider, ptr(0), dur(time.Hour), 0, SkipZeroAmount},
		{"price unknown: the configured fee", 15000, CancelledByRider, nil, dur(time.Hour), 15000, ""},
	}
	for _, c := range cases {
		p := Policy{CommissionBps: 2000, CancellationFee: c.fee, CancellationGrace: grace}
		got, skip := p.Cancellation(c.cancelledBy, c.price, c.assignedFor)
		if skip != c.wantSkip || got.Total != c.wantTotal {
			t.Errorf("%s: total %d, skip %q; want %d, %q", c.name, got.Total, skip, c.wantTotal, c.wantSkip)
		}
		if skip == "" {
			if got.Kind != KindCancellationFee || got.Driver+got.Platform != got.Total {
				t.Errorf("%s: charge %+v is not a balanced cancellation fee", c.name, got)
			}
		}
	}
}

// With no grace period configured, any positive time held is chargeable and only
// a cancel in the same instant as the assignment is free.
func TestZeroGraceChargesAsSoonAsTheDriverIsAssigned(t *testing.T) {
	p := Policy{CommissionBps: 2000, CancellationFee: 15000}
	if _, skip := p.Cancellation(CancelledByRider, ptr(100000), dur(time.Millisecond)); skip != "" {
		t.Errorf("1ms after the assignment with no grace: skip %q, want a charge", skip)
	}
	if _, skip := p.Cancellation(CancelledByRider, ptr(100000), dur(0)); skip != SkipWithinGrace {
		t.Errorf("same instant: skip %q, want %q", skip, SkipWithinGrace)
	}
}

func dur(d time.Duration) *time.Duration { return &d }

func TestPolicyValidate(t *testing.T) {
	good := []Policy{{}, {CommissionBps: 2000, CancellationFee: 15000, CancellationGrace: 3 * time.Minute}, {CommissionBps: 10000}}
	for _, p := range good {
		if err := p.Validate(); err != nil {
			t.Errorf("%+v: %v", p, err)
		}
	}
	bad := []Policy{{CommissionBps: -1}, {CommissionBps: 10001}, {CancellationFee: -5}, {CancellationFee: MaxAmount + 1}, {CancellationGrace: -time.Second}}
	for _, p := range bad {
		if err := p.Validate(); err == nil {
			t.Errorf("%+v: want an error", p)
		}
	}
}
