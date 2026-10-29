package main

import (
	"testing"

	"grab-mini/pkg/events"
)

func ptr(n int64) *int64 { return &n }

func TestFareIsTheLockedPriceSplitByCommission(t *testing.T) {
	p := Policy{CommissionBps: 2000}
	c, skip := p.Fare(ptr(100000))
	if skip != "" || c.Kind != events.PaymentKindFare || c.Total != 100000 || c.Driver != 80000 || c.Platform != 20000 {
		t.Errorf("Fare(100000) = %+v, %q", c, skip)
	}
}

func TestFareWithoutAPriceChargesNothing(t *testing.T) {
	p := Policy{CommissionBps: 2000}
	if _, skip := p.Fare(nil); skip != skipNoPrice {
		t.Errorf("Fare(nil) skip = %q, want %q", skip, skipNoPrice)
	}
	if _, skip := p.Fare(ptr(0)); skip != skipZeroAmount {
		t.Errorf("Fare(0) skip = %q, want %q", skip, skipZeroAmount)
	}
}

func TestTipGoesToTheDriverInFull(t *testing.T) {
	p := Policy{CommissionBps: 2000}
	c, skip := p.Tip(20000)
	if skip != "" || c.Kind != events.PaymentKindTip || c.Total != 20000 || c.Driver != 20000 || c.Platform != 0 {
		t.Errorf("Tip(20000) = %+v, %q", c, skip)
	}
	if _, skip := p.Tip(0); skip != skipZeroAmount {
		t.Errorf("Tip(0) skip = %q, want %q", skip, skipZeroAmount)
	}
}

func TestCancellationFeeRules(t *testing.T) {
	cases := []struct {
		name        string
		fee         int64
		cancelledBy string
		price       *int64
		wantTotal   int64
		wantSkip    string
	}{
		{"rider cancels, fee on", 15000, events.CancelledByRider, ptr(100000), 15000, ""},
		{"rider cancels, fee off (the default)", 0, events.CancelledByRider, ptr(100000), 0, skipFeeDisabled},
		{"driver cancels, fee on: never charged", 15000, events.CancelledByDriver, ptr(100000), 0, skipDriverCancel},
		{"driver cancels, fee off", 0, events.CancelledByDriver, ptr(100000), 0, skipDriverCancel},
		{"unknown canceller: not charged", 15000, "", ptr(100000), 0, skipUnknownActor},
		{"fee is capped at the price", 15000, events.CancelledByRider, ptr(10000), 10000, ""},
		{"free ride, nothing to cap to", 15000, events.CancelledByRider, ptr(0), 0, skipZeroAmount},
		{"price unknown: the configured fee", 15000, events.CancelledByRider, nil, 15000, ""},
	}
	for _, c := range cases {
		p := Policy{CommissionBps: 2000, CancellationFee: c.fee}
		got, skip := p.Cancellation(c.cancelledBy, c.price)
		if skip != c.wantSkip || got.Total != c.wantTotal {
			t.Errorf("%s: total %d, skip %q; want %d, %q", c.name, got.Total, skip, c.wantTotal, c.wantSkip)
		}
		if skip == "" {
			if got.Kind != events.PaymentKindCancellationFee || got.Driver+got.Platform != got.Total {
				t.Errorf("%s: charge %+v is not a balanced cancellation fee", c.name, got)
			}
		}
	}
}

func TestPolicyValidate(t *testing.T) {
	good := []Policy{{}, {CommissionBps: 2000, CancellationFee: 15000}, {CommissionBps: 10000}}
	for _, p := range good {
		if err := p.Validate(); err != nil {
			t.Errorf("%+v: %v", p, err)
		}
	}
	bad := []Policy{{CommissionBps: -1}, {CommissionBps: 10001}, {CancellationFee: -5}, {CancellationFee: maxAmount + 1}}
	for _, p := range bad {
		if err := p.Validate(); err == nil {
			t.Errorf("%+v: want an error", p)
		}
	}
}
