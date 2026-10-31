package main

import (
	"testing"
	"time"
)

func TestDiscountMath(t *testing.T) {
	percent := func(value int64, cap *int64) Promotion {
		return Promotion{Kind: kindPercent, Value: value, MaxDiscount: cap}
	}
	fixed := func(value int64) Promotion { return Promotion{Kind: kindFixed, Value: value} }

	cases := []struct {
		name  string
		p     Promotion
		price int64
		want  int64
	}{
		{"10 percent", percent(10, nil), 87000, 8700},
		{"percent rounds down to whole dong", percent(10, nil), 12345, 1234},
		{"percent of a tiny price rounds to zero", percent(10, nil), 9, 0},
		{"percent capped by max_discount", percent(10, ptr[int64](20000)), 500000, 20000},
		{"percent under the cap is not raised to it", percent(10, ptr[int64](20000)), 100000, 10000},
		{"cap equal to the raw discount", percent(10, ptr[int64](10000)), 100000, 10000},
		{"100 percent is the whole price", percent(100, nil), 87000, 87000},
		{"100 percent with a cap", percent(100, ptr[int64](30000)), 87000, 30000},
		{"fixed", fixed(15000), 87000, 15000},
		{"fixed never more than the price", fixed(15000), 12000, 12000},
		{"fixed equal to the price is a free ride", fixed(15000), 15000, 15000},
		{"a cap above the price still cannot exceed it", percent(100, ptr[int64](1_000_000)), 50000, 50000},
		{"zero price", fixed(15000), 0, 0},
		{"negative price is never a negative discount", fixed(15000), -5, 0},
		{"unknown kind gives nothing", Promotion{Kind: "BOGO", Value: 5}, 87000, 0},
		{"a corrupt percentage above 100 is clamped", percent(250, nil), 10000, 10000},
		{"a negative value gives nothing", fixed(-5), 10000, 0},
		{"largest price does not overflow", percent(100, nil), maxAmount, maxAmount},
	}
	for _, tc := range cases {
		got := tc.p.Discount(tc.price)
		if got != tc.want {
			t.Errorf("%s: discount(%d) = %d, want %d", tc.name, tc.price, got, tc.want)
		}
		if got < 0 || got > max(tc.price, 0) {
			t.Errorf("%s: discount %d outside [0, price %d]", tc.name, got, tc.price)
		}
	}
}

// Whatever the promotion and the price, the discount is whole, non-negative,
// and leaves a final price of 0 or more.
func TestDiscountInvariantsOverARangeOfInputs(t *testing.T) {
	promos := []Promotion{
		{Kind: kindPercent, Value: 1}, {Kind: kindPercent, Value: 33}, {Kind: kindPercent, Value: 100},
		{Kind: kindPercent, Value: 50, MaxDiscount: ptr[int64](7777)},
		{Kind: kindFixed, Value: 1}, {Kind: kindFixed, Value: 15000}, {Kind: kindFixed, Value: maxAmount},
	}
	for _, p := range promos {
		for price := int64(0); price <= 400000; price += 977 {
			d := p.Discount(price)
			if d < 0 || price-d < 0 {
				t.Fatalf("%+v price %d: discount %d leaves %d", p, price, d, price-d)
			}
		}
	}
}

var (
	t0     = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	active = Promotion{
		Code: "WELCOME10", Kind: kindPercent, Value: 10, MaxDiscount: ptr[int64](20000), MinFare: 40000,
		ValidFrom: t0.Add(-time.Hour), ValidTo: t0.Add(time.Hour), PerRider: 1,
	}
)

func TestCheckAcceptsAnApplicableCode(t *testing.T) {
	if r := active.Check(87000, "CAR", t0, Usage{}); r != nil {
		t.Errorf("rejected: %v", r)
	}
}

func TestCheckReportsEachReasonWithItsCode(t *testing.T) {
	disabled := active
	disabled.DisabledAt = &t0
	carOnly := active
	carOnly.VehicleType = ptr("CAR")
	limited := active
	limited.UsageLimit = ptr[int64](100)

	cases := []struct {
		name    string
		p       Promotion
		price   int64
		vehicle string
		now     time.Time
		usage   Usage
		want    string
	}{
		{"disabled looks like it does not exist", disabled, 87000, "CAR", t0, Usage{}, codeNotFound},
		{"before valid_from", active, 87000, "CAR", t0.Add(-2 * time.Hour), Usage{}, codeNotStarted},
		{"after valid_to", active, 87000, "CAR", t0.Add(2 * time.Hour), Usage{}, codeExpired},
		{"exactly at valid_to is expired", active, 87000, "CAR", active.ValidTo, Usage{}, codeExpired},
		{"exactly at valid_from is valid", active, 87000, "CAR", active.ValidFrom, Usage{}, ""},
		{"wrong vehicle", carOnly, 87000, "BIKE", t0, Usage{}, codeWrongVehicle},
		{"right vehicle", carOnly, 87000, "CAR", t0, Usage{}, ""},
		{"below the minimum fare", active, 39999, "CAR", t0, Usage{}, codeMinFare},
		{"exactly the minimum fare", active, 40000, "CAR", t0, Usage{}, ""},
		{"global limit reached", limited, 87000, "CAR", t0, Usage{Total: 100}, codeLimitReached},
		{"one below the global limit", limited, 87000, "CAR", t0, Usage{Total: 99}, ""},
		{"rider has used their share", active, 87000, "CAR", t0, Usage{Total: 5, ByRider: 1}, codeAlreadyUsed},
		{"limit reached outranks already used", limited, 87000, "CAR", t0, Usage{Total: 100, ByRider: 1}, codeLimitReached},
	}
	for _, tc := range cases {
		r := tc.p.Check(tc.price, tc.vehicle, tc.now, tc.usage)
		got := ""
		if r != nil {
			got = r.Code
			if r.Message == "" {
				t.Errorf("%s: no message", tc.name)
			}
		}
		if got != tc.want {
			t.Errorf("%s: code = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestOverLimitReasonNamesTheLimitThatWasReached(t *testing.T) {
	p := active
	p.UsageLimit = ptr[int64](10)
	p.PerRider = 2
	cases := []struct {
		u    Usage
		want string
	}{
		{Usage{Total: 3, ByRider: 1}, ""},
		{Usage{Total: 3, ByRider: 2}, "per_rider_limit"},
		{Usage{Total: 10, ByRider: 0}, "usage_limit"},
		{Usage{Total: 10, ByRider: 2}, "usage_limit"},
	}
	for _, tc := range cases {
		if got := p.overLimitReason(tc.u); got != tc.want {
			t.Errorf("%+v: reason = %q, want %q", tc.u, got, tc.want)
		}
	}
}
