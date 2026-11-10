package domain

import (
	"testing"
	"time"
)

var (
	t0     = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	active = Promotion{
		Code: "WELCOME10", Kind: KindPercent, Value: 10, MaxDiscount: ptr[int64](20000), MinFare: 40000,
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
		{"disabled looks like it does not exist", disabled, 87000, "CAR", t0, Usage{}, CodeNotFound},
		{"before valid_from", active, 87000, "CAR", t0.Add(-2 * time.Hour), Usage{}, CodeNotStarted},
		{"after valid_to", active, 87000, "CAR", t0.Add(2 * time.Hour), Usage{}, CodeExpired},
		{"exactly at valid_to is expired", active, 87000, "CAR", active.ValidTo, Usage{}, CodeExpired},
		{"exactly at valid_from is valid", active, 87000, "CAR", active.ValidFrom, Usage{}, ""},
		{"wrong vehicle", carOnly, 87000, "BIKE", t0, Usage{}, CodeWrongVehicle},
		{"right vehicle", carOnly, 87000, "CAR", t0, Usage{}, ""},
		{"below the minimum fare", active, 39999, "CAR", t0, Usage{}, CodeMinFare},
		{"exactly the minimum fare", active, 40000, "CAR", t0, Usage{}, ""},
		{"global limit reached", limited, 87000, "CAR", t0, Usage{Total: 100}, CodeLimitReached},
		{"one below the global limit", limited, 87000, "CAR", t0, Usage{Total: 99}, ""},
		{"rider has used their share", active, 87000, "CAR", t0, Usage{Total: 5, ByRider: 1}, CodeAlreadyUsed},
		{"limit reached outranks already used", limited, 87000, "CAR", t0, Usage{Total: 100, ByRider: 1}, CodeLimitReached},
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
		if got := p.OverLimitReason(tc.u); got != tc.want {
			t.Errorf("%+v: reason = %q, want %q", tc.u, got, tc.want)
		}
	}
}
