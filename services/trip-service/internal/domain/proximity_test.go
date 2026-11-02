package domain

import (
	"math"
	"testing"
	"time"
)

var (
	pickupPoint = LatLng{Lat: 10.7760, Lng: 106.7000}
	t0          = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
)

func metresNorth(p LatLng, m float64) LatLng { return LatLng{Lat: p.Lat + m/111_195, Lng: p.Lng} }

func TestTheDefaults(t *testing.T) {
	p := DefaultProximity()
	if p.PickupRadiusM != 200 || p.DropoffRadiusM != 300 || p.MaxLocationAge != time.Minute || p.Off {
		t.Errorf("defaults are %+v, want 200m / 300m / 60s / on", p)
	}
}

func TestTargetPicksThePointAndRadiusOfTheStep(t *testing.T) {
	drop := LatLng{Lat: 1, Lng: 2}
	r := Route{Pickup: &pickupPoint, Dropoff: &drop}
	p := DefaultProximity()
	if pt, rad := p.Target(StepPickup, r); pt != &pickupPoint || rad != 200 {
		t.Errorf("pickup target = %v, %v", pt, rad)
	}
	if pt, rad := p.Target(StepDropoff, r); pt != &drop || rad != 300 {
		t.Errorf("dropoff target = %v, %v", pt, rad)
	}
	if pt, _ := p.Target(StepPickup, Route{}); pt != nil {
		t.Error("a trip with no recorded route has a target")
	}
}

func TestJudge(t *testing.T) {
	p := DefaultProximity()
	at := func(pt LatLng, age time.Duration) Position {
		return Position{Lat: pt.Lat, Lng: pt.Lng, At: t0.Add(-age)}
	}
	for _, c := range []struct {
		name   string
		pos    Position
		found  bool
		reason string // "" to allow
		code   string
	}{
		{"right there", at(pickupPoint, time.Second), true, "", ""},
		{"150 m away", at(metresNorth(pickupPoint, 150), time.Second), true, "", ""},
		{"230 m away is outside 200", at(metresNorth(pickupPoint, 230), time.Second), true, ReasonTooFar, "driver_too_far"},
		{"1.5 km away", at(metresNorth(pickupPoint, 1500), time.Second), true, ReasonTooFar, "driver_too_far"},
		{"no position at all", Position{}, false, ReasonLocationUnknown, "driver_location_unknown"},
		{"right there, but 5 minutes old", at(pickupPoint, 5*time.Minute), true, ReasonLocationStale, "driver_location_stale"},
		{"exactly the maximum age", at(pickupPoint, time.Minute), true, "", ""},
		{"just over the maximum age", at(pickupPoint, time.Minute+time.Second), true, ReasonLocationStale, "driver_location_stale"},
		// stale beats far: a position that old says nothing about where the driver is
		{"far and stale", at(metresNorth(pickupPoint, 5000), 5*time.Minute), true, ReasonLocationStale, "driver_location_stale"},
	} {
		got := p.Judge(StepPickup, t0, pickupPoint, 200, c.pos, c.found)
		switch {
		case c.reason == "" && got != nil:
			t.Errorf("%s: refused: %+v", c.name, got)
		case c.reason != "" && (got == nil || got.Reason != c.reason || got.Code != c.code || got.MaxM != 200 || got.Step != StepPickup):
			t.Errorf("%s: got %+v, want %s / %s", c.name, got, c.reason, c.code)
		}
	}
}

func TestTheRefusalCarriesTheDistanceOnlyWhenTooFar(t *testing.T) {
	p := DefaultProximity()
	got := p.Judge(StepDropoff, t0, pickupPoint, 300, Position{Lat: metresNorth(pickupPoint, 1500).Lat, Lng: pickupPoint.Lng, At: t0}, true)
	if got == nil || got.DistanceM < 1400 || got.DistanceM > 1600 || got.MaxM != 300 {
		t.Fatalf("got %+v, want about 1500 m of 300", got)
	}
	if got := p.Judge(StepDropoff, t0, pickupPoint, 300, Position{}, false); got.DistanceM != 0 {
		t.Errorf("an unknown location has a distance: %+v", got)
	}
}

func TestTheMessagesNameTheStepAndTheNumbers(t *testing.T) {
	p := Proximity{PickupRadiusM: 200, DropoffRadiusM: 300, MaxLocationAge: 15 * time.Second}
	far := p.Judge(StepPickup, t0, pickupPoint, 200, Position{Lat: metresNorth(pickupPoint, 1000).Lat, Lng: pickupPoint.Lng, At: t0}, true)
	if far.Msg != "you are about 1000m from the pickup: get within 200m" {
		t.Errorf("too far: %q", far.Msg)
	}
	stale := p.Judge(StepPickup, t0, pickupPoint, 200, Position{At: t0.Add(-90 * time.Second)}, true)
	if stale.Msg != "last reported location is 90s old (max 15s): wait for a fresh position, then retry" {
		t.Errorf("stale: %q", stale.Msg)
	}
	unknown := p.Judge(StepPickup, t0, pickupPoint, 200, Position{}, false)
	if unknown.Msg != "no location reported for this driver: keep the app open so your position is shared, then retry" {
		t.Errorf("unknown: %q", unknown.Msg)
	}
	un := Unavailable(StepDropoff, 300)
	if un.Reason != ReasonLocationUnavailable || un.Code != "location_unavailable" || un.Msg != "can't verify your location right now: try again" || un.MaxM != 300 || un.Step != StepDropoff {
		t.Errorf("unavailable = %+v", un)
	}
}

func TestDistanceM(t *testing.T) {
	if d := DistanceM(10.776, 106.7, 10.776, 106.7); d != 0 {
		t.Errorf("same point = %v", d)
	}
	// One degree of latitude is about 111.2 km.
	if d := DistanceM(10, 106, 11, 106); math.Abs(d-111_195) > 100 {
		t.Errorf("1 degree of latitude = %v m", d)
	}
	if a, b := DistanceM(10.1, 106.2, 10.9, 106.8), DistanceM(10.9, 106.8, 10.1, 106.2); math.Abs(a-b) > 1e-6 {
		t.Errorf("not symmetric: %v vs %v", a, b)
	}
}
