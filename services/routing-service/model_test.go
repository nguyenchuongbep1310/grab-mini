package main

import (
	"errors"
	"math"
	"testing"
	"time"

	"grab-mini/pkg/events"
)

// at returns a moment whose local time in Vietnam (UTC+7) is hh:mm.
func at(hh, mm int) time.Time {
	return time.Date(2026, 3, 4, hh, mm, 0, 0, vietnam)
}

func TestSpeedByVehicleAndHour(t *testing.T) {
	for _, c := range []struct {
		name    string
		vehicle events.VehicleType
		at      time.Time
		want    float64
	}{
		{"car at midday", events.VehicleCar, at(12, 0), 28},
		{"car morning peak starts at 07:00", events.VehicleCar, at(7, 0), 16},
		{"car morning peak last minute", events.VehicleCar, at(8, 59), 16},
		{"car morning peak ends at 09:00", events.VehicleCar, at(9, 0), 28},
		{"car just before the morning peak", events.VehicleCar, at(6, 59), 28},
		{"car evening peak starts at 17:00", events.VehicleCar, at(17, 0), 16},
		{"car evening peak last minute", events.VehicleCar, at(18, 59), 16},
		{"car evening peak ends at 19:00", events.VehicleCar, at(19, 0), 28},
		{"car at night", events.VehicleCar, at(23, 30), 38},
		{"car just after midnight", events.VehicleCar, at(2, 0), 38},
		{"car night ends at 05:00", events.VehicleCar, at(5, 0), 28},
		{"bike at midday", events.VehicleBike, at(12, 0), 32},
		{"bike at peak", events.VehicleBike, at(8, 0), 22},
		{"bike at night", events.VehicleBike, at(22, 0), 40},
	} {
		if got := Speed(c.vehicle, c.at); got != c.want {
			t.Errorf("%s: Speed = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestPeakIsDecidedInVietnamTimeNotUTC(t *testing.T) {
	// 01:00 UTC is 08:00 in Vietnam: morning peak. 08:00 UTC is 15:00: not.
	if !isPeak(time.Date(2026, 3, 4, 1, 0, 0, 0, time.UTC)) {
		t.Error("01:00 UTC (08:00 in Ho Chi Minh City) should be peak")
	}
	if isPeak(time.Date(2026, 3, 4, 8, 0, 0, 0, time.UTC)) {
		t.Error("08:00 UTC (15:00 in Ho Chi Minh City) should not be peak")
	}
}

func TestEstimateTable(t *testing.T) {
	// Two points exactly 0.1 degree of latitude apart on one meridian: the
	// straight line is 0.1 x (pi/180) x 6371 km = 11.1195 km.
	from, to := Point{10.0, 106.0}, Point{10.1, 106.0}
	straight := 0.1 * math.Pi / 180 * 6371
	for _, c := range []struct {
		name         string
		vehicle      events.VehicleType
		at           time.Time
		wantKm       float64
		wantDuration int
	}{
		{"car off peak", events.VehicleCar, at(12, 0), round(straight*1.3, 2), int(math.Round(straight * 1.3 / 28 * 3600))},
		{"car at peak is slower", events.VehicleCar, at(17, 30), round(straight*1.3, 2), int(math.Round(straight * 1.3 / 16 * 3600))},
		{"bike off peak", events.VehicleBike, at(12, 0), round(straight*1.3, 2), int(math.Round(straight * 1.3 / 32 * 3600))},
		{"bike at peak", events.VehicleBike, at(7, 30), round(straight*1.3, 2), int(math.Round(straight * 1.3 / 22 * 3600))},
	} {
		got, err := Estimate(from, to, c.vehicle, c.at)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got.DistanceKm != c.wantKm || got.DurationSec != c.wantDuration {
			t.Errorf("%s: got %v km, %d s; want %v km, %d s", c.name, got.DistanceKm, got.DurationSec, c.wantKm, c.wantDuration)
		}
		if got.Vehicle != c.vehicle || got.Model != "detour-1.3" {
			t.Errorf("%s: vehicle %q model %q", c.name, got.Vehicle, got.Model)
		}
	}
}

func TestDetourIsAppliedToTheStraightLine(t *testing.T) {
	r, err := Estimate(Point{10.0, 106.0}, Point{10.1, 106.0}, events.VehicleCar, at(12, 0))
	if err != nil {
		t.Fatal(err)
	}
	// 11.12 km straight -> 14.46 km by road.
	if r.DistanceKm < 14.4 || r.DistanceKm > 14.5 {
		t.Errorf("DistanceKm = %v, want about 14.46 (11.12 x 1.3)", r.DistanceKm)
	}
}

func TestEstimateIsDeterministic(t *testing.T) {
	a, _ := Estimate(Point{10.776, 106.700}, Point{10.790, 106.710}, events.VehicleBike, at(8, 15))
	b, _ := Estimate(Point{10.776, 106.700}, Point{10.790, 106.710}, events.VehicleBike, at(8, 15))
	if a.DistanceKm != b.DistanceKm || a.DurationSec != b.DurationSec || len(a.Polyline) != len(b.Polyline) {
		t.Errorf("two runs differ: %+v vs %+v", a, b)
	}
}

func TestSamePointIsAZeroRoute(t *testing.T) {
	p := Point{10.776, 106.700}
	r, err := Estimate(p, p, events.VehicleCar, at(12, 0))
	if err != nil {
		t.Fatal(err)
	}
	if r.DistanceKm != 0 || r.DurationSec != 0 || len(r.Polyline) != 2 || r.Polyline[0] != p || r.Polyline[1] != p {
		t.Errorf("got %+v, want a zero-length route with a two-point polyline", r)
	}
}

func TestPolylineRunsFromOriginToDestination(t *testing.T) {
	from, to := Point{10.70, 106.60}, Point{10.80, 106.70}
	r, err := Estimate(from, to, events.VehicleCar, at(12, 0))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(r.Polyline); n < 3 || n > maxSegments+1 {
		t.Fatalf("polyline has %d points, want 3 to %d", n, maxSegments+1)
	}
	if r.Polyline[0] != from || r.Polyline[len(r.Polyline)-1] != to {
		t.Errorf("polyline runs %v to %v, want %v to %v", r.Polyline[0], r.Polyline[len(r.Polyline)-1], from, to)
	}
	for i := 1; i < len(r.Polyline); i++ {
		if r.Polyline[i].Lat < r.Polyline[i-1].Lat || r.Polyline[i].Lng < r.Polyline[i-1].Lng {
			t.Errorf("polyline goes backwards at point %d: %v after %v", i, r.Polyline[i], r.Polyline[i-1])
		}
	}
}

func TestPolylineIsCapped(t *testing.T) {
	r, err := Estimate(Point{10.0, 106.0}, Point{11.0, 106.0}, events.VehicleCar, at(12, 0)) // ~111 km straight
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Polyline) != maxSegments+1 {
		t.Errorf("polyline has %d points, want %d", len(r.Polyline), maxSegments+1)
	}
}

func TestRoutesBeyondTheCapAreRefused(t *testing.T) {
	for _, c := range []struct {
		name     string
		from, to Point
		tooFar   bool
	}{
		{"Ho Chi Minh City to Hanoi", Point{10.776, 106.700}, Point{21.028, 105.854}, true},
		{"just over 200 km after the detour (~153.9 km straight)", Point{10.0, 106.0}, Point{11.384, 106.0}, true},
		{"just under 200 km after the detour (~152 km straight)", Point{10.0, 106.0}, Point{11.367, 106.0}, false},
	} {
		_, err := Estimate(c.from, c.to, events.VehicleCar, at(12, 0))
		if got := errors.Is(err, errTooFar); got != c.tooFar {
			t.Errorf("%s: too far = %v (%v), want %v", c.name, got, err, c.tooFar)
		}
	}
}
