package redis

import (
	"math"
	"testing"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
	"grab-mini/trip-service/internal/domain"
)

// The domain owns its copy of a few values that Redis and Matching define (it
// may not import services/pkg). This test keeps the copies equal to the shared
// kit.
func TestDomainValuesMatchTheSharedKit(t *testing.T) {
	for _, c := range []struct{ name, got, want string }{
		{"available", string(domain.DriverAvailable), string(driverstatus.Available)},
		{"assigned", string(domain.DriverAssigned), string(driverstatus.Assigned)},
		{"on trip", string(domain.DriverOnTrip), string(driverstatus.OnTrip)},
	} {
		if c.got != c.want {
			t.Errorf("%s: %q in the domain, %q in driverstatus", c.name, c.got, c.want)
		}
	}
}

func TestTheDomainDistanceIsTheOneMatchingUses(t *testing.T) {
	pts := [][4]float64{
		{10.776, 106.7, 10.776, 106.7},
		{10.776, 106.7, 10.79, 106.71},
		{10.1, 106.2, 10.9, 106.8},
		{-33.86, 151.2, 51.5, -0.12},
		{0, 0, 0, 180},
	}
	for _, p := range pts {
		got, want := domain.DistanceM(p[0], p[1], p[2], p[3]), driverlocation.DistanceM(p[0], p[1], p[2], p[3])
		if math.Abs(got-want) > 1e-6 {
			t.Errorf("DistanceM%v: domain %v, driverlocation %v", p, got, want)
		}
	}
}
