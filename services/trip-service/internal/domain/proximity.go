package domain

import (
	"fmt"
	"math"
	"time"
)

// Proximity: a driver can only confirm pickup near the pickup point and
// complete near the dropoff, going by the last location they reported (the
// same Redis data Matching uses). Without it a driver could "complete" trips
// from the couch.

const (
	DefaultPickupRadiusM  = 200
	DefaultDropoffRadiusM = 300
	DefaultMaxLocationAge = 60 * time.Second
)

// Step names the place a driver has to be near.
type Step string

const (
	StepPickup  Step = "pickup"
	StepDropoff Step = "dropoff"
)

// Proximity is the configuration of the check.
type Proximity struct {
	PickupRadiusM  float64
	DropoffRadiusM float64
	// A last known location older than this is refused as stale.
	MaxLocationAge time.Duration
	// Off skips the check. Only a development stack may turn it off (the
	// loader in main refuses it otherwise): the driver-app-sim random-walks
	// near its seed point and never drives to a rider.
	Off bool
}

func DefaultProximity() Proximity {
	return Proximity{PickupRadiusM: DefaultPickupRadiusM, DropoffRadiusM: DefaultDropoffRadiusM, MaxLocationAge: DefaultMaxLocationAge}
}

// LatLng is a point on the map.
type LatLng struct{ Lat, Lng float64 }

// Route is where a trip starts and ends; a nil end is unknown (trips created
// before the route was recorded).
type Route struct{ Pickup, Dropoff *LatLng }

// Target is the point and radius the driver must be within for step.
func (p Proximity) Target(step Step, r Route) (*LatLng, float64) {
	if step == StepDropoff {
		return r.Dropoff, p.DropoffRadiusM
	}
	return r.Pickup, p.PickupRadiusM
}

// Position is a driver's last reported location.
type Position struct {
	Lat, Lng float64
	At       time.Time
}

// Age is how long ago the position was reported, by now's clock. The time is
// the driver device's, so a badly skewed phone clock skews this too.
func (p Position) Age(now time.Time) time.Duration { return now.Sub(p.At) }

// Why a pickup/complete was refused (also the label of the refusal metric).
const (
	ReasonTooFar              = "too_far"
	ReasonLocationUnknown     = "location_unknown"
	ReasonLocationStale       = "location_stale"
	ReasonLocationUnavailable = "location_unavailable"
)

// Refusal is why a pickup/complete was refused. It is an error so a use case
// can return it; Code is the stable machine-readable reason clients see.
type Refusal struct {
	Step   Step
	Reason string
	Code   string
	Msg    string
	// Set for too_far.
	DistanceM, MaxM float64
}

func (r *Refusal) Error() string { return r.Msg }

// Unavailable is the refusal for when the driver's location could not be
// read at all. The check fails closed: Redis being down must not let
// everyone through, so the driver is told to retry.
func Unavailable(step Step, radiusM float64) *Refusal {
	return &Refusal{Step: step, Reason: ReasonLocationUnavailable, Code: "location_unavailable",
		Msg: "can't verify your location right now: try again", MaxM: radiusM}
}

// Judge decides whether the driver, last seen at pos (found says whether any
// position is recorded), is close enough to target (a point on the route that
// the caller has already checked is known). It returns nil to allow, or the
// refusal to send.
func (p Proximity) Judge(step Step, now time.Time, target LatLng, radiusM float64, pos Position, found bool) *Refusal {
	refuse := func(code, reason, msg string, dist float64) *Refusal {
		return &Refusal{Step: step, Reason: reason, Code: code, Msg: msg, DistanceM: dist, MaxM: radiusM}
	}
	if !found {
		return refuse("driver_location_unknown", ReasonLocationUnknown,
			"no location reported for this driver: keep the app open so your position is shared, then retry", 0)
	}
	if age := pos.Age(now); age > p.MaxLocationAge {
		return refuse("driver_location_stale", ReasonLocationStale,
			fmt.Sprintf("last reported location is %ds old (max %ds): wait for a fresh position, then retry", int(age.Seconds()), int(p.MaxLocationAge.Seconds())), 0)
	}
	if d := DistanceM(pos.Lat, pos.Lng, target.Lat, target.Lng); d > radiusM {
		return refuse("driver_too_far", ReasonTooFar,
			fmt.Sprintf("you are about %dm from the %s: get within %dm", int(d+0.5), step, int(radiusM)), d)
	}
	return nil
}

// DistanceM is the great-circle distance in metres between two points
// (haversine). It is the same formula as services/pkg/driverlocation.DistanceM,
// which Matching uses; internal/adapters/redis pins the two together.
func DistanceM(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadiusM = 6371008.8
	rad := math.Pi / 180
	dLat, dLng := (lat2-lat1)*rad, (lng2-lng1)*rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLng/2)*math.Sin(dLng/2)
	return 2 * earthRadiusM * math.Asin(math.Min(1, math.Sqrt(a)))
}
