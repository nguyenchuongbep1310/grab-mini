package main

import (
	"fmt"
	"math"
	"time"

	"grab-mini/pkg/events"
	"grab-mini/pkg/surge"
)

// This file is the whole routing model. It is deterministic and uses no map
// data, no traffic feed and no external API: the same inputs at the same hour
// always give the same answer. It is an estimate for showing a rider "about N
// min" and for pricing, not a navigation engine.
//
//	distance = great-circle distance x DetourFactor
//	duration = distance / speed(vehicle, hour of day in Vietnam)
//	polyline = the straight line between the two points, in steps of about 1 km

// DetourFactor is how much longer a road trip is than the straight line. 1.3
// is a common rule of thumb for a dense city street grid.
const DetourFactor = 1.3

// Model is reported in every response, so a caller (and a log) can tell which
// version of the model produced a number if it ever changes.
var Model = fmt.Sprintf("detour-%.1f", DetourFactor)

// MaxRouteKm is the longest route the model answers for. Beyond it the
// straight-line-times-detour guess is meaningless (an intercity trip is not
// one street grid), and a coordinate typo (swapped lat/lng) is the likelier cause.
const MaxRouteKm = 200.0

// vietnam is UTC+7 all year; the country has had no daylight saving since 1975.
// A fixed zone avoids shipping the tz database in a distroless image.
var vietnam = time.FixedZone("Asia/Ho_Chi_Minh", 7*3600)

// speedKmh is the average speed on a street grid, by vehicle and period of the day.
var speedKmh = map[events.VehicleType]struct{ offPeak, peak, night float64 }{
	// A car loses the most in the rush hours.
	events.VehicleCar: {offPeak: 28, peak: 16, night: 38},
	// A motorbike filters through traffic: slower at peak too, but by less.
	events.VehicleBike: {offPeak: 32, peak: 22, night: 40},
}

// isPeak reports whether t falls in the morning (07:00 to 09:00) or evening
// (17:00 to 19:00) rush hour, in Vietnam local time.
func isPeak(t time.Time) bool {
	h := t.In(vietnam).Hour()
	return (h >= 7 && h < 9) || (h >= 17 && h < 19)
}

// isNight reports whether t is between 22:00 and 05:00 local time, when the roads are empty.
func isNight(t time.Time) bool {
	h := t.In(vietnam).Hour()
	return h >= 22 || h < 5
}

// Speed is the average speed in km/h for vehicle at time t.
func Speed(vehicle events.VehicleType, t time.Time) float64 {
	s := speedKmh[vehicle]
	switch {
	case isPeak(t):
		return s.peak
	case isNight(t):
		return s.night
	}
	return s.offPeak
}

// Point is a coordinate on a polyline.
type Point struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// Route is the model's answer.
type Route struct {
	DistanceKm  float64            `json:"distanceKm"`
	DurationSec int                `json:"durationSec"`
	Polyline    []Point            `json:"polyline"`
	Vehicle     events.VehicleType `json:"vehicle"`
	Model       string             `json:"model"`
}

// maxSegments bounds the polyline: a long route is not worth hundreds of points.
const maxSegments = 20

func round(x float64, places int) float64 {
	p := math.Pow10(places)
	return math.Round(x*p) / p
}

// Estimate computes the route from one point to another. It reports
// errTooFar when the modelled distance exceeds MaxRouteKm. Coordinates must
// already be validated.
func Estimate(from, to Point, vehicle events.VehicleType, at time.Time) (Route, error) {
	straightKm := surge.HaversineKm(from.Lat, from.Lng, to.Lat, to.Lng)
	km := straightKm * DetourFactor
	if km > MaxRouteKm {
		return Route{}, errTooFar
	}
	hours := km / Speed(vehicle, at)

	segments := min(max(int(math.Ceil(straightKm)), 1), maxSegments)
	line := make([]Point, 0, segments+1)
	for i := 0; i <= segments; i++ {
		f := float64(i) / float64(segments)
		line = append(line, Point{
			Lat: round(from.Lat+(to.Lat-from.Lat)*f, 6),
			Lng: round(from.Lng+(to.Lng-from.Lng)*f, 6),
		})
	}
	return Route{
		DistanceKm:  round(km, 2),
		DurationSec: int(math.Round(hours * 3600)),
		Polyline:    line,
		Vehicle:     vehicle,
		Model:       Model,
	}, nil
}
