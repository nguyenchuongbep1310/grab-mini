// Package surge holds the pure parts of surge pricing, shared by Pricing
// Service (which computes multipliers) and Order Service (which honours and
// re-checks them at booking): the cell a point falls in, the multiplier
// formula, smoothing, the base fare, and signed quotes.
//
// Nothing here touches Redis or Kafka, so every function is a table test.
package surge

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

// DefaultCellDegrees is the cell size when neither SURGE_CELL_DEGREES nor
// GEO_SHARD_DEGREES is set (0.05 degrees is about 5.5 km).
const DefaultCellDegrees = 0.05

// MaxMultiplier is the default ceiling on a multiplier.
const MaxMultiplier = 3.0

// CellDegrees is the cell size in degrees. It follows the driver GEO shard
// size (GEO_SHARD_DEGREES, see package driverlocation) so a surge cell lines
// up with the keys the drivers of that area live under; SURGE_CELL_DEGREES
// overrides it, and with neither set the default applies.
func CellDegrees() float64 {
	for _, name := range []string{"SURGE_CELL_DEGREES", "GEO_SHARD_DEGREES"} {
		if v, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil && v >= 0.001 {
			return v
		}
	}
	return DefaultCellDegrees
}

// CellID names the cell containing (lat, lng): the same floor(lat/deg),
// floor(lng/deg) pair driverlocation uses for its GEO shard keys, so the two
// agree about where a cell is. deg <= 0 means DefaultCellDegrees.
func CellID(deg, lat, lng float64) string {
	if deg <= 0 {
		deg = DefaultCellDegrees
	}
	return fmt.Sprintf("%d:%d", int(math.Floor(lat/deg)), int(math.Floor(lng/deg)))
}

// CellCenter is the middle of a cell named by CellID.
func CellCenter(deg float64, cell string) (lat, lng float64, err error) {
	if deg <= 0 {
		deg = DefaultCellDegrees
	}
	i, j, ok := strings.Cut(cell, ":")
	if !ok {
		return 0, 0, fmt.Errorf("surge: bad cell id %q", cell)
	}
	ii, err1 := strconv.Atoi(i)
	jj, err2 := strconv.Atoi(j)
	if err1 != nil || err2 != nil {
		return 0, 0, fmt.Errorf("surge: bad cell id %q", cell)
	}
	return (float64(ii) + 0.5) * deg, (float64(jj) + 0.5) * deg, nil
}

// Round1 rounds to one decimal place, the precision multipliers are shown at.
func Round1(x float64) float64 { return math.Round(x*10) / 10 }

// Multiplier is the surge multiplier for a cell:
//
//	m = clamp(1 + k * (demandPerMin / max(supply, 1) - 1), 1.0, max)
//
// rounded to 0.1. Demand equal to supply gives 1.0; each extra unit of
// demand per driver adds k. Never below 1.0: riders are not offered discounts.
func Multiplier(demandPerMin, supply, k, max float64) float64 {
	if demandPerMin < 0 {
		demandPerMin = 0
	}
	ratio := demandPerMin / math.Max(supply, 1)
	return Round1(clamp(1+k*(ratio-1), 1.0, max))
}

// Smooth moves the previous multiplier toward target: an exponential moving
// average (alpha in (0,1], 1 meaning no smoothing), with the move per tick
// capped at maxStep so a burst of orders cannot double a price between two
// samples. The result is clamped to [1, max] and rounded to 0.1.
func Smooth(prev, target, alpha, maxStep, max float64) float64 {
	if prev < 1 {
		prev = 1
	}
	next := prev + alpha*(target-prev)
	if maxStep > 0 {
		next = clamp(next, prev-maxStep, prev+maxStep)
	}
	return Round1(clamp(next, 1.0, max))
}

func clamp(x, lo, hi float64) float64 { return math.Min(math.Max(x, lo), hi) }

// HaversineKm is the great-circle distance between two points.
func HaversineKm(lat1, lng1, lat2, lng2 float64) float64 {
	const earthKm = 6371.0
	rad := math.Pi / 180
	dLat, dLng := (lat2-lat1)*rad, (lng2-lng1)*rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLng/2)*math.Sin(dLng/2)
	return earthKm * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

// Currency every price is in.
const Currency = "VND"

// BaseFare is the fare before surge, in VND: a flag fall plus a per-km rate
// on the straight-line distance, with a minimum, rounded to the nearest 500.
// A deliberately simple model; swap it for a real tariff behind this function.
func BaseFare(vehicle string, distanceKm float64) float64 {
	flag, perKm, min := 12000.0, 10000.0, 20000.0 // CAR
	if vehicle == "BIKE" {
		flag, perKm, min = 8000.0, 6000.0, 12000.0
	}
	fare := math.Max(flag+perKm*math.Max(distanceKm, 0), min)
	return math.Round(fare/500) * 500
}

// Price is baseFare times multiplier, rounded to the nearest 1000 VND.
func Price(baseFare, multiplier float64) float64 {
	return math.Round(baseFare*multiplier/1000) * 1000
}
