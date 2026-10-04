package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/events"
)

// findAndClaimNearestDriver implements US-2.1: claims the nearest AVAILABLE
// driver of the requested vehicle type within radiusKm, atomically, or
// returns "" if none could be claimed. Trying candidates in distance order
// and skipping ones another instance claims first keeps this race-free
// under concurrent matching-service instances.
//
// The claim is recorded against orderID, so if the order was already matched
// (a redelivered order.created) the earlier driver is returned and nobody
// else is claimed. The driver is leased for lease; see driverstatus.ClaimForOrder.
func findAndClaimNearestDriver(ctx context.Context, rdb *redis.Client, orderID string, lat, lng, radiusKm float64, vehicle events.VehicleType, lease time.Duration) (string, error) {
	// A GEOSEARCH costs in proportion to the drivers inside the radius, not
	// to the drivers online (measured: ~11 ms for 31k candidates, 0.4 ms for
	// 1k), and most orders have a free driver close by. So search outward in
	// rings and stop at the first that yields a claim: the result is still
	// the nearest claimable driver within radiusKm.
	for _, r := range searchRings(radiusKm) {
		driver, err := claimWithin(ctx, rdb, orderID, lat, lng, r, vehicle, lease)
		if err != nil || driver != "" {
			return driver, err
		}
	}
	return "", nil
}

// searchRings are the radii to try, smallest first, ending at radiusKm.
func searchRings(radiusKm float64) []float64 {
	rings := []float64{}
	for _, r := range []float64{1, 3} {
		if r < radiusKm {
			rings = append(rings, r)
		}
	}
	return append(rings, radiusKm)
}

// claimWithin claims the nearest claimable driver within radiusKm, or returns "".
func claimWithin(ctx context.Context, rdb *redis.Client, orderID string, lat, lng, radiusKm float64, vehicle events.VehicleType, lease time.Duration) (string, error) {
	// Several vehicle types share the GEO set, so look further than 20.
	results, err := driverlocation.Search(ctx, rdb, lat, lng, radiusKm, 50)
	if err != nil {
		return "", err
	}

	names := make([]string, len(results))
	for i, r := range results {
		names[i] = r.ID
	}
	driver, result, err := driverstatus.ClaimFirstForOrder(ctx, rdb, orderID, names, string(vehicle), lease)
	if err != nil {
		return "", err
	}
	switch result {
	case driverstatus.Claimed:
		return driver, nil
	case driverstatus.AlreadyMatched:
		return driverstatus.MatchedDriver(ctx, rdb, orderID)
	}
	return "", nil
}
