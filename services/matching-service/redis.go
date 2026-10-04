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
	results, err := rdb.GeoSearchLocation(ctx, driverlocation.GeoKey, &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude:  lng,
			Latitude:   lat,
			Radius:     radiusKm,
			RadiusUnit: "km",
			Sort:       "ASC",
			Count:      50, // several types share one GEO set, so look further than 20
		},
		WithDist: true,
	}).Result()
	if err != nil {
		return "", err
	}

	for _, candidate := range results {
		result, err := driverstatus.ClaimForOrder(ctx, rdb, orderID, candidate.Name, string(vehicle), lease)
		if err != nil {
			return "", err
		}
		switch result {
		case driverstatus.Claimed:
			return candidate.Name, nil
		case driverstatus.AlreadyMatched:
			return driverstatus.MatchedDriver(ctx, rdb, orderID)
		}
	}

	return "", nil
}
