package main

import (
	"context"

	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
)

// findAndClaimNearestDriver implements US-2.1: claims the nearest
// AVAILABLE driver within radiusKm, atomically, or returns "" if none
// could be claimed. Trying candidates in distance order and skipping
// ones another instance claims first keeps this race-free under
// concurrent matching-service instances.
func findAndClaimNearestDriver(ctx context.Context, rdb *redis.Client, lat, lng, radiusKm float64) (string, error) {
	results, err := rdb.GeoSearchLocation(ctx, driverlocation.GeoKey, &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{
			Longitude:  lng,
			Latitude:   lat,
			Radius:     radiusKm,
			RadiusUnit: "km",
			Sort:       "ASC",
			Count:      20,
		},
		WithDist: true,
	}).Result()
	if err != nil {
		return "", err
	}

	for _, candidate := range results {
		claimed, err := driverstatus.Claim(ctx, rdb, candidate.Name, driverstatus.Available, driverstatus.Assigned)
		if err != nil {
			return "", err
		}
		if claimed {
			return candidate.Name, nil
		}
	}

	return "", nil
}
