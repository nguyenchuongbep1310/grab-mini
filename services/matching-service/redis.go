package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const driverGeoKey = "drivers:geo"

type DriverStatus string

const (
	DriverAvailable DriverStatus = "AVAILABLE"
	DriverAssigned  DriverStatus = "ASSIGNED"
	DriverOnTrip    DriverStatus = "ON_TRIP"
)

func driverStatusKey(driverID string) string {
	return fmt.Sprintf("driver:%s:status", driverID)
}

func setDriverStatus(ctx context.Context, rdb *redis.Client, driverID string, status DriverStatus) error {
	return rdb.Set(ctx, driverStatusKey(driverID), string(status), 0).Err()
}

// claimDriverScript atomically moves a driver from AVAILABLE to the
// given status, so two matching-service instances racing on the same
// driver can never both succeed.
var claimDriverScript = redis.NewScript(`
	local current = redis.call("GET", KEYS[1])
	if current == ARGV[1] then
		redis.call("SET", KEYS[1], ARGV[2])
		return 1
	end
	return 0
`)

func claimDriver(ctx context.Context, rdb *redis.Client, driverID string, newStatus DriverStatus) (bool, error) {
	claimed, err := claimDriverScript.Run(ctx, rdb, []string{driverStatusKey(driverID)}, string(DriverAvailable), string(newStatus)).Int()
	if err != nil {
		return false, err
	}
	return claimed == 1, nil
}

// findAndClaimNearestDriver implements US-2.1: claims the nearest
// AVAILABLE driver within radiusKm, atomically, or returns "" if none
// could be claimed. Trying candidates in distance order and skipping
// ones another instance claims first keeps this race-free under
// concurrent matching-service instances.
func findAndClaimNearestDriver(ctx context.Context, rdb *redis.Client, lat, lng, radiusKm float64) (string, error) {
	results, err := rdb.GeoSearchLocation(ctx, driverGeoKey, &redis.GeoSearchLocationQuery{
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
		claimed, err := claimDriver(ctx, rdb, candidate.Name, DriverAssigned)
		if err != nil {
			return "", err
		}
		if claimed {
			return candidate.Name, nil
		}
	}

	return "", nil
}
