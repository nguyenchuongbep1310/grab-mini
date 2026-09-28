// Package driverstatus holds the Redis key format and operations for a
// driver's status, shared by every service that reads or moves a
// driver between AVAILABLE / ASSIGNED / ON_TRIP.
package driverstatus

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Status string

const (
	Available Status = "AVAILABLE"
	Assigned  Status = "ASSIGNED"
	OnTrip    Status = "ON_TRIP"
)

func key(driverID string) string {
	return fmt.Sprintf("driver:%s:status", driverID)
}

func Set(ctx context.Context, rdb *redis.Client, driverID string, status Status) error {
	return rdb.Set(ctx, key(driverID), string(status), 0).Err()
}

func Get(ctx context.Context, rdb *redis.Client, driverID string) (Status, error) {
	v, err := rdb.Get(ctx, key(driverID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return Status(v), nil
}

// claimScript atomically moves a driver from expectedCurrent to
// newStatus, so concurrent callers (e.g. two Matching Service
// instances) can never both succeed for the same driver.
var claimScript = redis.NewScript(`
	local current = redis.call("GET", KEYS[1])
	if current == ARGV[1] then
		redis.call("SET", KEYS[1], ARGV[2])
		return 1
	end
	return 0
`)

// Claim atomically moves a driver from expectedCurrent to newStatus.
// It reports whether this call performed the transition.
func Claim(ctx context.Context, rdb *redis.Client, driverID string, expectedCurrent, newStatus Status) (bool, error) {
	claimed, err := claimScript.Run(ctx, rdb, []string{key(driverID)}, string(expectedCurrent), string(newStatus)).Int()
	if err != nil {
		return false, err
	}
	return claimed == 1, nil
}
