// Package driverlocation holds the Redis representation of where
// drivers currently are: a GEO set for proximity search (used by
// Matching Service) plus a per-driver last-update timestamp used to
// discard out-of-order location updates (US-3.1).
package driverlocation

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// GeoKey is the Redis GEO set every driver's position is stored in.
// Matching Service reads it; Driver Location Service (and the seed
// script) write it, so both must agree on this key.
const GeoKey = "drivers:geo"

func timestampKey(driverID string) string {
	return fmt.Sprintf("driver:%s:location:ts", driverID)
}

// updateScript atomically applies a location update only if it's newer
// than the last one recorded for this driver, so a late-arriving event
// (delivered out of order) can never overwrite a fresher position.
var updateScript = redis.NewScript(`
	local storedTs = redis.call("GET", KEYS[2])
	if (not storedTs) or tonumber(ARGV[3]) > tonumber(storedTs) then
		redis.call("GEOADD", KEYS[1], ARGV[1], ARGV[2], ARGV[4])
		redis.call("SET", KEYS[2], ARGV[3])
		return 1
	end
	return 0
`)

// Update applies a driver's location update, keyed by unixMillis, and
// reports whether it was applied (false means a newer update was
// already recorded and this one was discarded).
func Update(ctx context.Context, rdb *redis.Client, driverID string, lat, lng float64, unixMillis int64) (bool, error) {
	applied, err := updateScript.Run(ctx, rdb,
		[]string{GeoKey, timestampKey(driverID)},
		lng, lat, unixMillis, driverID,
	).Int()
	if err != nil {
		return false, err
	}
	return applied == 1, nil
}

// ResetTimestamp clears a driver's recorded last-update time, e.g. when
// re-seeding test data, so the next Update is accepted unconditionally.
func ResetTimestamp(ctx context.Context, rdb *redis.Client, driverID string) error {
	return rdb.Del(ctx, timestampKey(driverID)).Err()
}
