// Package driverlocation holds the Redis representation of where
// drivers currently are: a GEO set for proximity search (used by
// Matching Service) plus a per-driver last-update timestamp used to
// discard out-of-order location updates (US-3.1).
package driverlocation

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// GeoKey is the Redis GEO set every driver's position is stored in.
// Matching Service reads it; Driver Location Service (and the seed
// script) write it, so both must agree on this key.
const GeoKey = "drivers:geo"

func timestampKey(driverID string) string {
	return fmt.Sprintf("driver:%s:location:ts", driverID)
}

// One GEOSEARCH costs in proportion to the drivers inside the radius, and a
// single key lives on one Redis core. GEO_SHARD_DEGREES splits the set into
// cells of that many degrees (0.05 is about 5.5 km) stored under separate
// keys, so the searches of a big city spread over many keys (and, with Redis
// Cluster, many nodes). Unset or 0 keeps the single GeoKey.
//
// The writers (Driver Location Service, the seed) and the reader (Matching)
// must run with the same value; changing it needs the GEO set rebuilt
// (drivers repopulate it within seconds as they report) because positions
// already stored stay under the old layout.
//
// The cell keys are built inside a script, so on Redis Cluster they need a
// hash tag chosen to spread them (this package doesn't do that yet).
const shardEnv = "GEO_SHARD_DEGREES"

// ShardDegrees is the cell size in degrees, 0 when sharding is off.
func ShardDegrees() float64 {
	v, err := strconv.ParseFloat(os.Getenv(shardEnv), 64)
	if err != nil || v < 0.001 {
		return 0
	}
	return v
}

// cellKey is the GEO key holding the drivers in the cell containing (lat, lng).
func cellKey(deg, lat, lng float64) string {
	if deg == 0 {
		return GeoKey
	}
	return fmt.Sprintf("%s:%d:%d", GeoKey, int(math.Floor(lat/deg)), int(math.Floor(lng/deg)))
}

func cellPointerKey(driverID string) string {
	return fmt.Sprintf("driver:%s:location:cell", driverID)
}

// updateScript atomically applies a location update only if it's newer
// than the last one recorded for this driver, so a late-arriving event
// (delivered out of order) can never overwrite a fresher position. When the
// driver moved into another cell it is removed from the cell it was in.
var updateScript = redis.NewScript(`
	local storedTs = redis.call("GET", KEYS[2])
	if (not storedTs) or tonumber(ARGV[3]) > tonumber(storedTs) then
		local old = redis.call("GET", KEYS[3])
		if old and old ~= KEYS[1] then
			redis.call("ZREM", old, ARGV[4])
		end
		redis.call("GEOADD", KEYS[1], ARGV[1], ARGV[2], ARGV[4])
		redis.call("SET", KEYS[2], ARGV[3])
		redis.call("SET", KEYS[3], KEYS[1])
		return 1
	end
	return 0
`)

// Update applies a driver's location update, keyed by unixMillis, and
// reports whether it was applied (false means a newer update was
// already recorded and this one was discarded).
func Update(ctx context.Context, rdb *redis.Client, driverID string, lat, lng float64, unixMillis int64) (bool, error) {
	applied, err := updateScript.Run(ctx, rdb,
		[]string{cellKey(ShardDegrees(), lat, lng), timestampKey(driverID), cellPointerKey(driverID)},
		lng, lat, unixMillis, driverID,
	).Int()
	if err != nil {
		return false, err
	}
	return applied == 1, nil
}

// Candidate is a driver found by Search.
type Candidate struct {
	ID     string
	DistKm float64
}

// maxCells bounds how many cell keys one search touches, so a huge radius
// with tiny cells can't turn into thousands of commands. A search that would
// need more fails instead of silently skipping cells (and so drivers).
const maxCells = 64

// Search returns up to count drivers within radiusKm of (lat, lng), nearest
// first. With sharding on it searches the cells the circle overlaps and
// merges the results.
func Search(ctx context.Context, rdb *redis.Client, lat, lng, radiusKm float64, count int) ([]Candidate, error) {
	keys := searchKeys(ShardDegrees(), lat, lng, radiusKm)
	if len(keys) > maxCells {
		return nil, fmt.Errorf("driverlocation: a %g km radius spans %d cells (max %d): raise %s or lower the radius", radiusKm, len(keys), maxCells, shardEnv)
	}
	pipe := rdb.Pipeline()
	cmds := make([]*redis.GeoSearchLocationCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.GeoSearchLocation(ctx, key, &redis.GeoSearchLocationQuery{
			GeoSearchQuery: redis.GeoSearchQuery{
				Longitude: lng, Latitude: lat, Radius: radiusKm, RadiusUnit: "km",
				Sort: "ASC", Count: count,
			},
			WithDist: true,
		})
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	var out []Candidate
	for _, cmd := range cmds {
		res, err := cmd.Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		for _, r := range res {
			out = append(out, Candidate{ID: r.Name, DistKm: r.Dist})
		}
	}
	if len(keys) > 1 {
		sort.Slice(out, func(i, j int) bool { return out[i].DistKm < out[j].DistKm })
	}
	if len(out) > count {
		out = out[:count]
	}
	return out, nil
}

// searchKeys lists the GEO keys a circle overlaps: every cell touching the
// circle's bounding box (a few extra cells are harmless, an empty or missing
// key just returns nothing). It stops listing once past maxCells.
func searchKeys(deg, lat, lng, radiusKm float64) []string {
	if deg == 0 {
		return []string{GeoKey}
	}
	const kmPerDegree = 111.32
	dLat := radiusKm / kmPerDegree
	dLng := radiusKm / (kmPerDegree * math.Max(math.Cos(lat*math.Pi/180), 0.01))
	i0, i1 := int(math.Floor((lat-dLat)/deg)), int(math.Floor((lat+dLat)/deg))
	j0, j1 := int(math.Floor((lng-dLng)/deg)), int(math.Floor((lng+dLng)/deg))
	var keys []string
	for i := i0; i <= i1; i++ {
		for j := j0; j <= j1; j++ {
			if len(keys) > maxCells {
				return keys // already too many: Search reports it
			}
			keys = append(keys, fmt.Sprintf("%s:%d:%d", GeoKey, i, j))
		}
	}
	return keys
}

// Clear removes every driver position (all cells), e.g. when re-seeding.
func Clear(ctx context.Context, rdb *redis.Client) error {
	if err := rdb.Del(ctx, GeoKey).Err(); err != nil {
		return err
	}
	iter := rdb.Scan(ctx, 0, GeoKey+":*", 500).Iterator()
	for iter.Next(ctx) {
		if err := rdb.Del(ctx, iter.Val()).Err(); err != nil {
			return err
		}
	}
	return iter.Err()
}

// Position is a driver's last known location and the (device) time of the
// update that set it.
type Position struct {
	Lat, Lng float64
	At       time.Time
}

// Age is how long ago the position was reported, by now's clock. The time is
// the driver device's, so a badly skewed phone clock skews this too.
func (p Position) Age(now time.Time) time.Duration { return now.Sub(p.At) }

// Get returns a driver's last known position, or found=false if none is
// recorded. It follows the driver's cell pointer so it works with and without
// GEO_SHARD_DEGREES.
func Get(ctx context.Context, rdb *redis.Client, driverID string) (pos Position, found bool, err error) {
	ms, err := rdb.Get(ctx, timestampKey(driverID)).Int64()
	if err == redis.Nil {
		return Position{}, false, nil
	}
	if err != nil {
		return Position{}, false, err
	}
	key, err := rdb.Get(ctx, cellPointerKey(driverID)).Result()
	if err == redis.Nil {
		key = GeoKey // written before cells existed
	} else if err != nil {
		return Position{}, false, err
	}
	res, err := rdb.GeoPos(ctx, key, driverID).Result()
	if err != nil {
		return Position{}, false, err
	}
	if len(res) == 0 || res[0] == nil {
		return Position{}, false, nil
	}
	return Position{Lat: res[0].Latitude, Lng: res[0].Longitude, At: time.UnixMilli(ms)}, true, nil
}

// DistanceM is the great-circle distance in metres between two points.
func DistanceM(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadiusM = 6371008.8
	rad := math.Pi / 180
	dLat, dLng := (lat2-lat1)*rad, (lng2-lng1)*rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLng/2)*math.Sin(dLng/2)
	return 2 * earthRadiusM * math.Asin(math.Min(1, math.Sqrt(a)))
}

// ResetTimestamp clears a driver's recorded last-update time, e.g. when
// re-seeding test data, so the next Update is accepted unconditionally.
func ResetTimestamp(ctx context.Context, rdb *redis.Client, driverID string) error {
	return rdb.Del(ctx, timestampKey(driverID), cellPointerKey(driverID)).Err()
}
