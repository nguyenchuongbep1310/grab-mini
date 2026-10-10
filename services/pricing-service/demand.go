package main

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/events"
	"grab-mini/pkg/surge"
)

// cellsSetTTL bounds the set of active cells if nothing ever cleans it.
const cellsSetTTL = time.Hour

// recordDemand counts an order against its pickup cell for the current
// minute: surge:demand:{vehicle}:{cell}:{minute}, kept for 5 minutes. A
// redelivered order.created counts twice, which is noise at this scale.
func recordDemand(ctx context.Context, rdb *redis.Client, deg float64, order events.OrderCreated, now time.Time) error {
	vehicle := string(order.VehicleType)
	if !order.VehicleType.Valid() {
		vehicle = string(events.VehicleCar)
	}
	cell := surge.CellID(deg, order.Pickup.Lat, order.Pickup.Lng)
	key := surge.DemandKey(vehicle, cell, surge.Minute(now))

	pipe := rdb.TxPipeline()
	pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, surge.DemandTTL)
	pipe.SAdd(ctx, surge.CellsKey, surge.CellMember(vehicle, cell))
	pipe.Expire(ctx, surge.CellsKey, cellsSetTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// demandPerMin is the average number of orders per minute over the last
// window minute buckets (the current, partly elapsed minute included).
func demandPerMin(ctx context.Context, rdb *redis.Client, vehicle, cell string, window int, now time.Time) (float64, error) {
	if window < 1 {
		window = 1
	}
	keys := make([]string, window)
	m := surge.Minute(now)
	for i := range keys {
		keys[i] = surge.DemandKey(vehicle, cell, m-int64(i))
	}
	vals, err := rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return 0, err
	}
	var total float64
	for _, v := range vals {
		s, ok := v.(string)
		if !ok {
			continue
		}
		if n, err := strconv.ParseFloat(s, 64); err == nil {
			total += n
		}
	}
	return total / float64(window), nil
}
