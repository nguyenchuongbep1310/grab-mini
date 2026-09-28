package main

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
)

type seedDriver struct {
	ID  string
	Lat float64
	Lng float64
}

// Points scattered around central Ho Chi Minh City, for local testing.
var seedDrivers = []seedDriver{
	{"driver-1", 10.7769, 106.7009},
	{"driver-2", 10.7797, 106.6990},
	{"driver-3", 10.7720, 106.6980},
	{"driver-4", 10.7830, 106.7050},
	{"driver-5", 10.7700, 106.7100},
}

func runSeedDrivers(env Env) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr})
	defer rdb.Close()

	if err := rdb.Del(ctx, driverlocation.GeoKey).Err(); err != nil {
		log.Fatalf("failed to clear driver geo set: %v", err)
	}
	for _, d := range seedDrivers {
		if err := driverlocation.ResetTimestamp(ctx, rdb, d.ID); err != nil {
			log.Fatalf("failed to clear location timestamp for %s: %v", d.ID, err)
		}
	}

	for _, d := range seedDrivers {
		// unixMillis 0 so any real location update from the simulator
		// (task 3.3) is treated as newer and takes over.
		if _, err := driverlocation.Update(ctx, rdb, d.ID, d.Lat, d.Lng, 0); err != nil {
			log.Fatalf("failed to seed driver %s: %v", d.ID, err)
		}
		if err := driverstatus.Set(ctx, rdb, d.ID, driverstatus.Available); err != nil {
			log.Fatalf("failed to set status for %s: %v", d.ID, err)
		}
	}

	log.Printf("Seeded %d AVAILABLE drivers into Redis", len(seedDrivers))
}
