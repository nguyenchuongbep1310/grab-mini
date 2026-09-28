package main

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
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

	if err := rdb.Del(ctx, driverGeoKey).Err(); err != nil {
		log.Fatalf("failed to clear driver geo set: %v", err)
	}

	for _, d := range seedDrivers {
		if err := rdb.GeoAdd(ctx, driverGeoKey, &redis.GeoLocation{Name: d.ID, Longitude: d.Lng, Latitude: d.Lat}).Err(); err != nil {
			log.Fatalf("failed to seed driver %s: %v", d.ID, err)
		}
		if err := setDriverStatus(ctx, rdb, d.ID, DriverAvailable); err != nil {
			log.Fatalf("failed to set status for %s: %v", d.ID, err)
		}
	}

	log.Printf("Seeded %d AVAILABLE drivers into Redis", len(seedDrivers))
}
