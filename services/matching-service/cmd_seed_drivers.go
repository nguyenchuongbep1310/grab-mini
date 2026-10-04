package main

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/events"
)

type seedDriver struct {
	ID      string
	Lat     float64
	Lng     float64
	Vehicle events.VehicleType
}

// Points scattered around central Ho Chi Minh City, for local testing.
var seedDrivers = []seedDriver{
	{"driver-1", 10.7769, 106.7009, events.VehicleCar},
	{"driver-2", 10.7797, 106.6990, events.VehicleCar},
	{"driver-3", 10.7720, 106.6980, events.VehicleCar},
	{"driver-4", 10.7830, 106.7050, events.VehicleBike},
	{"driver-5", 10.7700, 106.7100, events.VehicleBike},
}

func runSeedDrivers(env Env) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr})
	defer rdb.Close()

	if err := driverlocation.Clear(ctx, rdb); err != nil {
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
		if err := driverstatus.SetVehicle(ctx, rdb, d.ID, string(d.Vehicle)); err != nil {
			log.Fatalf("failed to set vehicle for %s: %v", d.ID, err)
		}
		if err := driverstatus.Set(ctx, rdb, d.ID, driverstatus.Available); err != nil {
			log.Fatalf("failed to set status for %s: %v", d.ID, err)
		}
	}

	log.Printf("Seeded %d AVAILABLE drivers into Redis (driver-1..3 CAR, driver-4..5 BIKE)", len(seedDrivers))
}
