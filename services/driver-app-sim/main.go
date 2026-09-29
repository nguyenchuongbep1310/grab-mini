// Command driver-app-sim stands in for the real Driver App (built in a
// later phase): it publishes driver.location.updated every 3-5 seconds
// for a set of drivers, each doing a small random walk from a starting
// point (US-3.1).
package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
)

type simDriver struct {
	ID  string
	Lat float64
	Lng float64
}

// Matches the drivers matching-service seeds by default, so a
// simulator run without configuration lines up with a fresh seed.
var defaultDrivers = []simDriver{
	{"driver-1", 10.7769, 106.7009},
	{"driver-2", 10.7797, 106.6990},
	{"driver-3", 10.7720, 106.6980},
	{"driver-4", 10.7830, 106.7050},
	{"driver-5", 10.7700, 106.7100},
}

func loadDrivers() []simDriver {
	ids := os.Getenv("DRIVER_IDS")
	if ids == "" {
		return defaultDrivers
	}
	var drivers []simDriver
	for _, id := range strings.Split(ids, ",") {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		// Start near central Ho Chi Minh City if not one of the known seed drivers.
		lat, lng := 10.776, 106.700
		for _, d := range defaultDrivers {
			if d.ID == id {
				lat, lng = d.Lat, d.Lng
			}
		}
		drivers = append(drivers, simDriver{ID: id, Lat: lat, Lng: lng})
	}
	return drivers
}

func randomWalk(d *simDriver) {
	const stepDegrees = 0.0015 // roughly a short block per tick
	d.Lat += (rand.Float64()*2 - 1) * stepDegrees
	d.Lng += (rand.Float64()*2 - 1) * stepDegrees
}

func simulateDriver(ctx context.Context, wg *sync.WaitGroup, producer *kafka.Writer, codec *eventcodec.Codec, d simDriver) {
	defer wg.Done()

	for {
		randomWalk(&d)

		event := events.DriverLocationUpdated{
			Envelope:  events.Envelope{EventID: uuid.NewString(), OccurredAt: time.Now().UTC().Format(time.RFC3339)},
			DriverID:  d.ID,
			Lat:       d.Lat,
			Lng:       d.Lng,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		}
		payload, err := codec.Encode(ctx, events.TopicDriverLocationUpdate, event)
		if err == nil {
			err = producer.WriteMessages(ctx, kafka.Message{Key: []byte(d.ID), Value: payload})
		}
		if err != nil {
			log.Printf("driver=%s failed to publish location: %v", d.ID, err)
		} else {
			log.Printf("driver=%s lat=%.5f lng=%.5f published", d.ID, d.Lat, d.Lng)
		}

		// US-3.1: every 3-5 seconds.
		delay := 3*time.Second + time.Duration(rand.Intn(2001))*time.Millisecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func main() {
	brokers := []string{getEnv("KAFKA_BROKERS", "localhost:9092")}
	drivers := loadDrivers()

	producer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  events.TopicDriverLocationUpdate,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	defer producer.Close()

	log.Printf("driver-app-sim publishing for %d driver(s): %v", len(drivers), driverIDs(drivers))

	codec := eventcodec.New(getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"))

	var wg sync.WaitGroup
	ctx := context.Background()
	for _, d := range drivers {
		wg.Add(1)
		go simulateDriver(ctx, &wg, producer, codec, d)
	}
	wg.Wait()
}

func driverIDs(drivers []simDriver) []string {
	ids := make([]string, len(drivers))
	for i, d := range drivers {
		ids[i] = d.ID
	}
	return ids
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
