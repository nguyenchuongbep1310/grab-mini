package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/events"
)

// runConsumer implements task 3.1/3.2: consume driver.location.updated
// in its own consumer group and keep only the latest position per
// driver, discarding updates older than what's already recorded
// (US-3.1).
func runConsumer(env Env) {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr})
	defer rdb.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: env.KafkaBrokers,
		GroupID: "driver-location-service-group",
		Topic:   events.TopicDriverLocationUpdate,
	})
	defer reader.Close()

	log.Println("driver-location-service consumer running")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatalf("failed to fetch message: %v", err)
		}

		var evt events.DriverLocationUpdated
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("failed to unmarshal driver.location.updated: %v", err)
			continue
		}

		ts, err := time.Parse(time.RFC3339, evt.Timestamp)
		if err != nil {
			log.Printf("invalid timestamp %q for driver %s: %v", evt.Timestamp, evt.DriverID, err)
			continue
		}

		applied, err := driverlocation.Update(ctx, rdb, evt.DriverID, evt.Lat, evt.Lng, ts.UnixMilli())
		if err != nil {
			log.Printf("failed to update location for driver %s: %v", evt.DriverID, err)
			continue
		}

		if applied {
			log.Printf("partition=%d offset=%d driver=%s lat=%.5f lng=%.5f applied", msg.Partition, msg.Offset, evt.DriverID, evt.Lat, evt.Lng)
		} else {
			log.Printf("partition=%d offset=%d driver=%s discarded as out-of-order", msg.Partition, msg.Offset, evt.DriverID)
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("failed to commit offset: %v", err)
		}
	}
}

func main() {
	runConsumer(loadEnv())
}
