package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/health"
	"grab-mini/pkg/kafkaconsumer"
	"grab-mini/pkg/logging"
)

var locationUpdates = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "location_updates_total",
	Help: "Location updates consumed, by outcome: applied, or out_of_order (older than what is stored).",
}, []string{"result"})

// runConsumer implements task 3.1/3.2: consume driver.location.updated
// in its own consumer group and keep only the latest position per
// driver, discarding updates older than what's already recorded
// (US-3.1).
func runConsumer(ctx context.Context, env Env) {
	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr})
	defer rdb.Close()

	codec := eventcodec.New(env.SchemaRegistryURL)

	go health.Serve(ctx, env.HealthPort, health.Redis(rdb), health.Kafka(env.KafkaBrokers))

	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:  env.KafkaBrokers,
		GroupID:  "driver-location-service-group",
		Topic:    events.TopicDriverLocationUpdate,
		LogLabel: "[driver-location-service]",
	}, func(ctx context.Context, msg kafka.Message) error {
		var evt events.DriverLocationUpdated
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}

		ts, err := time.Parse(time.RFC3339, evt.Timestamp)
		if err != nil {
			return err
		}

		applied, err := driverlocation.Update(ctx, rdb, evt.DriverID, evt.Lat, evt.Lng, ts.UnixMilli())
		if err != nil {
			return err
		}

		if applied {
			locationUpdates.WithLabelValues("applied").Inc()
			log.Printf("driver=%s lat=%.5f lng=%.5f applied", evt.DriverID, evt.Lat, evt.Lng)
		} else {
			locationUpdates.WithLabelValues("out_of_order").Inc()
			log.Printf("driver=%s discarded as out-of-order", evt.DriverID)
		}
		return nil
	})
}

func main() {
	logging.Setup("driver-location-service")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runConsumer(ctx, loadEnv())
}
