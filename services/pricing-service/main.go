// Command pricing-service computes surge pricing.
//
// It watches demand (order.created, counted per pickup cell per minute) and
// supply (AVAILABLE drivers per cell, sampled every few seconds), keeps a
// short-lived multiplier per vehicle type and cell in Redis, and quotes
// prices to riders: GET /quotes returns a signed, stateless quote that Order
// Service honours at booking. It has no database, and if Redis is unavailable
// every quote is simply priced at 1.0 (see currentMultiplier).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/health"
	"grab-mini/pkg/kafkaconsumer"
	"grab-mini/pkg/logging"
	"grab-mini/pkg/metrics"
	"grab-mini/pkg/redisconn"
	"grab-mini/pkg/serve"
	"grab-mini/pkg/surge"
	"grab-mini/pkg/tracing"
)

func main() {
	logging.Setup("pricing-service")
	env := loadEnv()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "pricing-service")()

	rdb := redisconn.MustNew(env.RedisAddr)
	defer rdb.Close()

	signer, err := surge.SignerFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	codec := eventcodec.New(env.SchemaRegistryURL)

	var background sync.WaitGroup
	background.Go(func() {
		kafkaconsumer.Run(ctx, kafkaconsumer.Config{
			Brokers:        env.KafkaBrokers,
			GroupID:        "pricing-service-group",
			Topic:          events.TopicOrderCreated,
			LogLabel:       "[" + env.InstanceID + "]",
			RetryTiers:     kafkaconsumer.DefaultRetryTiers,
			CommitInterval: 100 * time.Millisecond,
		}, func(ctx context.Context, msg kafka.Message) error {
			var order events.OrderCreated
			if err := codec.Decode(ctx, msg.Topic, msg.Value, &order); err != nil {
				return err
			}
			return recordDemand(ctx, rdb, env.CellDegrees, order, time.Now())
		})
	})
	background.Go(func() {
		(&sampler{rdb: rdb, env: env, supply: redisSupply(rdb, env.CellDegrees), now: time.Now}).run(ctx)
	})

	s := &quoteServer{rdb: rdb, signer: signer, env: env, now: time.Now}
	mux := s.routes(verifier)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	health.Mount(ctx, mux, health.Redis(rdb), health.Kafka(env.KafkaBrokers))

	log.Printf("pricing-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("pricing-service", metrics.HTTP(mux))); err != nil {
		log.Fatal(err)
	}
	background.Wait()
	log.Printf("pricing-service stopped")
}
