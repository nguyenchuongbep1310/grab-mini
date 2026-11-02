// Command trip-service owns the life of a trip: it creates it when a driver is
// matched, moves it through pickup, completion and cancellation, and records the
// rating. This file is the composition root: it reads the environment, builds
// the adapters, wires them into the use cases and starts them. The rules live in
// internal/domain, the use cases in internal/app, and everything that touches
// Postgres, Redis, Kafka or HTTP in internal/adapters (see docs/adr/0014).
package main

import (
	"context"
	"embed"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/health"
	"grab-mini/pkg/kafkaconn"
	"grab-mini/pkg/logging"
	"grab-mini/pkg/metrics"
	"grab-mini/pkg/outbox"
	"grab-mini/pkg/redisconn"
	"grab-mini/pkg/serve"
	"grab-mini/pkg/tracing"
	httpadapter "grab-mini/trip-service/internal/adapters/http"
	kafkaadapter "grab-mini/trip-service/internal/adapters/kafka"
	tripmetrics "grab-mini/trip-service/internal/adapters/metrics"
	"grab-mini/trip-service/internal/adapters/postgres"
	redisadapter "grab-mini/trip-service/internal/adapters/redis"
	"grab-mini/trip-service/internal/app"
)

// The SQL migrations stay next to main, where scripts and the Dockerfile expect
// them, and are embedded in the binary.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

func main() {
	logging.Setup("trip-service")
	env := loadEnv()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		postgres.Migrate(context.Background(), env.DatabaseURL, migrationFiles)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "trip-service")()

	db, err := postgres.Connect(ctx, env.DatabaseURL, migrationFiles)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	rdb := redisconn.MustNew(env.RedisAddr)
	defer rdb.Close()

	// The writer has no Topic: the outbox rows name theirs.
	producer := kafkaconn.Writer(&kafka.Writer{
		Addr:                   kafka.TCP(env.KafkaBrokers...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
	})
	defer producer.Close()

	codec := eventcodec.New(env.SchemaRegistryURL)
	relay := outbox.NewRelay(db, producer, postgres.OutboxSource)
	store := postgres.NewStore(db)
	svc := app.NewService(app.Deps{
		Trips:      store,
		Routes:     store,
		Orders:     store,
		Receipts:   store,
		UnitOfWork: postgres.NewUnitOfWork(db, relay),
		Drivers:    redisadapter.NewDrivers(rdb),
		Locations:  redisadapter.NewLocations(rdb),
		Encoder:    kafkaadapter.NewEncoder(codec),
		Metrics:    tripmetrics.Prometheus{},
		Proximity:  env.Proximity,
	})
	events := kafkaadapter.NewHandler(svc)

	var background sync.WaitGroup
	background.Go(func() { relay.Run(ctx) })
	background.Go(func() { events.RunOrderMatched(ctx, env.KafkaBrokers, codec) })
	background.Go(func() { events.RunDriverRelease(ctx, env.KafkaBrokers, codec) })
	background.Go(func() { events.RunOrderCancelled(ctx, env.KafkaBrokers, codec) })

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	mux := httpadapter.New(svc).Routes(verifier)
	health.Mount(ctx, mux, health.Postgres(db), health.Redis(rdb), health.Kafka(env.KafkaBrokers))

	log.Printf("trip-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("trip-service", metrics.HTTP(mux))); err != nil {
		log.Fatal(err)
	}
	background.Wait() // the consumer stops and the relay drains before the pool closes
	log.Printf("trip-service stopped")
}
