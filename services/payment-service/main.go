// Command payment-service charges rides and keeps the ledger. This file is the
// composition root: it reads the environment, builds the adapters, wires them
// into the use cases and starts them. The rules live in internal/domain, the use
// cases in internal/app, and everything that touches Postgres, Kafka, HTTP or
// the payment provider in internal/adapters (see docs/adr/0014).
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

	httpadapter "grab-mini/payment-service/internal/adapters/http"
	kafkaadapter "grab-mini/payment-service/internal/adapters/kafka"
	paymetrics "grab-mini/payment-service/internal/adapters/metrics"
	"grab-mini/payment-service/internal/adapters/postgres"
	"grab-mini/payment-service/internal/adapters/provider"
	"grab-mini/payment-service/internal/app"
	"grab-mini/pkg/auth"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/health"
	"grab-mini/pkg/kafkaconn"
	"grab-mini/pkg/logging"
	"grab-mini/pkg/metrics"
	"grab-mini/pkg/outbox"
	"grab-mini/pkg/serve"
	"grab-mini/pkg/tracing"
)

// The SQL migrations stay next to main, where scripts and the Dockerfile expect
// them, and are embedded in the binary.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

func main() {
	logging.Setup("payment-service")
	env := loadEnv()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		postgres.Migrate(context.Background(), env.DatabaseURL, migrationFiles)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "payment-service")()

	psp, err := provider.FromEnv(env.Provider)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("payment provider: %s; commission %d bps; cancellation fee %d VND after a %s grace period",
		psp.Name(), env.Policy.CommissionBps, env.Policy.CancellationFee, env.Policy.CancellationGrace)

	db, err := postgres.Connect(ctx, env.DatabaseURL, migrationFiles)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

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
		Orders:      store,
		Assignments: store,
		Payments:    store,
		UnitOfWork:  postgres.NewUnitOfWork(db, relay),
		Encoder:     kafkaadapter.NewEncoder(codec),
		Reader:      store,
		Provider:    psp,
		Policy:      env.Policy,
		Metrics:     paymetrics.Prometheus{},
	})

	var background sync.WaitGroup
	background.Go(func() { relay.Run(ctx) })
	background.Go(func() {
		kafkaadapter.NewHandler(svc).Run(ctx, kafkaadapter.Config{Brokers: env.KafkaBrokers}, codec)
	})

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	mux := httpadapter.New(svc).Routes(verifier)
	health.Mount(ctx, mux, health.Postgres(db), health.Kafka(env.KafkaBrokers))

	log.Printf("payment-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("payment-service", metrics.HTTP(mux))); err != nil {
		log.Fatal(err)
	}
	background.Wait() // the consumer stops and the relay drains before the pool closes
	log.Printf("payment-service stopped")
}
