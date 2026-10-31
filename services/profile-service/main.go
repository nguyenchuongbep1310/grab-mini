// Command profile-service keeps who people are: rider profiles (name, phone) and driver
// profiles (name, phone, vehicle), and decides who may drive.
//
// A driver saves a profile and applies; an admin approves, rejects or suspends. Every
// decision is queued on driver.application.reviewed through the outbox, in the transaction
// that changes the status, and Matching Service makes the driver eligible for trips or takes
// them out. A rider's app shows the matched driver's name, vehicle and plate from the public
// view, which never has a phone number.
//
// Names, phones and plates are personal data: they are never written to a log (only ids).
// This service consumes no topic.
package main

import (
	"context"
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
	"grab-mini/pkg/secret"
	"grab-mini/pkg/serve"
	"grab-mini/pkg/tracing"
)

func main() {
	logging.Setup("profile-service")
	env := loadEnv()
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "migrate":
			runMigrate(env)
			return
		case "seed-dev":
			runSeedDev(env)
			return
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "profile-service")()

	db, err := connectDB(ctx, env.DatabaseURL)
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
	relay := outbox.NewRelay(db, producer, outboxSource)
	s := newService(db, codec, relay, env)

	var background sync.WaitGroup
	background.Go(func() { relay.Run(ctx) })

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	mux := s.routes(verifier)
	health.Mount(ctx, mux, health.Postgres(db), health.Kafka(env.KafkaBrokers))

	log.Printf("profile-service listening on :%s (%d public lookups a minute per caller)", env.Port, env.PublicRatePerMinute)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("profile-service", metrics.HTTP(mux))); err != nil {
		log.Fatal(err)
	}
	background.Wait() // the relay drains before the pool closes
	log.Printf("profile-service stopped")
}

// runSeedDev is `profile-service seed-dev`: approved profiles for the development
// drivers and a few riders (scripts/dev.sh runs it, like promotion-service seed-dev). It
// refuses to run outside APP_ENV=development, so demo people never reach a real environment.
func runSeedDev(env Env) {
	if !secret.Development() {
		log.Fatal("seed-dev adds demo profiles and approves five drivers, and runs only with APP_ENV=development")
	}
	ctx := context.Background()
	// Brings the schema up to date first: this runs before the service has started.
	pool, err := connectDB(ctx, env.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()
	drivers, riders, err := seedDev(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s: %d demo driver profile(s) and %d rider profile(s) added", serviceName, drivers, riders)
}
