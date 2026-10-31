// Command promotion-service keeps promotions (vouchers) and their redemptions.
//
// A rider checks a code with GET /promotions/validate and gets the discount and a
// signed promo token. Order Service verifies that token itself when the rider
// books (it never calls this service on the booking path) and locks the discount
// on the order. This service then learns of the order from order.created and
// records the redemption, releases it when the order ends without a ride
// (order.cancelled, a cancelled trip, order.no_driver_found), and announces it
// on promotion.redeemed through the outbox. Admins create and disable promotions.
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
	"grab-mini/pkg/promo"
	"grab-mini/pkg/secret"
	"grab-mini/pkg/serve"
	"grab-mini/pkg/tracing"
)

func main() {
	logging.Setup("promotion-service")
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
	defer tracing.Setup(ctx, "promotion-service")()

	signer, err := promo.SignerFromEnv()
	if err != nil {
		log.Fatal(err)
	}

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
	s := newService(db, signer, codec, relay)

	var background sync.WaitGroup
	background.Go(func() { relay.Run(ctx) })
	background.Go(func() { s.runConsumer(ctx, env, "", codec) })

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	mux := s.routes(verifier)
	health.Mount(ctx, mux, health.Postgres(db), health.Kafka(env.KafkaBrokers))

	log.Printf("promotion-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("promotion-service", metrics.HTTP(mux))); err != nil {
		log.Fatal(err)
	}
	background.Wait() // the consumer stops and the relay drains before the pool closes
	log.Printf("promotion-service stopped")
}

// runSeedDev is `promotion-service seed-dev`: a few promotions to try the
// feature with (scripts/dev.sh runs it, like matching-service seed-drivers).
// It refuses to run outside APP_ENV=development, so the demo codes never reach
// a real environment.
func runSeedDev(env Env) {
	if !secret.Development() {
		log.Fatal("seed-dev adds public demo promotion codes and runs only with APP_ENV=development")
	}
	ctx := context.Background()
	// Brings the schema up to date first: this runs before the service has started.
	pool, err := connectDB(ctx, env.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()
	n, err := seedDev(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s: %d demo promotion(s) added", serviceName, n)
}
