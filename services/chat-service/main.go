// Command chat-service lets the rider and the driver of a trip exchange short
// text messages while they meet.
//
// It learns who is on a trip, and when the trip ended, from trip.status.changed.
// POST /trips/{id}/messages saves a message and announces it on chat.message.sent
// through the outbox, in one transaction; Notification Service pushes it to the
// other person. GET /trips/{id}/messages is the history, which is also how an app
// recovers what a push missed. A chat is open while the trip is on and for a short
// grace period after it ended, then read-only; messages are deleted after
// CHAT_RETENTION_DAYS.
//
// Messages are personal data: their text is never written to a log.
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
	"grab-mini/pkg/serve"
	"grab-mini/pkg/tracing"
)

func main() {
	logging.Setup("chat-service")
	env := loadEnv()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		runMigrate(env)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "chat-service")()

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
	background.Go(func() { s.runConsumer(ctx, env, "", codec) })
	background.Go(func() { runPrune(ctx, db, env.RetentionDays, pruneInterval) })

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	mux := s.routes(verifier)
	health.Mount(ctx, mux, health.Postgres(db), health.Kafka(env.KafkaBrokers))

	log.Printf("chat-service listening on :%s (grace %s, retention %d days, %d messages a minute per sender)", env.Port, env.Grace, env.RetentionDays, env.RatePerMinute)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("chat-service", metrics.HTTP(mux))); err != nil {
		log.Fatal(err)
	}
	background.Wait() // the consumer stops and the relay drains before the pool closes
	log.Printf("chat-service stopped")
}
