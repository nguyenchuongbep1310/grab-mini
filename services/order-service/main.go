// Command order-service is the composition root: it reads the environment,
// builds the adapters, hands them to the use cases in internal/app and runs
// the HTTP API, the outbox relay, the outcome consumer and the key purge.
package main

import (
	"context"
	"embed"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"grab-mini/order-service/internal/adapters/http"
	kafkaadapter "grab-mini/order-service/internal/adapters/kafka"
	ordermetrics "grab-mini/order-service/internal/adapters/metrics"
	"grab-mini/order-service/internal/adapters/postgres"
	redisadapter "grab-mini/order-service/internal/adapters/redis"
	"grab-mini/order-service/internal/app"
	"grab-mini/pkg/auth"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/health"
	"grab-mini/pkg/kafkaconn"
	"grab-mini/pkg/logging"
	"grab-mini/pkg/metrics"
	"grab-mini/pkg/outbox"
	"grab-mini/pkg/promo"
	"grab-mini/pkg/redisconn"
	"grab-mini/pkg/serve"
	"grab-mini/pkg/surge"
	"grab-mini/pkg/tracing"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// outboxSource names this service in the shared outbox table.
const outboxSource = "order-service"

func main() {
	logging.Setup("order-service")
	env := loadEnv()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		// `order-service migrate`: apply pending migrations and exit.
		if err := postgres.Migrate(context.Background(), env.DatabaseURL, migrationFiles); err != nil {
			log.Fatal(err)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "order-service")()

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
	relay := outbox.NewRelay(db, producer, outboxSource)
	signer, err := surge.SignerFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	rdb := redisconn.MustNew(env.RedisAddr)
	defer rdb.Close()
	promoSigner, err := promo.SignerFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	m := ordermetrics.New()
	claims := redisadapter.NewClaims(rdb)
	store := postgres.NewStore(db)
	svc := app.New(app.Deps{
		Orders:   store,
		Keys:     store,
		Outcomes: store,
		UoW:      postgres.NewUnitOfWork(db, outboxSource),
		Encoder:  kafkaadapter.NewEncoder(codec),
		Receipt:  http.ReceiptEncoder(),
		Pricer: &app.Pricer{
			Signer: signer, Mult: redisadapter.NewMultipliers(rdb, m), Claims: claims,
			Tolerance: env.QuoteTolerance, CellDeg: surge.CellDegrees(), Now: time.Now, Metrics: m,
		},
		Promos:  &app.PromoChecker{Signer: promoSigner, Claims: claims, Now: time.Now, Metrics: m},
		Relay:   relay,
		Metrics: m,
		Now:     time.Now,
	})

	var background sync.WaitGroup
	background.Go(func() { relay.Run(ctx) })
	background.Go(func() { kafkaadapter.NewHandler(svc, codec).Run(ctx, env.KafkaBrokers) })
	background.Go(func() { runKeyPurge(ctx, svc, time.Hour) })

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewAPI(svc, verifier).Routes()
	health.Mount(ctx, mux, health.Postgres(db), health.Kafka(env.KafkaBrokers))

	log.Printf("order-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("order-service", metrics.HTTP(mux))); err != nil {
		log.Fatal(err)
	}
	background.Wait() // the consumer stops and the relay drains before the pool closes
	log.Printf("order-service stopped")
}

// purgeBatch is the most rows one PurgeExpiredKeys call deletes (the
// postgres store's LIMIT); a full batch means there may be more.
const purgeBatch = 5000

// runKeyPurge purges expired idempotency keys every interval until ctx ends.
// Replicas all run it; deleting the same rows twice is harmless.
func runKeyPurge(ctx context.Context, svc *app.Service, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		for {
			n, err := svc.PurgeExpiredKeys(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("purging expired idempotency keys: %v", err)
				}
				break
			}
			if n > 0 {
				log.Printf("purged %d expired idempotency key(s)", n)
			}
			if n < purgeBatch {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
