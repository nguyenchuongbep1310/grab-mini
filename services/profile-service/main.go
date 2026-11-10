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
//
// This file is the composition root: it reads the environment, builds the adapters, hands
// them to the use cases in internal/app and runs the HTTP API and the outbox relay, or one of
// the subcommands (migrate, seed-dev, resync).
package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
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
	httpadapter "grab-mini/profile-service/internal/adapters/http"
	kafkaadapter "grab-mini/profile-service/internal/adapters/kafka"
	appmetrics "grab-mini/profile-service/internal/adapters/metrics"
	"grab-mini/profile-service/internal/adapters/postgres"
	"grab-mini/profile-service/internal/app"
	"grab-mini/profile-service/internal/domain"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const (
	serviceName  = "profile-service"
	outboxSource = "profile-service"
)

// The HTTP adapter panics on a refusal without a Refusals: the metrics adapter is it.
var _ httpadapter.Refusals = (*appmetrics.Metrics)(nil)

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
		case "resync":
			runResync(env, os.Args[2:])
			return
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "profile-service")()

	db, err := postgres.Connect(ctx, env.DatabaseURL, serviceName, migrationFiles)
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
	// Always a real relay: a nil *outbox.Relay put in app.Deps.Relay would be a non-nil
	// Waker (a typed nil).
	relay := outbox.NewRelay(db, producer, outboxSource)
	m := appmetrics.New()
	// Built here, before the relay starts and before the verifier, so a bad RATE_LIMIT_SCALE
	// stops the start at the same point as before the migration.
	limiter := httpadapter.NewPublicLimiter(env.PublicRatePerMinute)
	svc := newService(db, codec, relay, m)

	var background sync.WaitGroup
	background.Go(func() { relay.Run(ctx) })

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	mux := httpadapter.NewAPI(svc, verifier, limiter, m).Routes()
	health.Mount(ctx, mux, health.Postgres(db), health.Kafka(env.KafkaBrokers))

	log.Printf("profile-service listening on :%s (%d public lookups a minute per caller)", env.Port, env.PublicRatePerMinute)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("profile-service", metrics.HTTP(mux))); err != nil {
		log.Fatal(err)
	}
	background.Wait() // the relay drains before the pool closes
	log.Printf("profile-service stopped")
}

// newService wires the use cases over Postgres. relay is the running service's real relay,
// or an untyped nil for a command that only queues (never a nil *outbox.Relay: stored in the
// app.Waker it would be a non-nil interface).
func newService(db *pgxpool.Pool, codec kafkaadapter.Codec, relay app.Waker, m *appmetrics.Metrics) *app.Service {
	store := postgres.NewStore(db)
	return app.New(app.Deps{
		Riders:  store,
		Drivers: store,
		Queue:   store,
		Resync:  store,
		Locker:  postgres.NewResyncLocker(db),
		UoW:     postgres.NewUnitOfWork(db, outboxSource),
		Encoder: kafkaadapter.NewEncoder(codec),
		Relay:   relay,
		Metrics: m,
	})
}

// runMigrate is the `profile-service migrate` command: apply pending migrations and exit. A
// connection failure reads "failed to connect to database: ..." (postgres.Migrate wraps it
// so), as before.
func runMigrate(env Env) {
	if err := postgres.Migrate(context.Background(), env.DatabaseURL, serviceName, migrationFiles); err != nil {
		log.Fatal(err)
	}
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
	pool, err := postgres.Connect(ctx, env.DatabaseURL, serviceName, migrationFiles)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()
	drivers, riders, err := postgres.SeedDev(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s: %d demo driver profile(s) and %d rider profile(s) added", serviceName, drivers, riders)
}

// runResync is `profile-service resync [--driver id]`: the same resync as the admin
// endpoint, for an operator with database access and no admin token. It only queues the
// events; the running service's relay publishes them (they wait in the outbox if none runs).
// Unlike seed-dev it runs in every environment.
func runResync(env Env, args []string) {
	fs := flag.NewFlagSet("resync", flag.ExitOnError)
	driver := fs.String("driver", "", "resync only this driver (default: every APPROVED, REJECTED or SUSPENDED driver)")
	fs.Parse(args)
	if *driver != "" && !domain.ValidDriverID(*driver) {
		log.Fatal("resync: --driver is not a valid driver id")
	}

	ctx, cancel := context.WithTimeout(context.Background(), app.ResyncTimeout)
	defer cancel()
	pool, err := postgres.Connect(ctx, env.DatabaseURL, serviceName, migrationFiles)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()
	// No relay: Deps.Relay is an untyped nil, so the use case does not wake anything.
	s := newService(pool, eventcodec.New(env.SchemaRegistryURL), nil, appmetrics.New())
	res, err := s.Resync(ctx, *driver)
	switch {
	case errors.Is(err, app.ErrResyncRunning):
		log.Fatalf("%s: a resync is already running; wait for it and run this again", serviceName)
	case errors.Is(err, domain.ErrNoDriver):
		log.Fatalf("%s: no such driver", serviceName)
	case err != nil:
		log.Fatalf("%s: resync stopped after %d event(s) queued: %v", serviceName, res.Total, err)
	}
	fmt.Fprintf(os.Stdout, "%s: queued %d event(s): APPROVED %d, REJECTED %d, SUSPENDED %d (%d skipped)\n",
		serviceName, res.Total, res.Counts[domain.StatusApproved], res.Counts[domain.StatusRejected], res.Counts[domain.StatusSuspended], res.Skipped)
}
