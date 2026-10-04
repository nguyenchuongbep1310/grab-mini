package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/health"
	"grab-mini/pkg/kafkaconsumer"
	"grab-mini/pkg/serve"
)

func runConsumer(ctx context.Context, env Env, db *pgxpool.Pool) {
	codec := eventcodec.New(env.SchemaRegistryURL)

	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:    env.KafkaBrokers,
		GroupID:    "rating-service-group",
		Topic:      events.TopicTripRated,
		LogLabel:   "[rating-service]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}, func(ctx context.Context, msg kafka.Message) error {
		var evt events.TripRated
		if err := codec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		if err := validate(evt); err != nil {
			return err
		}

		counted, err := record(ctx, db, evt)
		if err != nil {
			return err
		}
		if counted {
			log.Printf("driver=%s rated %d stars (trip %s)", evt.DriverID, evt.Stars, evt.TripID)
		} else {
			log.Printf("duplicate trip.rated event %s, skipping", evt.EventID)
		}
		return nil
	})
}

func main() {
	env := loadEnv()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		runMigrate(env)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := connectDB(ctx, env.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	var consumer sync.WaitGroup
	consumer.Go(func() { runConsumer(ctx, env, db) })

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	// A driver's average is shown to riders and to the driver, so any signed-in caller may read it.
	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("GET /drivers/{id}/rating", verifier.Require(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rating, err := getRating(r.Context(), db, r.PathValue("id"))
		if err != nil {
			log.Printf("failed to load rating: %v", err)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rating)
	}), auth.Rider, auth.Driver))

	health.Register(ctx, mux, health.Postgres(db), health.Kafka(env.KafkaBrokers))

	log.Printf("rating-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, mux); err != nil {
		log.Fatal(err)
	}
	consumer.Wait() // stops before the pool closes
	log.Printf("rating-service stopped")
}
