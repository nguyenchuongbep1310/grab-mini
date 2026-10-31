// Command routing-service estimates the road distance and travel time between
// two points.
//
// It is stateless and has no Kafka, database or Redis, and calls no external
// API: the model (see model.go) is the great-circle distance times a detour
// factor, divided by a speed that depends on the vehicle and the hour of the
// day in Vietnam. That makes it cheap, deterministic and always available, and
// also only an estimate. Callers: the Rider App (distance and ETA before
// booking, "driver arrives in N min" after a match) and Pricing Service (the
// distance behind a quote, failing open to its own straight-line distance).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/health"
	"grab-mini/pkg/logging"
	"grab-mini/pkg/metrics"
	"grab-mini/pkg/serve"
	"grab-mini/pkg/tracing"
)

func main() {
	logging.Setup("routing-service")
	env := loadEnv()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "routing-service")()

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	s := &routeServer{now: time.Now}
	mux := s.routes(verifier)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	// Nothing to depend on, so ready whenever alive (and not draining).
	health.Mount(ctx, mux)

	log.Printf("routing-service listening on :%s (instance %s, model %s)", env.Port, env.InstanceID, Model)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("routing-service", metrics.HTTP(mux))); err != nil {
		log.Fatal(err)
	}
	log.Printf("routing-service stopped")
}
