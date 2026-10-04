// Package health gives a service the two probes an orchestrator wants, which
// answer different questions:
//
//	/livez   is the process alive? Always 200 while it can answer at all.
//	         Failing it gets the process restarted, so it must not depend on
//	         anything outside the process: a database outage must not restart
//	         every service.
//	/readyz  can it do its job right now? 200 only while its dependencies
//	         answer. Failing it takes the instance out of rotation without
//	         restarting it. It also turns 503 the moment shutdown begins, so a
//	         load balancer stops sending traffic before the server stops.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/serve"
)

// CheckTimeout bounds each dependency check, so a hung dependency makes the
// probe fail rather than hang.
const CheckTimeout = 2 * time.Second

// Check is one dependency a service needs in order to be ready.
type Check struct {
	Name string
	Fn   func(ctx context.Context) error
}

func Postgres(pool *pgxpool.Pool) Check {
	return Check{"postgres", func(ctx context.Context) error { return pool.Ping(ctx) }}
}

func Redis(rdb *redis.Client) Check {
	return Check{"redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() }}
}

// Kafka is ready when a broker answers a metadata request.
func Kafka(brokers []string) Check {
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: CheckTimeout}
	return Check{"kafka", func(ctx context.Context) error {
		resp, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{}})
		if err != nil {
			return err
		}
		if len(resp.Brokers) == 0 {
			return errors.New("no brokers in the cluster metadata")
		}
		return nil
	}}
}

// Live is the /livez handler.
func Live() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
}

// Ready is the /readyz handler. ctx is the process context: once it is
// cancelled (shutdown has begun) the handler answers 503.
func Ready(ctx context.Context, checks ...Check) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ctx.Err() != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "shutting down"})
			return
		}
		results := run(r.Context(), checks)

		status, code := "ok", http.StatusOK
		for _, v := range results {
			if v != "ok" {
				status, code = "unavailable", http.StatusServiceUnavailable
			}
		}
		writeJSON(w, code, map[string]any{"status": status, "checks": results})
	})
}

// Register adds both probes to mux.
func Register(ctx context.Context, mux *http.ServeMux, checks ...Check) {
	mux.Handle("GET /livez", Live())
	mux.Handle("GET /readyz", Ready(ctx, checks...))
}

// run executes the checks concurrently and returns "ok" or the error text
// for each, by name.
func run(ctx context.Context, checks []Check) map[string]string {
	results := make(map[string]string, len(checks))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, c := range checks {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, CheckTimeout)
			defer cancel()
			out := "ok"
			if err := c.Fn(cctx); err != nil {
				out = err.Error()
			}
			mu.Lock()
			results[c.Name] = out
			mu.Unlock()
		})
	}
	wg.Wait()
	return results
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// Serve runs just the two probes on port until ctx is cancelled, for a
// service that has no HTTP API of its own (matching, driver-location). An
// empty port does nothing, so it is off unless configured.
func Serve(ctx context.Context, port string, checks ...Check) {
	if port == "" {
		return
	}
	mux := http.NewServeMux()
	Register(ctx, mux, checks...)
	if err := serve.Run(ctx, ":"+port, mux); err != nil {
		log.Printf("health server on :%s stopped: %v", port, err)
	}
}
