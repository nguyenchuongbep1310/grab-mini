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

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/health"
	"grab-mini/pkg/serve"
)

var upgrader = websocket.Upgrader{
	// Local learning project, single origin: no CSRF-relevant cross-site
	// state to protect, so any origin is fine here.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// serveWS implements task 4.1: upgrade to a WebSocket, register the
// connection under the given id, and keep reading until the client
// disconnects (a WS reader is required to process control frames and
// notice a closed connection).
func serveWS(w http.ResponseWriter, r *http.Request, register func(*websocket.Conn) *conn, unregister func(*conn)) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}
	c := register(ws)
	defer func() {
		unregister(c)
		ws.Close()
	}()

	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			return
		}
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// newMux wires the HTTP and WebSocket routes. ctx is the process context,
// which the sockets' Redis subscriptions use.
func newMux(ctx context.Context, h *hub, checks ...health.Check) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	health.Register(ctx, mux, checks...)

	for _, kind := range []string{kindRider, kindDriver} {
		mux.HandleFunc("GET /ws/"+kind+"/{id}", func(w http.ResponseWriter, r *http.Request) {
			id := r.PathValue("id")
			serveWS(w, r,
				func(ws *websocket.Conn) *conn { return h.register(ctx, kind, id, ws) },
				func(c *conn) { h.unregister(ctx, kind, id, c) },
			)
		})
	}
	return mux
}

func main() {
	env := loadEnv()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr})
	defer rdb.Close()

	h := newHub(ctx, rdb)
	var background sync.WaitGroup
	background.Go(func() { h.run(ctx) })
	background.Go(func() { runConsumer(ctx, env, h) })

	log.Printf("notification-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, newMux(ctx, h, health.Redis(rdb), health.Kafka(env.KafkaBrokers))); err != nil {
		log.Fatal(err)
	}
	background.Wait()
	log.Printf("notification-service stopped")
}
