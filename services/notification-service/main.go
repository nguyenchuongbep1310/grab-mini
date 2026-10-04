package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/health"
	"grab-mini/pkg/serve"
)

// newUpgrader accepts a WebSocket only from a page served by an allowed
// origin. Browsers always send Origin on a WebSocket handshake and let any
// site open one to any host, so without this check a page on another site
// could use a visitor's token to listen to their trip. A client that sends no
// Origin (a script, not a browser) has no such risk and is let through; it
// still needs a valid token.
func newUpgrader(allowedOrigins []string) *websocket.Upgrader {
	allowed := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[strings.TrimRight(strings.ToLower(o), "/")] = true
	}
	return &websocket.Upgrader{
		// The browser sends the token as the second subprotocol; the
		// handshake must name one of the offered protocols in its answer.
		Subprotocols: []string{auth.WebSocketSubprotocol},
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			return origin == "" || allowed[strings.TrimRight(strings.ToLower(origin), "/")]
		},
	}
}

// serveWS implements task 4.1: upgrade to a WebSocket, register the
// connection under the given id, and keep reading until the client
// disconnects (a WS reader is required to process control frames and
// notice a closed connection).
func serveWS(upgrader *websocket.Upgrader, w http.ResponseWriter, r *http.Request, register func(*websocket.Conn) *conn, unregister func(*conn)) {
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
//
// A socket is opened for one rider or driver, and only that person may open
// it: the token must be for the same role and the same id as the URL.
func newMux(ctx context.Context, h *hub, verifier *auth.Verifier, allowedOrigins []string, checks ...health.Check) *http.ServeMux {
	upgrader := newUpgrader(allowedOrigins)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	health.Register(ctx, mux, checks...)

	for _, kind := range []string{kindRider, kindDriver} {
		role := auth.Role(kind)
		mux.HandleFunc("GET /ws/"+kind+"/{id}", func(w http.ResponseWriter, r *http.Request) {
			id := r.PathValue("id")
			token, ok := auth.TokenFromWebSocket(r)
			if !ok {
				auth.Deny(w, http.StatusUnauthorized, "missing token")
				return
			}
			caller, err := verifier.Verify(token)
			if err != nil {
				auth.Deny(w, http.StatusUnauthorized, "invalid or expired token")
				return
			}
			if caller.Role != role || caller.Subject != id {
				auth.Deny(w, http.StatusForbidden, "this token isn't for that "+kind)
				return
			}
			serveWS(upgrader, w, r,
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

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	h := newHub(ctx, rdb)
	var background sync.WaitGroup
	background.Go(func() { h.run(ctx) })
	background.Go(func() { runConsumer(ctx, env, h) })

	log.Printf("notification-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, newMux(ctx, h, verifier, env.AllowedOrigins, health.Redis(rdb), health.Kafka(env.KafkaBrokers))); err != nil {
		log.Fatal(err)
	}
	background.Wait()
	log.Printf("notification-service stopped")
}
