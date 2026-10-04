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

func main() {
	env := loadEnv()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	h := newHub()
	var consumer sync.WaitGroup
	consumer.Go(func() { runConsumer(ctx, env, h) })

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)

	mux.HandleFunc("GET /ws/rider/{id}", func(w http.ResponseWriter, r *http.Request) {
		riderID := r.PathValue("id")
		serveWS(w, r,
			func(ws *websocket.Conn) *conn { return h.registerRider(riderID, ws) },
			func(c *conn) { h.unregisterRider(riderID, c) },
		)
	})

	mux.HandleFunc("GET /ws/driver/{id}", func(w http.ResponseWriter, r *http.Request) {
		driverID := r.PathValue("id")
		serveWS(w, r,
			func(ws *websocket.Conn) *conn { return h.registerDriver(driverID, ws) },
			func(c *conn) { h.unregisterDriver(driverID, c) },
		)
	})

	log.Printf("notification-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, mux); err != nil {
		log.Fatal(err)
	}
	consumer.Wait()
	log.Printf("notification-service stopped")
}
