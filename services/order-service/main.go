package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
	"grab-mini/pkg/serve"
)

type createOrderRequest struct {
	RiderID string         `json:"riderId"`
	Pickup  *events.LatLng `json:"pickup"`
	Dropoff *events.LatLng `json:"dropoff"`
	// Optional; defaults to CAR.
	VehicleType events.VehicleType `json:"vehicleType"`
}

func isValidLatLng(p *events.LatLng) bool {
	return p != nil && math.Abs(p.Lat) <= 90 && math.Abs(p.Lng) <= 180
}

// outboxSource names this service in the shared outbox table.
const outboxSource = "order-service"

type server struct {
	db    *pgxpool.Pool
	relay *outbox.Relay
	codec *eventcodec.Codec
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// US-1.1: create an order and publish order.created.
func (s *server) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}

	if req.RiderID == "" || !isValidLatLng(req.Pickup) || !isValidLatLng(req.Dropoff) {
		http.Error(w, `{"error":"riderId, pickup {lat,lng} and dropoff {lat,lng} are required"}`, http.StatusBadRequest)
		return
	}

	if req.VehicleType == "" {
		req.VehicleType = events.VehicleCar
	}
	if !req.VehicleType.Valid() {
		http.Error(w, `{"error":"vehicleType must be CAR or BIKE"}`, http.StatusBadRequest)
		return
	}

	orderID := uuid.NewString()
	ctx := r.Context()

	event := events.OrderCreated{
		Envelope: events.Envelope{
			EventID:    uuid.NewString(),
			OccurredAt: time.Now().UTC().Format(time.RFC3339),
		},
		OrderID: orderID,
		RiderID: req.RiderID,
		Pickup:  *req.Pickup,
		Dropoff: *req.Dropoff,

		VehicleType: req.VehicleType,
	}

	// Encode before touching the database: if the schema registry is down
	// or rejects the schema, no order row is left behind without an event.
	payload, err := s.codec.Encode(ctx, events.TopicOrderCreated, event)
	if err != nil {
		log.Printf("failed to encode order.created: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	// The order and its order.created event commit together, so there is
	// never an order that nobody is told about. The relay publishes the
	// event once the transaction is committed.
	if err := s.insertOrder(ctx, orderID, req, payload); err != nil {
		log.Printf("failed to save order: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	s.relay.Wake()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"orderId": orderID, "status": "PENDING"})
}

func (s *server) insertOrder(ctx context.Context, orderID string, req createOrderRequest, payload []byte) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING')`,
		orderID, req.RiderID, req.Pickup.Lat, req.Pickup.Lng, req.Dropoff.Lat, req.Dropoff.Lng, req.VehicleType,
	)
	if err != nil {
		return err
	}
	if err := outbox.Add(ctx, tx, outboxSource, events.TopicOrderCreated, []byte(orderID), payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// orderView is what clients see of an order.
type orderView struct {
	ID          string        `json:"id"`
	RiderID     string        `json:"riderId"`
	Pickup      events.LatLng `json:"pickup"`
	Dropoff     events.LatLng `json:"dropoff"`
	VehicleType string        `json:"vehicleType"`
	// PENDING until matching reports back, then MATCHED or NO_DRIVER (see outcomes.go).
	Status string `json:"status"`
}

const orderColumns = `id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type, status`

func queryOrder(ctx context.Context, db *pgxpool.Pool, where string, arg string) (*orderView, error) {
	var o orderView
	err := db.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE `+where+` ORDER BY created_at DESC LIMIT 1`, arg).
		Scan(&o.ID, &o.RiderID, &o.Pickup.Lat, &o.Pickup.Lng, &o.Dropoff.Lat, &o.Dropoff.Lng, &o.VehicleType, &o.Status)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func respondWithOrder(w http.ResponseWriter, o *orderView, err error) {
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, `{"error":"order not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("failed to load order: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(o)
}

// handleGetOrder lets a client that only knows the orderId (e.g. the Driver
// App after an order.matched push) find where the trip starts and ends.
func (s *server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	// orders.id is a UUID column: a malformed id would surface as a
	// Postgres error (500) instead of the "not found" it really is.
	if _, err := uuid.Parse(r.PathValue("id")); err != nil {
		http.Error(w, `{"error":"order not found"}`, http.StatusNotFound)
		return
	}
	o, err := queryOrder(r.Context(), s.db, "id = $1", r.PathValue("id"))
	respondWithOrder(w, o, err)
}

// handleGetLatestOrder is how a Rider App that was closed or reloaded finds
// the ride it may still be in the middle of.
func (s *server) handleGetLatestOrder(w http.ResponseWriter, r *http.Request) {
	o, err := queryOrder(r.Context(), s.db, "rider_id = $1", r.PathValue("id"))
	respondWithOrder(w, o, err)
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

	// The writer has no Topic: the outbox rows name theirs.
	producer := &kafka.Writer{
		Addr:                   kafka.TCP(env.KafkaBrokers...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
	}
	defer producer.Close()

	codec := eventcodec.New(env.SchemaRegistryURL)
	relay := outbox.NewRelay(db, producer, outboxSource)
	s := &server{db: db, relay: relay, codec: codec}

	var background sync.WaitGroup
	background.Go(func() { relay.Run(ctx) })
	background.Go(func() { runOutcomeConsumer(ctx, env, db, codec) })

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /orders", s.handleCreateOrder)
	mux.HandleFunc("GET /orders/{id}", s.handleGetOrder)
	mux.HandleFunc("GET /riders/{id}/latest-order", s.handleGetLatestOrder)

	log.Printf("order-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, mux); err != nil {
		log.Fatal(err)
	}
	background.Wait() // the consumer stops and the relay drains before the pool closes
	log.Printf("order-service stopped")
}
