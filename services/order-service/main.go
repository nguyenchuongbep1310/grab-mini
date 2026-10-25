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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/health"
	"grab-mini/pkg/kafkaconn"
	"grab-mini/pkg/logging"
	"grab-mini/pkg/metrics"
	"grab-mini/pkg/outbox"
	"grab-mini/pkg/redisconn"
	"grab-mini/pkg/serve"
	"grab-mini/pkg/surge"
	"grab-mini/pkg/tracing"
)

type createOrderRequest struct {
	RiderID string         `json:"riderId"`
	Pickup  *events.LatLng `json:"pickup"`
	Dropoff *events.LatLng `json:"dropoff"`
	// Optional; defaults to CAR.
	VehicleType events.VehicleType `json:"vehicleType"`
	// Optional: the quote the rider was shown (GET /quotes on Pricing
	// Service). Booking with it locks that price; see pricer.Price.
	QuoteID string `json:"quoteId"`

	// Set by the server once the price is settled, never read from the body.
	surgeMultiplier float64
	price           *float64
}

func isValidLatLng(p *events.LatLng) bool {
	return p != nil && math.Abs(p.Lat) <= 90 && math.Abs(p.Lng) <= 180
}

// ordersCreated is the booking rate, by vehicle type.
var ordersCreated = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "orders_created_total",
	Help: "Orders accepted (saved with their order.created event), by vehicle type.",
}, []string{"vehicle_type"})

// outboxSource names this service in the shared outbox table.
const outboxSource = "order-service"

type server struct {
	db    *pgxpool.Pool
	relay *outbox.Relay
	codec *eventcodec.Codec
	// Settles the price of a booking; nil means no surge (tests).
	pricer *pricer
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// US-1.1: create an order and publish order.created.
func (s *server) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}

	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}

	// The rider is whoever the token says. A body that names someone else is
	// refused rather than quietly overridden: it is a bug or an attack.
	if req.RiderID != "" && req.RiderID != caller.Subject {
		auth.Deny(w, http.StatusForbidden, "riderId doesn't match the token")
		return
	}
	req.RiderID = caller.Subject

	if !isValidLatLng(req.Pickup) || !isValidLatLng(req.Dropoff) {
		http.Error(w, `{"error":"pickup {lat,lng} and dropoff {lat,lng} are required"}`, http.StatusBadRequest)
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

	pr := s.pricer
	if pr == nil {
		pr = defaultPricer()
	}
	settled, err := pr.Price(ctx, req.RiderID, req, req.QuoteID)
	var moved *conflict
	switch {
	case errors.As(err, &moved):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]any{"error": "the price has changed", "newQuote": moved.NewQuote})
		return
	case errors.Is(err, errInvalidQuote):
		http.Error(w, `{"error":"quote is invalid: request a new one"}`, http.StatusBadRequest)
		return
	case err != nil:
		log.Printf("failed to price order: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	req.surgeMultiplier, req.price = settled.Multiplier, &settled.Price

	event := events.OrderCreated{
		Envelope: events.Envelope{
			EventID: uuid.NewString(),
			// Nanosecond precision: Matching measures how long a rider waited from
			// this moment, and whole seconds would make that figure wrong by up to a second.
			OccurredAt: time.Now().UTC().Format(time.RFC3339Nano),
		},
		OrderID: orderID,
		RiderID: req.RiderID,
		Pickup:  *req.Pickup,
		Dropoff: *req.Dropoff,

		VehicleType: req.VehicleType,

		SurgeMultiplier: settled.Multiplier,
		Price:           &settled.Price,
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
	ordersCreated.WithLabelValues(string(req.VehicleType)).Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"orderId": orderID, "status": "PENDING",
		"surgeMultiplier": settled.Multiplier, "price": settled.Price, "currency": surge.Currency,
	})
}

func (s *server) insertOrder(ctx context.Context, orderID string, req createOrderRequest, payload []byte) error {
	multiplier := req.surgeMultiplier
	if multiplier == 0 {
		multiplier = 1.0 // an order built without pricing (tests)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type, status, surge_multiplier, price)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', $8, $9)`,
		orderID, req.RiderID, req.Pickup.Lat, req.Pickup.Lng, req.Dropoff.Lat, req.Dropoff.Lng, req.VehicleType,
		multiplier, req.price,
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
	// Set once CANCELLED: when, by whom (RIDER or DRIVER) and why.
	CancelledAt  *time.Time `json:"cancelledAt,omitempty"`
	CancelledBy  string     `json:"cancelledBy,omitempty"`
	CancelReason string     `json:"cancelReason,omitempty"`
	// What the rider pays (VND) and the surge multiplier it includes; Price is
	// null on orders booked before surge pricing existed.
	SurgeMultiplier float64  `json:"surgeMultiplier"`
	Price           *float64 `json:"price"`

	// The driver it was matched to; never sent, only used to decide who may read it.
	driverID string
}

const orderColumns = `id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type, status, surge_multiplier, price, cancelled_at, cancelled_by, cancel_reason, driver_id`

func queryOrder(ctx context.Context, db *pgxpool.Pool, where string, arg string) (*orderView, error) {
	var o orderView
	err := db.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE `+where+` ORDER BY created_at DESC LIMIT 1`, arg).
		Scan(&o.ID, &o.RiderID, &o.Pickup.Lat, &o.Pickup.Lng, &o.Dropoff.Lat, &o.Dropoff.Lng, &o.VehicleType, &o.Status, &o.SurgeMultiplier, &o.Price, &o.CancelledAt, &o.CancelledBy, &o.CancelReason, &o.driverID)
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

// mayReadOrder says who can see an order: its rider, and the driver it was
// matched to (who learns its id from order.matched and needs the pickup and
// dropoff). No other driver: an order holds a rider's id and where they are
// going, and knowing its UUID is not a reason to hand that over. Order
// Service learns the match from the same event the driver is told by, so the
// driver app retries a 404 for a moment.
func mayReadOrder(c auth.Identity, o *orderView) bool {
	switch c.Role {
	case auth.Rider:
		return o.RiderID == c.Subject
	case auth.Driver:
		return o.driverID != "" && o.driverID == c.Subject
	}
	return false
}

// handleGetOrder lets a client that only knows the orderId (e.g. the Driver
// App after an order.matched push) find where the trip starts and ends.
func (s *server) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	// orders.id is a UUID column: a malformed id would surface as a
	// Postgres error (500) instead of the "not found" it really is.
	if _, err := uuid.Parse(r.PathValue("id")); err != nil {
		http.Error(w, `{"error":"order not found"}`, http.StatusNotFound)
		return
	}
	o, err := queryOrder(r.Context(), s.db, "id = $1", r.PathValue("id"))
	// To anyone who may not read it, an order doesn't exist.
	if err == nil && !mayReadOrder(caller, o) {
		err = pgx.ErrNoRows
	}
	respondWithOrder(w, o, err)
}

// handleGetLatestOrder is how a Rider App that was closed or reloaded finds
// the ride it may still be in the middle of.
func (s *server) handleGetLatestOrder(w http.ResponseWriter, r *http.Request) {
	if caller, ok := auth.From(r.Context()); !ok || caller.Subject != r.PathValue("id") {
		auth.Deny(w, http.StatusForbidden, "you can only read your own orders")
		return
	}
	o, err := queryOrder(r.Context(), s.db, "rider_id = $1", r.PathValue("id"))
	respondWithOrder(w, o, err)
}

// routes is the HTTP API. Every route but /health needs a token, and says
// which roles may call it; the handlers then check the caller owns what they
// touch.
func (s *server) routes(verifier *auth.Verifier) *http.ServeMux {
	// Placing and cancelling orders is what a person does a few times; a
	// script that does it faster is abusing the pricing, matching and database
	// behind it.
	writes := auth.LimiterFromEnv(1, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.Handle("POST /orders", verifier.Require(writes.Wrap(http.HandlerFunc(s.handleCreateOrder)), auth.Rider))
	mux.Handle("GET /orders/{id}", verifier.Require(http.HandlerFunc(s.handleGetOrder), auth.Rider, auth.Driver))
	mux.Handle("POST /orders/{id}/cancel", verifier.Require(writes.Wrap(http.HandlerFunc(s.handleCancelOrder)), auth.Rider))
	mux.Handle("GET /riders/{id}/latest-order", verifier.Require(http.HandlerFunc(s.handleGetLatestOrder), auth.Rider))
	return mux
}

func main() {
	logging.Setup("order-service")
	env := loadEnv()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		runMigrate(env)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "order-service")()

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
	signer, err := surge.SignerFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	rdb := redisconn.MustNew(env.RedisAddr)
	defer rdb.Close()
	s := &server{db: db, relay: relay, codec: codec, pricer: &pricer{
		signer: signer, mult: redisMultipliers{rdb}, tolerance: env.QuoteTolerance,
		cellDeg: surge.CellDegrees(), now: time.Now,
	}}

	var background sync.WaitGroup
	background.Go(func() { relay.Run(ctx) })
	background.Go(func() { runOutcomeConsumer(ctx, env, db, codec) })

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	mux := s.routes(verifier)
	health.Mount(ctx, mux, health.Postgres(db), health.Kafka(env.KafkaBrokers))

	log.Printf("order-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("order-service", metrics.HTTP(mux))); err != nil {
		log.Fatal(err)
	}
	background.Wait() // the consumer stops and the relay drains before the pool closes
	log.Printf("order-service stopped")
}
