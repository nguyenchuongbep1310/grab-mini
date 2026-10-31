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
	"grab-mini/pkg/promo"
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
	// Optional: the promoToken from GET /promotions/validate (Promotion
	// Service), bound to this rider, price and vehicle type. It is verified
	// here, without calling Promotion Service, and its discount locked on the order.
	PromoToken string `json:"promoToken"`

	// Set by the server once the price and discount are settled, never read from the body.
	surgeMultiplier float64
	price           *float64
	promoCode       string
	discount        int64
}

// writePromoError answers a booking whose promo token was refused.
func writePromoError(w http.ResponseWriter, e *promoError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Status)
	json.NewEncoder(w).Encode(map[string]string{"error": e.Message, "code": e.Code})
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
	// Verifies promo tokens; nil means the development signer (tests).
	promos *promoChecker
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

	idemKey, err := idempotencyKey(r.Header)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadRequest)
		return
	}

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

	ctx := r.Context()

	// An Idempotency-Key retry is answered from what the first attempt stored,
	// before the quote is looked at (a retry carries the quote the first
	// attempt used up). The key is locked until this request ends, so a
	// concurrent duplicate waits and then replays rather than booking twice.
	var tx pgx.Tx
	fp := fingerprint(req)
	if idemKey != "" {
		var err error
		if tx, err = s.db.Begin(ctx); err != nil {
			log.Printf("failed to begin transaction: %v", err)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}
		defer tx.Rollback(ctx)
		var rec *idemRecord
		if err = lockKey(ctx, tx, req.RiderID, idemKey); err == nil {
			rec, err = lookupKey(ctx, tx, req.RiderID, idemKey)
		}
		if err != nil {
			log.Printf("failed to look up idempotency key: %v", err)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}
		if answerFromKey(w, rec, fp) {
			return
		}
	}

	orderID := uuid.NewString()

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
		json.NewEncoder(w).Encode(map[string]any{"error": moved.Error(), "newQuote": moved.NewQuote})
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
	// From here a failure must hand the quote back, so the retry can use it.
	saved := false
	defer func() {
		if !saved {
			pr.release(ctx, settled)
		}
	}()

	// A promo token is checked against the price just settled, so a token for
	// another price (the quote expired and the price moved) is refused rather than
	// applied to a different fare. Not applied on a failure here or later: the
	// deferred release gives the token back along with the quote.
	if req.PromoToken != "" {
		pc := s.promos
		if pc == nil {
			pc = defaultPromoChecker()
		}
		a, err := pc.Apply(ctx, req.RiderID, req.VehicleType, settled.Price, req.PromoToken)
		var pe *promoError
		if errors.As(err, &pe) {
			writePromoError(w, pe)
			return
		}
		req.promoCode, req.discount = a.Code, a.Discount
		defer func() {
			if !saved {
				pc.release(ctx, a)
			}
		}()
	}

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

		PromoCode: req.promoCode,
		Discount:  req.discount,
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
	resp := map[string]any{
		"orderId": orderID, "status": "PENDING",
		"surgeMultiplier": settled.Multiplier, "price": settled.Price, "currency": surge.Currency,
		"discount": req.discount, "finalPrice": settled.Price - float64(req.discount),
	}
	if req.promoCode != "" {
		resp["promoCode"] = req.promoCode
	}
	respBody, _ := json.Marshal(resp)
	respBody = append(respBody, '\n')

	var idem *idemRecord
	if idemKey != "" {
		idem = &idemRecord{Key: idemKey, Fingerprint: fp, OrderID: orderID, Status: http.StatusCreated, Body: respBody}
	}
	if tx != nil {
		err = s.insertOrderTx(ctx, tx, orderID, req, payload, idem)
		if err == nil {
			err = tx.Commit(ctx)
		}
	} else {
		err = s.insertOrder(ctx, orderID, req, payload)
	}
	if isDuplicateKey(err) {
		// Another request with this key committed first (the lock normally
		// makes it finish before we get here): answer with its order.
		rec, lerr := lookupKey(ctx, s.db, req.RiderID, idemKey)
		if lerr == nil && answerFromKey(w, rec, fp) {
			return
		}
	}
	if err != nil {
		log.Printf("failed to save order: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	saved = true
	s.relay.Wake()
	ordersCreated.WithLabelValues(string(req.VehicleType)).Inc()
	if idemKey != "" {
		idempotentRequests.WithLabelValues("new").Inc()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(respBody)
}

// insertOrder saves an order and its event in one transaction.
func (s *server) insertOrder(ctx context.Context, orderID string, req createOrderRequest, payload []byte) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := s.insertOrderTx(ctx, tx, orderID, req, payload, nil); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// insertOrderTx adds the order, its order.created outbox row and, when the
// booking carries an Idempotency-Key, the key's record to tx. The caller commits.
func (s *server) insertOrderTx(ctx context.Context, tx pgx.Tx, orderID string, req createOrderRequest, payload []byte, idem *idemRecord) error {
	multiplier := req.surgeMultiplier
	if multiplier == 0 {
		multiplier = 1.0 // an order built without pricing (tests)
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO orders (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type, status, surge_multiplier, price, promo_code, discount)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', $8, $9, $10, $11)`,
		orderID, req.RiderID, req.Pickup.Lat, req.Pickup.Lng, req.Dropoff.Lat, req.Dropoff.Lng, req.VehicleType,
		multiplier, req.price, req.promoCode, req.discount,
	)
	if err != nil {
		return err
	}
	if err := outbox.Add(ctx, tx, outboxSource, events.TopicOrderCreated, []byte(orderID), payload); err != nil {
		return err
	}
	if idem != nil {
		return saveKey(ctx, tx, req.RiderID, *idem)
	}
	return nil
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
	// The promo code the order booked with and the VND it takes off the price
	// (0 and empty for none); FinalPrice is what the rider pays, null with Price.
	PromoCode  string   `json:"promoCode,omitempty"`
	Discount   int64    `json:"discount"`
	FinalPrice *float64 `json:"finalPrice"`

	// The driver it was matched to; never sent, only used to decide who may read it.
	driverID string
}

const orderColumns = `id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type, status, surge_multiplier, price, promo_code, discount, cancelled_at, cancelled_by, cancel_reason, driver_id`

func queryOrder(ctx context.Context, db *pgxpool.Pool, where string, arg string) (*orderView, error) {
	var o orderView
	err := db.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE `+where+` ORDER BY created_at DESC LIMIT 1`, arg).
		Scan(&o.ID, &o.RiderID, &o.Pickup.Lat, &o.Pickup.Lng, &o.Dropoff.Lat, &o.Dropoff.Lng, &o.VehicleType, &o.Status, &o.SurgeMultiplier, &o.Price, &o.PromoCode, &o.Discount, &o.CancelledAt, &o.CancelledBy, &o.CancelReason, &o.driverID)
	if err != nil {
		return nil, err
	}
	if o.Price != nil {
		final := max(*o.Price-float64(o.Discount), 0)
		o.FinalPrice = &final
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
	promoSigner, err := promo.SignerFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	s := &server{db: db, relay: relay, codec: codec, pricer: &pricer{
		signer: signer, mult: redisMultipliers{rdb}, tolerance: env.QuoteTolerance,
		cellDeg: surge.CellDegrees(), now: time.Now, claims: redisClaims{rdb},
	}, promos: &promoChecker{signer: promoSigner, claims: redisClaims{rdb}, now: time.Now}}

	var background sync.WaitGroup
	background.Go(func() { relay.Run(ctx) })
	background.Go(func() { runOutcomeConsumer(ctx, env, db, codec) })
	background.Go(func() { runKeyPurge(ctx, db, time.Hour) })

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
