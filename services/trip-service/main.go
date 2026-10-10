package main

import (
	"context"
	"encoding/json"
	"errors"
	"grab-mini/pkg/redisconn"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/health"
	"grab-mini/pkg/kafkaconn"
	"grab-mini/pkg/logging"
	"grab-mini/pkg/metrics"
	"grab-mini/pkg/outbox"
	"grab-mini/pkg/serve"
	"grab-mini/pkg/tracing"
)

var (
	tripTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "trip_transitions_total",
		Help: "Trip status changes committed, by the status moved to.",
	}, []string{"status"})

	tripRatings = promauto.NewCounter(prometheus.CounterOpts{
		Name: "trip_ratings_total",
		Help: "Ratings saved.",
	})
)

// outboxSource names this service in the shared outbox table.
const outboxSource = "trip-service"

type server struct {
	db    *pgxpool.Pool
	rdb   *redis.Client
	relay *outbox.Relay
	codec *eventcodec.Codec
}

func (s *server) encodeStatusChanged(ctx context.Context, t *trip, status events.TripStatus, cancel ...cancelInfo) ([]byte, error) {
	evt := events.TripStatusChanged{
		Envelope: events.Envelope{EventID: uuid.NewString(), OccurredAt: time.Now().UTC().Format(time.RFC3339)},
		TripID:   t.ID,
		OrderID:  t.OrderID,
		DriverID: t.DriverID,
		Status:   status,
		RiderID:  t.RiderID,
	}
	if len(cancel) > 0 {
		evt.CancelledBy, evt.Reason = cancel[0].By, cancel[0].Reason
	}
	return s.codec.Encode(ctx, events.TopicTripStatusChanged, evt)
}

// transitionWithEvent moves a trip to targetStatus and queues its event in
// one transaction, so the state and the announcement can't disagree. It
// reports false (and queues nothing) if the trip wasn't in one of fromStatuses.
func (s *server) transitionWithEvent(ctx context.Context, id string, fromStatuses []events.TripStatus, targetStatus events.TripStatus, field timestampField, key, payload []byte, cancel ...cancelInfo) (bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	transitioned, err := transitionTrip(ctx, tx, id, fromStatuses, targetStatus, field, cancel...)
	if err != nil || !transitioned {
		return false, err
	}
	if err := outbox.Add(ctx, tx, outboxSource, events.TopicTripStatusChanged, key, payload); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	s.relay.Wake()
	tripTransitions.WithLabelValues(string(targetStatus)).Inc()
	return true, nil
}

// isDriverOf and isRiderOf say whether the caller is that side of the trip.
// Trips made before rider_id was recorded have none, so no rider matches them.
func isDriverOf(c auth.Identity, t *trip) bool {
	return c.Role == auth.Driver && c.Subject == t.DriverID
}
func isRiderOf(c auth.Identity, t *trip) bool {
	return c.Role == auth.Rider && t.RiderID != "" && c.Subject == t.RiderID
}

func isParty(c auth.Identity, t *trip) bool { return isDriverOf(c, t) || isRiderOf(c, t) }

// caller returns who is calling, or writes a 401 and reports false.
func caller(w http.ResponseWriter, r *http.Request) (auth.Identity, bool) {
	c, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
	}
	return c, ok
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *server) handleGetTrip(w http.ResponseWriter, r *http.Request) {
	s.respondWithTrip(w, r, func() (*trip, error) { return getTripByID(r.Context(), s.db, r.PathValue("id")) })
}

func (s *server) handleGetTripByOrder(w http.ResponseWriter, r *http.Request) {
	s.respondWithTrip(w, r, func() (*trip, error) { return getTripByOrderID(r.Context(), s.db, r.PathValue("orderId")) })
}

func (s *server) handleGetActiveTrip(w http.ResponseWriter, r *http.Request) {
	if c, ok := caller(w, r); !ok {
		return
	} else if c.Role != auth.Driver || c.Subject != r.PathValue("id") {
		auth.Deny(w, http.StatusForbidden, "you can only read your own active trip")
		return
	}
	s.respondWithTrip(w, r, func() (*trip, error) { return getActiveTripByDriver(r.Context(), s.db, r.PathValue("id")) })
}

// respondWithTrip sends the trip load returns, to its rider or its driver. To
// anyone else it is a 404: whether a trip exists is not theirs to learn.
func (s *server) respondWithTrip(w http.ResponseWriter, r *http.Request, load func() (*trip, error)) {
	c, ok := caller(w, r)
	if !ok {
		return
	}
	t, err := load()
	if err == nil && !isParty(c, t) {
		err = errTripNotFound
	}
	if errors.Is(err, errTripNotFound) {
		http.Error(w, `{"error":"trip not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("failed to load trip: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(t)
}

// US-4.1: confirm pickup moves a trip from ASSIGNED to IN_PROGRESS.
func (s *server) handlePickup(w http.ResponseWriter, r *http.Request) {
	s.handleTransition(w, r, isDriverOf,
		[]events.TripStatus{events.TripStatusAssigned},
		events.TripStatusInProgress,
		startedAt,
		nil,
	)
}

// US-4.2: completing a trip moves IN_PROGRESS to COMPLETED and releases
// the driver back to AVAILABLE.
func (s *server) handleComplete(w http.ResponseWriter, r *http.Request) {
	s.handleTransition(w, r, isDriverOf,
		[]events.TripStatus{events.TripStatusInProgress},
		events.TripStatusCompleted,
		completedAt,
		func(ctx context.Context, t *trip) error {
			return driverstatus.Set(ctx, s.rdb, t.DriverID, driverstatus.Available)
		},
	)
}

// US-4.3: cancelling is allowed from ASSIGNED or IN_PROGRESS, never once
// COMPLETED. The caller's role says who cancelled (a rider or the driver),
// and an optional {"reason": "..."} body says why; both go out on the event.
func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	s.handleTransition(w, r, isParty,
		[]events.TripStatus{events.TripStatusAssigned, events.TripStatusInProgress},
		events.TripStatusCancelled,
		cancelledAt,
		func(ctx context.Context, t *trip) error {
			return driverstatus.Set(ctx, s.rdb, t.DriverID, driverstatus.Available)
		},
	)
}

// handleTransition implements the shared idempotent-transition shape
// used by pickup/complete/cancel: move the trip to targetStatus only if
// it's currently in fromStatuses. If it's already at targetStatus, the
// call is a no-op success (US-6.1). Any other current status is a
// rejected transition (e.g. US-4.3: cancelling an already-completed trip).
func (s *server) handleTransition(w http.ResponseWriter, r *http.Request, mayDo func(auth.Identity, *trip) bool, fromStatuses []events.TripStatus, targetStatus events.TripStatus, field timestampField, onTransition func(context.Context, *trip) error) {
	c, ok := caller(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	id := r.PathValue("id")

	var cancel []cancelInfo
	if targetStatus == events.TripStatusCancelled {
		info, ok := readCancelInfo(w, r, c)
		if !ok {
			return
		}
		cancel = append(cancel, info)
	}

	t, err := getTripByID(ctx, s.db, id)
	if errors.Is(err, errTripNotFound) || (err == nil && !mayDo(c, t)) {
		http.Error(w, `{"error":"trip not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("failed to load trip: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	if t.Status == targetStatus {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"tripId": t.ID, "status": string(t.Status)})
		return
	}

	// Encode before changing any state: if the schema registry is down we
	// refuse the request rather than move the trip without telling anyone.
	// The event is queued with the state change (see transitionWithEvent).
	payload, err := s.encodeStatusChanged(ctx, t, targetStatus, cancel...)
	if err != nil {
		log.Printf("failed to encode trip.status.changed: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	transitioned, err := s.transitionWithEvent(ctx, id, fromStatuses, targetStatus, field, []byte(t.ID), payload, cancel...)
	if err != nil {
		log.Printf("failed to transition trip: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	if !transitioned {
		http.Error(w, `{"error":"trip cannot transition to `+string(targetStatus)+` from its current status"}`, http.StatusConflict)
		return
	}

	t.Status = targetStatus

	if onTransition != nil {
		if err := onTransition(ctx, t); err != nil {
			log.Printf("post-transition step failed for trip %s: %v", t.ID, err)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"tripId": t.ID, "status": string(t.Status)})
}

type rateRequest struct {
	RiderID string `json:"riderId"`
	Stars   int    `json:"stars"`
	Comment string `json:"comment"`
	// TipAmount is optional, whole VND (0 to maxTip). It is recorded only.
	TipAmount int64 `json:"tipAmount"`
}

const (
	maxCommentLength = 500
	maxTipAmount     = 1_000_000 // VND
)

// normalizeRating trims the comment in place and returns a client-facing
// error message, or "" when the request is valid.
func normalizeRating(req *rateRequest) string {
	req.Comment = strings.TrimSpace(req.Comment)
	if req.Stars < 1 || req.Stars > 5 || utf8.RuneCountInString(req.Comment) > maxCommentLength {
		return "stars (1 to 5) are required; comment is at most 500 characters"
	}
	for _, r := range req.Comment {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return "comment contains control characters"
		}
	}
	if req.TipAmount < 0 || req.TipAmount > maxTipAmount {
		return "tipAmount must be a whole number from 0 to 1000000"
	}
	return ""
}

// A rider rates a completed trip once, from 1 to 5 stars. There's no real
// authentication in this project, so riderId comes from the request (the
// Rider App sends the id it connected with).
func (s *server) handleRateTrip(w http.ResponseWriter, r *http.Request) {
	c, ok := caller(w, r)
	if !ok {
		return
	}

	var req rateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}
	if msg := normalizeRating(&req); msg != "" {
		http.Error(w, `{"error":"`+msg+`"}`, http.StatusBadRequest)
		return
	}
	// The rider is whoever the token says; a body naming someone else is refused.
	if req.RiderID != "" && req.RiderID != c.Subject {
		auth.Deny(w, http.StatusForbidden, "riderId doesn't match the token")
		return
	}
	req.RiderID = c.Subject

	id := r.PathValue("id")
	// trips.id is a UUID column: a malformed id would otherwise be a Postgres error.
	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, `{"error":"trip not found"}`, http.StatusNotFound)
		return
	}

	ctx := r.Context()
	t, err := getTripByID(ctx, s.db, id)
	if errors.Is(err, errTripNotFound) || (err == nil && !isRiderOf(c, t)) { // only the trip's rider may rate it
		http.Error(w, `{"error":"trip not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("failed to load trip: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	if t.Status != events.TripStatusCompleted {
		http.Error(w, `{"error":"only completed trips can be rated"}`, http.StatusConflict)
		return
	}
	if t.Rating != nil {
		http.Error(w, `{"error":"this trip was already rated"}`, http.StatusConflict)
		return
	}

	// Encode before saving, like status changes: no rating without an event.
	payload, err := s.codec.Encode(ctx, events.TopicTripRated, events.TripRated{
		Envelope:  events.Envelope{EventID: uuid.NewString(), OccurredAt: time.Now().UTC().Format(time.RFC3339)},
		TripID:    t.ID,
		OrderID:   t.OrderID,
		RiderID:   req.RiderID,
		DriverID:  t.DriverID,
		Stars:     req.Stars,
		Comment:   req.Comment,
		TipAmount: req.TipAmount,
		Currency:  "VND",
	})
	if err != nil {
		log.Printf("failed to encode trip.rated: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	saved, err := s.rateWithEvent(ctx, t, req, payload)
	if err != nil {
		log.Printf("failed to save rating: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	if !saved {
		http.Error(w, `{"error":"this trip was already rated"}`, http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{"tripId": t.ID, "stars": req.Stars, "tipAmount": req.TipAmount})
}

// rateWithEvent saves the rating and queues trip.rated in one transaction, so
// a rating that is stored is always announced (Rating Service counts it).
func (s *server) rateWithEvent(ctx context.Context, t *trip, req rateRequest, payload []byte) (bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	saved, err := saveRating(ctx, tx, t.ID, req.Stars, req.Comment, req.TipAmount)
	if err != nil || !saved {
		return false, err
	}
	if err := outbox.Add(ctx, tx, outboxSource, events.TopicTripRated, []byte(t.DriverID), payload); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	s.relay.Wake()
	tripRatings.Inc()
	return true, nil
}

// routes is the HTTP API. Every route but /health needs a token and names the
// roles allowed; the handlers then check the caller is a party to the trip.
func (s *server) routes(verifier *auth.Verifier) *http.ServeMux {
	anyone := func(h http.HandlerFunc) http.Handler { return verifier.Require(h, auth.Rider, auth.Driver) }
	only := func(role auth.Role, h http.HandlerFunc) http.Handler { return verifier.Require(h, role) }

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.Handle("GET /trips/{id}", anyone(s.handleGetTrip))
	mux.Handle("GET /trips/by-order/{orderId}", anyone(s.handleGetTripByOrder))
	mux.Handle("GET /drivers/{id}/active-trip", only(auth.Driver, s.handleGetActiveTrip))
	mux.Handle("POST /trips/{id}/pickup", only(auth.Driver, s.handlePickup))
	mux.Handle("POST /trips/{id}/complete", only(auth.Driver, s.handleComplete))
	mux.Handle("POST /trips/{id}/cancel", anyone(s.handleCancel))
	mux.Handle("POST /trips/{id}/rating", only(auth.Rider, s.handleRateTrip))
	return mux
}

func main() {
	logging.Setup("trip-service")
	env := loadEnv()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		runMigrate(env)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	defer tracing.Setup(ctx, "trip-service")()

	db, err := connectDB(ctx, env.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	rdb := redisconn.MustNew(env.RedisAddr)
	defer rdb.Close()

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
	s := &server{db: db, rdb: rdb, relay: relay, codec: codec}

	var background sync.WaitGroup
	background.Go(func() { relay.Run(ctx) })
	background.Go(func() { runOrderMatchedConsumer(ctx, env, db, rdb, codec) })
	background.Go(func() { runDriverReleaseConsumer(ctx, env, db, rdb, codec) })
	background.Go(func() { runOrderCancelledConsumer(ctx, env, s, codec) })

	verifier, err := auth.VerifierFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	mux := s.routes(verifier)
	health.Mount(ctx, mux, health.Postgres(db), health.Redis(rdb), health.Kafka(env.KafkaBrokers))

	log.Printf("trip-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, tracing.HTTP("trip-service", metrics.HTTP(mux))); err != nil {
		log.Fatal(err)
	}
	background.Wait() // the consumer stops and the relay drains before the pool closes
	log.Printf("trip-service stopped")
}
