package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
	"grab-mini/pkg/serve"
)

// outboxSource names this service in the shared outbox table.
const outboxSource = "trip-service"

type server struct {
	db    *pgxpool.Pool
	rdb   *redis.Client
	relay *outbox.Relay
	codec *eventcodec.Codec
}

func (s *server) encodeStatusChanged(ctx context.Context, t *trip, status events.TripStatus) ([]byte, error) {
	return s.codec.Encode(ctx, events.TopicTripStatusChanged, events.TripStatusChanged{
		Envelope: events.Envelope{EventID: uuid.NewString(), OccurredAt: time.Now().UTC().Format(time.RFC3339)},
		TripID:   t.ID,
		OrderID:  t.OrderID,
		DriverID: t.DriverID,
		Status:   status,
		RiderID:  t.RiderID,
	})
}

// transitionWithEvent moves a trip to targetStatus and queues its event in
// one transaction, so the state and the announcement can't disagree. It
// reports false (and queues nothing) if the trip wasn't in one of fromStatuses.
func (s *server) transitionWithEvent(ctx context.Context, id string, fromStatuses []events.TripStatus, targetStatus events.TripStatus, field timestampField, key, payload []byte) (bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	transitioned, err := transitionTrip(ctx, tx, id, fromStatuses, targetStatus, field)
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
	return true, nil
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
	s.respondWithTrip(w, r, func() (*trip, error) { return getActiveTripByDriver(r.Context(), s.db, r.PathValue("id")) })
}

func (s *server) respondWithTrip(w http.ResponseWriter, r *http.Request, load func() (*trip, error)) {
	t, err := load()
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
	s.handleTransition(w, r,
		[]events.TripStatus{events.TripStatusAssigned},
		events.TripStatusInProgress,
		startedAt,
		nil,
	)
}

// US-4.2: completing a trip moves IN_PROGRESS to COMPLETED and releases
// the driver back to AVAILABLE.
func (s *server) handleComplete(w http.ResponseWriter, r *http.Request) {
	s.handleTransition(w, r,
		[]events.TripStatus{events.TripStatusInProgress},
		events.TripStatusCompleted,
		completedAt,
		func(ctx context.Context, t *trip) error {
			return driverstatus.Set(ctx, s.rdb, t.DriverID, driverstatus.Available)
		},
	)
}

// US-4.3: cancelling is allowed from ASSIGNED or IN_PROGRESS, never once
// COMPLETED.
func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	s.handleTransition(w, r,
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
func (s *server) handleTransition(w http.ResponseWriter, r *http.Request, fromStatuses []events.TripStatus, targetStatus events.TripStatus, field timestampField, onTransition func(context.Context, *trip) error) {
	ctx := r.Context()
	id := r.PathValue("id")

	t, err := getTripByID(ctx, s.db, id)
	if errors.Is(err, errTripNotFound) {
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
	payload, err := s.encodeStatusChanged(ctx, t, targetStatus)
	if err != nil {
		log.Printf("failed to encode trip.status.changed: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	transitioned, err := s.transitionWithEvent(ctx, id, fromStatuses, targetStatus, field, []byte(t.ID), payload)
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
}

const maxCommentLength = 500

// A rider rates a completed trip once, from 1 to 5 stars. There's no real
// authentication in this project, so riderId comes from the request (the
// Rider App sends the id it connected with).
func (s *server) handleRateTrip(w http.ResponseWriter, r *http.Request) {
	var req rateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON body"}`, http.StatusBadRequest)
		return
	}
	if req.RiderID == "" || req.Stars < 1 || req.Stars > 5 || utf8.RuneCountInString(req.Comment) > maxCommentLength {
		http.Error(w, `{"error":"riderId and stars (1 to 5) are required; comment is at most 500 characters"}`, http.StatusBadRequest)
		return
	}

	id := r.PathValue("id")
	// trips.id is a UUID column: a malformed id would otherwise be a Postgres error.
	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, `{"error":"trip not found"}`, http.StatusNotFound)
		return
	}

	ctx := r.Context()
	t, err := getTripByID(ctx, s.db, id)
	if errors.Is(err, errTripNotFound) {
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
		Envelope: events.Envelope{EventID: uuid.NewString(), OccurredAt: time.Now().UTC().Format(time.RFC3339)},
		TripID:   t.ID,
		OrderID:  t.OrderID,
		RiderID:  req.RiderID,
		DriverID: t.DriverID,
		Stars:    req.Stars,
		Comment:  req.Comment,
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
	json.NewEncoder(w).Encode(map[string]any{"tripId": t.ID, "stars": req.Stars})
}

// rateWithEvent saves the rating and queues trip.rated in one transaction, so
// a rating that is stored is always announced (Rating Service counts it).
func (s *server) rateWithEvent(ctx context.Context, t *trip, req rateRequest, payload []byte) (bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	saved, err := saveRating(ctx, tx, t.ID, req.Stars, req.Comment)
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
	return true, nil
}

func main() {
	env := loadEnv()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := connectDB(ctx, env.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: env.RedisAddr})
	defer rdb.Close()

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
	s := &server{db: db, rdb: rdb, relay: relay, codec: codec}

	var background sync.WaitGroup
	background.Go(func() { relay.Run(ctx) })
	background.Go(func() { runOrderMatchedConsumer(ctx, env, db, rdb, codec) })
	background.Go(func() { runDriverReleaseConsumer(ctx, env, db, rdb, codec) })

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /trips/{id}", s.handleGetTrip)
	mux.HandleFunc("GET /trips/by-order/{orderId}", s.handleGetTripByOrder)
	mux.HandleFunc("GET /drivers/{id}/active-trip", s.handleGetActiveTrip)
	mux.HandleFunc("POST /trips/{id}/pickup", s.handlePickup)
	mux.HandleFunc("POST /trips/{id}/complete", s.handleComplete)
	mux.HandleFunc("POST /trips/{id}/cancel", s.handleCancel)
	mux.HandleFunc("POST /trips/{id}/rating", s.handleRateTrip)

	log.Printf("trip-service listening on :%s", env.Port)
	if err := serve.Run(ctx, ":"+env.Port, mux); err != nil {
		log.Fatal(err)
	}
	background.Wait() // the consumer stops and the relay drains before the pool closes
	log.Printf("trip-service stopped")
}
