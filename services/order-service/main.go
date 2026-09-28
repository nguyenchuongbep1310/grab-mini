package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
)

type createOrderRequest struct {
	RiderID string         `json:"riderId"`
	Pickup  *events.LatLng `json:"pickup"`
	Dropoff *events.LatLng `json:"dropoff"`
}

func isValidLatLng(p *events.LatLng) bool {
	return p != nil && math.Abs(p.Lat) <= 90 && math.Abs(p.Lng) <= 180
}

type server struct {
	db       *pgxpool.Pool
	producer *kafka.Writer
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

	orderID := uuid.NewString()
	ctx := r.Context()

	_, err := s.db.Exec(ctx, `
		INSERT INTO orders (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, status)
		VALUES ($1, $2, $3, $4, $5, $6, 'PENDING')`,
		orderID, req.RiderID, req.Pickup.Lat, req.Pickup.Lng, req.Dropoff.Lat, req.Dropoff.Lng,
	)
	if err != nil {
		log.Printf("failed to insert order: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	event := events.OrderCreated{
		Envelope: events.Envelope{
			EventID:    uuid.NewString(),
			OccurredAt: time.Now().UTC().Format(time.RFC3339),
		},
		OrderID: orderID,
		RiderID: req.RiderID,
		Pickup:  *req.Pickup,
		Dropoff: *req.Dropoff,
	}
	payload, _ := json.Marshal(event)

	err = s.producer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(orderID),
		Value: payload,
	})
	if err != nil {
		log.Printf("failed to publish order.created: %v", err)
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"orderId": orderID, "status": "PENDING"})
}

func main() {
	env := loadEnv()
	ctx := context.Background()

	db, err := connectDB(ctx, env.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	producer := &kafka.Writer{
		Addr:     kafka.TCP(env.KafkaBrokers...),
		Topic:    events.TopicOrderCreated,
		Balancer: &kafka.Hash{},
	}
	defer producer.Close()

	s := &server{db: db, producer: producer}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /orders", s.handleCreateOrder)

	log.Printf("order-service listening on :%s", env.Port)
	if err := http.ListenAndServe(":"+env.Port, mux); err != nil {
		log.Fatal(err)
	}
}
