// Package e2e_test boots the entire grab-mini backend microservice fleet
// (dev-registry, auth, pricing, order, matching, trip, driver-location,
// location-gateway, notification, rating) as child processes against a real
// Kafka broker, Postgres database, and Redis instance, then exercises the
// full ride-booking lifecycle over HTTP and WebSockets.
//
// Like the other Kafka/Postgres/Redis integration tests in this repository, it
// skips itself unless TEST_KAFKA_BROKERS, TEST_DATABASE_URL, and
// TEST_REDIS_ADDR are set.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/events"
)

type stack struct {
	t           *testing.T
	rdb         *redis.Client
	db          *pgxpool.Pool
	http        *http.Client
	registryURL string
	authURL     string
	pricingURL  string
	orderURL    string
	tripURL     string
	notifyURL   string
	ratingURL   string
	gatewayURL  string
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	return port
}

// createTopics provisions the topics the fleet reads and writes, the way the
// kafka-init service does in docker-compose.yml. The services create only their
// own retry and dead-letter topics, never these: a production broker doesn't
// auto-create topics.
//
// It has to happen before the services start, not whenever the first event
// happens to create a topic. A consumer group that joined while its topic didn't
// exist yet never sees the partitions that appear afterwards, so on a fresh
// broker the first events would reach nobody (and a producer's first write can
// fail with Unknown Topic Or Partition while the broker creates the topic).
func createTopics(t *testing.T, brokers string) {
	t.Helper()
	topics := []string{
		events.TopicOrderCreated,
		events.TopicOrderMatched,
		events.TopicOrderNoDriverFound,
		events.TopicOrderCancelled,
		events.TopicDriverLocationUpdate,
		events.TopicTripStatusChanged,
		events.TopicTripRated,
	}

	var specs []kafka.TopicConfig
	for _, topic := range topics {
		specs = append(specs, kafka.TopicConfig{Topic: topic, NumPartitions: 3, ReplicationFactor: 1})
	}
	client := &kafka.Client{Addr: kafka.TCP(strings.Split(brokers, ",")...), Timeout: 10 * time.Second}
	resp, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{Topics: specs})
	if err != nil {
		t.Fatalf("create topics: %v", err)
	}
	for topic, err := range resp.Errors {
		if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}

	// Creating a topic returns before every partition has a leader to talk to.
	deadline := time.Now().Add(15 * time.Second)
	for {
		meta, err := client.Metadata(context.Background(), &kafka.MetadataRequest{Topics: topics})
		ready := err == nil && len(meta.Topics) == len(topics)
		if ready {
			for _, tp := range meta.Topics {
				if tp.Error != nil || len(tp.Partitions) != 3 {
					ready = false
					break
				}
				for _, p := range tp.Partitions {
					if p.Error != nil || p.Leader.ID < 0 {
						ready = false
					}
				}
			}
		}
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("topics were not ready in time: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func startStack(t *testing.T) *stack {
	t.Helper()
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	dbURL := os.Getenv("TEST_DATABASE_URL")
	redisAddr := os.Getenv("TEST_REDIS_ADDR")
	if brokers == "" || dbURL == "" || redisAddr == "" {
		t.Skip("TEST_KAFKA_BROKERS, TEST_DATABASE_URL, and TEST_REDIS_ADDR required for full-stack E2E test")
	}

	createTopics(t, brokers)

	ctx := context.Background()
	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binDir := t.TempDir()

	services := []string{
		"dev-registry",
		"auth-service",
		"pricing-service",
		"order-service",
		"matching-service",
		"trip-service",
		"driver-location-service",
		"notification-service",
		"rating-service",
		"location-gateway",
	}
	for _, svc := range services {
		cmd := exec.Command("go", "build", "-o", filepath.Join(binDir, svc), ".")
		cmd.Dir = filepath.Join(root, "services", svc)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", svc, err, out)
		}
	}

	regPort := freePort(t)
	authPort := freePort(t)
	pricingPort := freePort(t)
	orderPort := freePort(t)
	tripPort := freePort(t)
	notifyPort := freePort(t)
	ratingPort := freePort(t)
	gatewayPort := freePort(t)
	matchHealthPort := freePort(t)
	locHealthPort := freePort(t)

	st := &stack{
		t:           t,
		rdb:         rdb,
		db:          db,
		http:        &http.Client{Timeout: 5 * time.Second},
		registryURL: "http://127.0.0.1:" + regPort,
		authURL:     "http://127.0.0.1:" + authPort,
		pricingURL:  "http://127.0.0.1:" + pricingPort,
		orderURL:    "http://127.0.0.1:" + orderPort,
		tripURL:     "http://127.0.0.1:" + tripPort,
		notifyURL:   "http://127.0.0.1:" + notifyPort,
		ratingURL:   "http://127.0.0.1:" + ratingPort,
		gatewayURL:  "http://127.0.0.1:" + gatewayPort,
	}

	baseEnv := append(os.Environ(),
		"KAFKA_BROKERS="+brokers,
		"DATABASE_URL="+dbURL,
		"REDIS_ADDR="+redisAddr,
		"SCHEMA_REGISTRY_URL="+st.registryURL,
		"JWT_SECRET=0123456789abcdef0123456789abcdef",
		"QUOTE_SECRET=fedcba9876543210fedcba9876543210",
		"MATCH_RETRIES=2",
		"MATCH_RETRY_DELAY_MS=200",
	)

	launch := func(name string, extraEnv ...string) {
		cmd := exec.Command(filepath.Join(binDir, name))
		cmd.Dir = filepath.Join(root, "services", name)
		cmd.Env = append(append([]string{}, baseEnv...), extraEnv...)
		var logs bytes.Buffer
		cmd.Stdout = &logs
		cmd.Stderr = &logs
		if err := cmd.Start(); err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		t.Cleanup(func() {
			_ = cmd.Process.Signal(os.Interrupt)
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				_ = cmd.Process.Kill()
			}
			if t.Failed() {
				t.Logf("=== %s logs ===\n%s", name, logs.String())
			}
		})
	}

	launch("dev-registry", "PORT="+regPort)
	st.waitReady(st.registryURL + "/subjects")

	launch("auth-service", "PORT="+authPort)
	launch("pricing-service", "PORT="+pricingPort)
	launch("order-service", "PORT="+orderPort)
	launch("trip-service", "PORT="+tripPort)
	launch("notification-service", "PORT="+notifyPort)
	launch("rating-service", "PORT="+ratingPort)
	launch("location-gateway", "PORT="+gatewayPort)
	launch("matching-service", "HEALTH_PORT="+matchHealthPort, "INSTANCE_ID=matching-e2e")
	launch("driver-location-service", "HEALTH_PORT="+locHealthPort)

	for _, url := range []string{
		st.authURL + "/health",
		st.pricingURL + "/readyz",
		st.orderURL + "/readyz",
		st.tripURL + "/readyz",
		st.notifyURL + "/readyz",
		st.ratingURL + "/readyz",
		st.gatewayURL + "/readyz",
		"http://127.0.0.1:" + matchHealthPort + "/readyz",
		"http://127.0.0.1:" + locHealthPort + "/readyz",
	} {
		st.waitReady(url)
	}

	// Clean any residual rows after migrations have run
	_, _ = db.Exec(ctx, `DELETE FROM outbox; DELETE FROM processed_events; DELETE FROM cancelled_orders; DELETE FROM trips; DELETE FROM orders; DELETE FROM driver_reviews; DELETE FROM rating_events; DELETE FROM driver_ratings`)
	return st
}

func (s *stack) waitReady(url string) {
	s.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		res, err := s.http.Get(url)
		if err == nil {
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.t.Fatalf("service at %s did not become ready in time", url)
}

func (s *stack) token(role, id string) string {
	s.t.Helper()
	body, _ := json.Marshal(map[string]string{"role": role, "id": id})
	res, err := s.http.Post(s.authURL+"/token", "application/json", bytes.NewReader(body))
	if err != nil {
		s.t.Fatal(err)
	}
	defer res.Body.Close()
	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil || out.Token == "" {
		s.t.Fatalf("token response %d: %v", res.StatusCode, err)
	}
	return out.Token
}

func (s *stack) request(role, id, method, url string, payload any, out any) int {
	s.t.Helper()
	var body io.Reader
	if payload != nil {
		b, _ := json.Marshal(payload)
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		s.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+s.token(role, id))
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := s.http.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if out != nil && res.StatusCode < 300 {
		_ = json.Unmarshal(raw, out)
	}
	return res.StatusCode
}

type wsMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func (s *stack) connectWS(role, id string) <-chan wsMessage {
	s.t.Helper()
	wsURL := "ws" + strings.TrimPrefix(s.notifyURL, "http") + fmt.Sprintf("/ws/%s/%s", role, id)
	dialer := websocket.Dialer{
		Subprotocols: []string{"bearer", s.token(role, id)},
	}
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		s.t.Fatalf("dial ws %s/%s: %v", role, id, err)
	}
	ch := make(chan wsMessage, 32)
	s.t.Cleanup(func() { conn.Close() })
	go func() {
		for {
			var msg wsMessage
			if err := conn.ReadJSON(&msg); err != nil {
				close(ch)
				return
			}
			ch <- msg
		}
	}()
	return ch
}

func waitWS(t *testing.T, ch <-chan wsMessage, wantType string, timeout time.Duration) wsMessage {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				t.Fatalf("websocket closed while waiting for %s", wantType)
			}
			if msg.Type == wantType {
				return msg
			}
		case <-deadline:
			t.Fatalf("timed out waiting for websocket event %q", wantType)
		}
	}
}

func TestFullSystemEndToEnd(t *testing.T) {
	st := startStack(t)
	ctx := context.Background()

	// 1. Seed driver-1 (CAR) in Redis and publish a fresh location through Location Gateway
	driverstatus.SetVehicle(ctx, st.rdb, "driver-1", "CAR")
	driverstatus.Set(ctx, st.rdb, "driver-1", driverstatus.Available)
	driverlocation.Update(ctx, st.rdb, "driver-1", 10.776, 106.700, 0)

	riderEvents := st.connectWS("rider", "rider-1")
	driverEvents := st.connectWS("driver", "driver-1")

	code := st.request("driver", "driver-1", http.MethodPost, st.gatewayURL+"/locations", map[string]any{
		"lat":       10.7765,
		"lng":       106.7005,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}, nil)
	if code != http.StatusAccepted {
		t.Fatalf("gateway report = %d, want 202", code)
	}
	waitWS(t, driverEvents, "driver.location.updated", 10*time.Second)

	// 2. Rider gets a signed price quote from Pricing Service
	var quote struct {
		QuoteID string  `json:"quoteId"`
		Price   float64 `json:"price"`
	}
	code = st.request("rider", "rider-1", http.MethodGet,
		st.pricingURL+"/quotes?pickupLat=10.776&pickupLng=106.700&dropoffLat=10.790&dropoffLng=106.710&vehicleType=CAR",
		nil, &quote)
	if code != http.StatusOK || quote.QuoteID == "" || quote.Price <= 0 {
		t.Fatalf("quote = %d %+v, want 200 with signed quoteId", code, quote)
	}

	// 3. Rider creates an order locking the quoted price
	var created struct {
		OrderID string  `json:"orderId"`
		Price   float64 `json:"price"`
	}
	code = st.request("rider", "rider-1", http.MethodPost, st.orderURL+"/orders", map[string]any{
		"riderId":     "rider-1",
		"pickup":      map[string]float64{"lat": 10.776, "lng": 106.700},
		"dropoff":     map[string]float64{"lat": 10.790, "lng": 106.710},
		"vehicleType": "CAR",
		"quoteId":     quote.QuoteID,
	}, &created)
	if code != http.StatusCreated || created.OrderID == "" || created.Price != quote.Price {
		t.Fatalf("create order = %d %+v, want 201 with locked price %v", code, created, quote.Price)
	}

	// 4. Both Rider and Driver receive order.matched over WebSocket
	waitWS(t, riderEvents, "order.matched", 15*time.Second)
	waitWS(t, driverEvents, "order.matched", 15*time.Second)

	// 5. Wait for Trip Service to create the ASSIGNED trip
	var trip struct {
		ID       string `json:"id"`
		DriverID string `json:"driverId"`
		Status   string `json:"status"`
	}
	for i := 0; i < 30; i++ {
		if st.request("rider", "rider-1", http.MethodGet, st.tripURL+"/trips/by-order/"+created.OrderID, nil, &trip) == http.StatusOK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if trip.ID == "" || trip.DriverID != "driver-1" || trip.Status != "ASSIGNED" {
		t.Fatalf("trip = %+v, want ASSIGNED trip for driver-1", trip)
	}

	// 6. Driver confirms pickup -> IN_PROGRESS, then completes -> COMPLETED
	if code := st.request("driver", "driver-1", http.MethodPost, st.tripURL+"/trips/"+trip.ID+"/pickup", nil, nil); code != http.StatusOK {
		t.Fatalf("pickup = %d, want 200", code)
	}
	waitWS(t, riderEvents, "trip.status.changed", 10*time.Second)

	// The driver is still at the pickup point, ~1.9 km from the dropoff: Trip
	// Service refuses to complete from there.
	if code := st.request("driver", "driver-1", http.MethodPost, st.tripURL+"/trips/"+trip.ID+"/complete", nil, nil); code != http.StatusConflict {
		t.Fatalf("complete from the pickup point = %d, want 409", code)
	}
	// Drive to the dropoff (what the Driver App's location reports would do).
	if _, err := driverlocation.Update(context.Background(), st.rdb, "driver-1", 10.7901, 106.7100, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}

	if code := st.request("driver", "driver-1", http.MethodPost, st.tripURL+"/trips/"+trip.ID+"/complete", nil, nil); code != http.StatusOK {
		t.Fatalf("complete = %d, want 200", code)
	}
	waitWS(t, riderEvents, "trip.status.changed", 10*time.Second)

	// 7. Rider rates the trip 5 stars with a 20,000 VND tip
	if code := st.request("rider", "rider-1", http.MethodPost, st.tripURL+"/trips/"+trip.ID+"/rating", map[string]any{
		"riderId":   "rider-1",
		"stars":     5,
		"comment":   "Full-stack E2E test ride",
		"tipAmount": 20000,
	}, nil); code != http.StatusCreated {
		t.Fatalf("rate trip = %d, want 201", code)
	}
	waitWS(t, driverEvents, "trip.rated", 10*time.Second)

	// 8. Verify Rating Service recorded the rating and tip
	var rating struct {
		Average   float64 `json:"average"`
		Count     int     `json:"count"`
		TotalTips int64   `json:"totalTips"`
	}
	for i := 0; i < 30; i++ {
		st.request("driver", "driver-1", http.MethodGet, st.ratingURL+"/drivers/driver-1/rating", nil, &rating)
		if rating.Count == 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if rating.Count != 1 || rating.Average != 5 || rating.TotalTips != 20000 {
		t.Fatalf("driver rating = %+v, want count=1 average=5 totalTips=20000", rating)
	}
}
