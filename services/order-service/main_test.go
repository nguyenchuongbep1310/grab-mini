package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
)

// Validation happens before the database or Kafka is touched, so a server
// with no dependencies is enough to test what gets rejected.
func post(body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	(&server{}).handleCreateOrder(rec, req)
	return rec
}

func TestCreateOrderRejectsBadInput(t *testing.T) {
	const where = `"pickup":{"lat":10.776,"lng":106.7},"dropoff":{"lat":10.79,"lng":106.71}`
	cases := map[string]string{
		"not JSON":              `nope`,
		"missing rider":         `{` + where + `}`,
		"missing pickup":        `{"riderId":"r","dropoff":{"lat":10.79,"lng":106.71}}`,
		"latitude out of range": `{"riderId":"r","pickup":{"lat":91,"lng":0},"dropoff":{"lat":10.79,"lng":106.71}}`,
		"unknown vehicle type":  `{"riderId":"r",` + where + `,"vehicleType":"TRUCK"}`,
		"lowercase vehicle":     `{"riderId":"r",` + where + `,"vehicleType":"car"}`,
	}
	for name, body := range cases {
		if rec := post(body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
	}
}

func TestVehicleTypeValidation(t *testing.T) {
	rec := post(`{"riderId":"r","pickup":{"lat":10.776,"lng":106.7},"dropoff":{"lat":10.79,"lng":106.71},"vehicleType":"TRUCK"}`)
	if !strings.Contains(rec.Body.String(), "CAR or BIKE") {
		t.Errorf("body = %q, want a message naming the allowed types", rec.Body.String())
	}
}

func TestAnOutcomeForANonUUIDOrderIsPermanent(t *testing.T) {
	// The guard runs before the database, so no connection is needed.
	err := setOutcome(context.Background(), nil, "not-a-uuid", "MATCHED")
	var bad *badOutcome
	if !errors.As(err, &bad) || !bad.Permanent() {
		t.Errorf("err = %v, want a permanent badOutcome (retrying can't fix it)", err)
	}
}

// The tests below need Postgres: set TEST_DATABASE_URL (a throwaway database).
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	db, err := connectDB(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(context.Background(), `DELETE FROM outbox; DELETE FROM orders`); err != nil {
		t.Fatal(err)
	}
	return db
}

type recordingWriter struct{ sent []kafka.Message }

func (w *recordingWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.sent = append(w.sent, msgs...)
	return nil
}

func TestAnOrderAndItsEventAreSavedTogether(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	w := &recordingWriter{}
	s := &server{db: db, relay: outbox.NewRelay(db, w, outboxSource)}
	id := uuid.NewString()
	req := createOrderRequest{
		RiderID: "rider-1", Pickup: &events.LatLng{Lat: 1, Lng: 2}, Dropoff: &events.LatLng{Lat: 3, Lng: 4},
		VehicleType: events.VehicleCar,
	}

	if err := s.insertOrder(ctx, id, req, []byte("avro-bytes")); err != nil {
		t.Fatal(err)
	}
	if o, err := queryOrder(ctx, db, "id = $1", id); err != nil || o.Status != "PENDING" {
		t.Fatalf("order = %v, %v; want a PENDING order", o, err)
	}

	if n, err := s.relay.Flush(ctx); err != nil || n != 1 {
		t.Fatalf("Flush = %d, %v; want the order.created event", n, err)
	}
	m := w.sent[0]
	if m.Topic != events.TopicOrderCreated || string(m.Key) != id || string(m.Value) != "avro-bytes" {
		t.Errorf("published topic=%s key=%s value=%s", m.Topic, m.Key, m.Value)
	}
}

func TestNoOrderIsLeftBehindWhenTheEventCannotBeQueued(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	// Break the outbox so the second statement of the transaction fails.
	if _, err := db.Exec(ctx, `ALTER TABLE outbox RENAME TO outbox_off`); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(ctx, `ALTER TABLE outbox_off RENAME TO outbox`)

	s := &server{db: db, relay: outbox.NewRelay(db, &recordingWriter{}, outboxSource)}
	id := uuid.NewString()
	req := createOrderRequest{
		RiderID: "rider-1", Pickup: &events.LatLng{}, Dropoff: &events.LatLng{}, VehicleType: events.VehicleCar,
	}
	if err := s.insertOrder(ctx, id, req, []byte("x")); err == nil {
		t.Fatal("insertOrder succeeded without being able to queue the event")
	}
	if _, err := queryOrder(ctx, db, "id = $1", id); err == nil {
		t.Error("the order row survived although its event could not be queued")
	}
}
