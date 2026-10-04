package main

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
)

// These tests need Postgres and Redis: set TEST_DATABASE_URL and
// TEST_REDIS_ADDR to throwaway instances (they delete rows and FLUSH Redis).
func testEnv(t *testing.T) (*pgxpool.Pool, *redis.Client) {
	t.Helper()
	url, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_REDIS_ADDR")
	if url == "" || addr == "" {
		t.Skip("TEST_DATABASE_URL and TEST_REDIS_ADDR not set")
	}
	ctx := context.Background()
	db, err := connectDB(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(ctx, `DELETE FROM outbox; DELETE FROM processed_events; DELETE FROM trips`); err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	return db, rdb
}

type recordingWriter struct{ sent []kafka.Message }

func (w *recordingWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.sent = append(w.sent, msgs...)
	return nil
}

func matched(orderID, driverID string) events.OrderMatched {
	return events.OrderMatched{
		Envelope: events.Envelope{EventID: uuid.NewString()},
		OrderID:  orderID, DriverID: driverID, VehicleType: events.VehicleCar,
	}
}

func TestMatchedOrderCreatesATripAndConfirmsTheDriver(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	driverstatus.SetVehicle(ctx, rdb, "d1", "CAR")
	driverstatus.Set(ctx, rdb, "d1", driverstatus.Available)
	if r, _ := driverstatus.ClaimForOrder(ctx, rdb, "o1", "d1", "CAR", 1); r != driverstatus.Claimed {
		t.Fatal("setup claim failed")
	}

	if err := handleOrderMatched(ctx, db, rdb, matched("o1", "d1")); err != nil {
		t.Fatal(err)
	}

	if tr, err := getTripByOrderID(ctx, db, "o1"); err != nil || tr.Status != events.TripStatusAssigned || tr.DriverID != "d1" {
		t.Fatalf("trip = %+v, %v; want an ASSIGNED trip for d1", tr, err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "d1"); s != driverstatus.OnTrip {
		t.Errorf("driver is %s, want ON_TRIP (this is what ends Matching's lease)", s)
	}
}

func TestReplayingAMatchedEventDoesNotRevertAFinishedTripsDriver(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	evt := matched("o1", "d1")
	if err := handleOrderMatched(ctx, db, rdb, evt); err != nil {
		t.Fatal(err)
	}
	// The trip finished and the driver was released; then the topic is replayed.
	driverstatus.Set(ctx, rdb, "d1", driverstatus.Available)

	if err := handleOrderMatched(ctx, db, rdb, evt); err != nil {
		t.Fatal(err)
	}
	if err := handleOrderMatched(ctx, db, rdb, matched("o1", "d1")); err != nil { // same order, new eventId (Matching re-announced)
		t.Fatal(err)
	}

	if s, _ := driverstatus.Get(ctx, rdb, "d1"); s != driverstatus.Available {
		t.Errorf("driver is %s after a replay, want AVAILABLE", s)
	}
	var n int
	db.QueryRow(ctx, `SELECT count(*) FROM trips WHERE order_id = 'o1'`).Scan(&n)
	if n != 1 {
		t.Errorf("%d trips for one order, want 1", n)
	}
}

func newServer(db *pgxpool.Pool, rdb *redis.Client) (*server, *recordingWriter) {
	w := &recordingWriter{}
	return &server{db: db, rdb: rdb, relay: outbox.NewRelay(db, w, outboxSource)}, w
}

func TestATransitionQueuesItsEventInTheSameTransaction(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	handleOrderMatched(ctx, db, rdb, matched("o1", "d1"))
	tr, _ := getTripByOrderID(ctx, db, "o1")
	s, w := newServer(db, rdb)

	ok, err := s.transitionWithEvent(ctx, tr.ID, []events.TripStatus{events.TripStatusAssigned}, events.TripStatusInProgress, startedAt, []byte(tr.ID), []byte("evt"))
	if err != nil || !ok {
		t.Fatalf("transition = %v, %v; want true", ok, err)
	}
	if got, _ := getTripByID(ctx, db, tr.ID); got.Status != events.TripStatusInProgress {
		t.Errorf("trip is %s, want IN_PROGRESS", got.Status)
	}
	if n, err := s.relay.Flush(ctx); err != nil || n != 1 {
		t.Fatalf("Flush = %d, %v; want the status event", n, err)
	}
	if m := w.sent[0]; m.Topic != events.TopicTripStatusChanged || string(m.Key) != tr.ID {
		t.Errorf("published topic=%s key=%s", m.Topic, m.Key)
	}
}

func TestARejectedTransitionQueuesNothing(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	handleOrderMatched(ctx, db, rdb, matched("o1", "d1"))
	tr, _ := getTripByOrderID(ctx, db, "o1")
	s, w := newServer(db, rdb)

	// Completing needs IN_PROGRESS; the trip is still ASSIGNED.
	ok, err := s.transitionWithEvent(ctx, tr.ID, []events.TripStatus{events.TripStatusInProgress}, events.TripStatusCompleted, completedAt, []byte(tr.ID), []byte("evt"))
	if err != nil || ok {
		t.Fatalf("transition = %v, %v; want false, nil", ok, err)
	}
	if n, _ := s.relay.Flush(ctx); n != 0 || len(w.sent) != 0 {
		t.Errorf("a rejected transition queued %d event(s)", n)
	}
}

func TestARatingQueuesItsEventAndASecondOneDoesNot(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	handleOrderMatched(ctx, db, rdb, matched("o1", "d1"))
	tr, _ := getTripByOrderID(ctx, db, "o1")
	db.Exec(ctx, `UPDATE trips SET status = 'COMPLETED' WHERE id = $1`, tr.ID)
	s, w := newServer(db, rdb)
	req := rateRequest{RiderID: "r1", Stars: 5}

	if ok, err := s.rateWithEvent(ctx, tr, req, []byte("rated")); err != nil || !ok {
		t.Fatalf("first rating = %v, %v; want true", ok, err)
	}
	if ok, err := s.rateWithEvent(ctx, tr, req, []byte("rated again")); err != nil || ok {
		t.Fatalf("second rating = %v, %v; want false, nil", ok, err)
	}

	if n, _ := s.relay.Flush(ctx); n != 1 {
		t.Fatalf("%d trip.rated events queued, want exactly 1", n)
	}
	if m := w.sent[0]; m.Topic != events.TopicTripRated || string(m.Key) != "d1" {
		t.Errorf("published topic=%s key=%s, want trip.rated keyed by the driver", m.Topic, m.Key)
	}
}
