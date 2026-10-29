package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/auth/authtest"
	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
)

// These tests need only Postgres (TEST_DATABASE_URL, a throwaway database).

// registry is a Schema Registry that keeps the one schema it is given, which
// is enough to encode and decode trip.status.changed.
func registry(t *testing.T) *eventcodec.Codec {
	t.Helper()
	var mu sync.Mutex
	var schema string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodPost {
			var body struct{ Schema string }
			json.NewDecoder(r.Body).Decode(&body)
			schema = body.Schema
			json.NewEncoder(w).Encode(map[string]int{"id": 1})
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"schema": schema})
	}))
	t.Cleanup(srv.Close)
	return eventcodec.New(srv.URL)
}

type cancelEnv struct {
	db     *pgxpool.Pool
	s      *server
	mux    http.Handler
	issuer *auth.Issuer
}

func newCancelEnv(t *testing.T) *cancelEnv {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	db, err := connectDB(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(ctx, `DELETE FROM outbox; DELETE FROM processed_events; DELETE FROM cancelled_orders; DELETE FROM trips`); err != nil {
		t.Fatal(err)
	}
	// Nothing listens here: the "free the driver" step after a cancel fails and
	// is only logged, which is all these tests need of Redis.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	t.Cleanup(func() { rdb.Close() })

	s := &server{db: db, rdb: rdb, relay: outbox.NewRelay(db, &recordingWriter{}, outboxSource), codec: registry(t)}
	issuer, verifier := authtest.Pair(t, time.Hour)
	return &cancelEnv{db, s, s.routes(verifier), issuer}
}

func (e *cancelEnv) trip(t *testing.T, orderID string, status events.TripStatus) string {
	t.Helper()
	id := uuid.NewString()
	_, err := e.db.Exec(context.Background(),
		`INSERT INTO trips (id, order_id, driver_id, rider_id, status) VALUES ($1, $2, 'driver-1', 'rider-1', $3)`, id, orderID, status)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (e *cancelEnv) cancel(tripID string, role auth.Role, subject, body string) int {
	req := httptest.NewRequest("POST", "/trips/"+tripID+"/cancel", strings.NewReader(body))
	tok, _, _ := e.issuer.Issue(role, subject)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	return rec.Code
}

// statusEvents are the trip.status.changed events queued so far, decoded.
func (e *cancelEnv) statusEvents(t *testing.T) []events.TripStatusChanged {
	t.Helper()
	rows, err := e.db.Query(context.Background(), `SELECT payload FROM outbox WHERE topic = $1 ORDER BY id`, events.TopicTripStatusChanged)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []events.TripStatusChanged
	for rows.Next() {
		var payload []byte
		rows.Scan(&payload)
		var evt events.TripStatusChanged
		if err := e.s.codec.Decode(context.Background(), events.TopicTripStatusChanged, payload, &evt); err != nil {
			t.Fatal(err)
		}
		out = append(out, evt)
	}
	return out
}

func TestTheCancelEventAndRecordNameWhoCancelled(t *testing.T) {
	cases := []struct {
		name    string
		role    auth.Role
		subject string
		want    string
	}{
		{"the rider", auth.Rider, "rider-1", events.CancelledByRider},
		{"the driver", auth.Driver, "driver-1", events.CancelledByDriver},
	}
	for _, c := range cases {
		e := newCancelEnv(t)
		id := e.trip(t, "o1", events.TripStatusAssigned)

		if code := e.cancel(id, c.role, c.subject, `{"reason":"too slow"}`); code != 200 {
			t.Fatalf("%s: status %d, want 200", c.name, code)
		}
		tr, _ := getTripByID(context.Background(), e.db, id)
		if tr.Status != events.TripStatusCancelled || tr.CancelledBy != c.want || tr.CancelReason != "too slow" {
			t.Errorf("%s: trip = %+v, want CANCELLED by %s with the reason", c.name, tr, c.want)
		}
		evs := e.statusEvents(t)
		if len(evs) != 1 || evs[0].Status != events.TripStatusCancelled || evs[0].CancelledBy != c.want || evs[0].Reason != "too slow" {
			t.Errorf("%s: events = %+v, want one CANCELLED event naming %s", c.name, evs, c.want)
		}
	}
}

func TestCancellingNeedsNoBodyAndAnInProgressTripCanBeCancelled(t *testing.T) {
	e := newCancelEnv(t)
	id := e.trip(t, "o1", events.TripStatusInProgress)
	if code := e.cancel(id, auth.Driver, "driver-1", ``); code != 200 {
		t.Fatalf("status %d, want 200", code)
	}
	if evs := e.statusEvents(t); len(evs) != 1 || evs[0].CancelledBy != events.CancelledByDriver || evs[0].Reason != "" {
		t.Errorf("events = %+v", evs)
	}
}

func TestACompletedTripCannotBeCancelled(t *testing.T) {
	e := newCancelEnv(t)
	id := e.trip(t, "o1", events.TripStatusCompleted)

	for _, who := range []struct {
		role    auth.Role
		subject string
	}{{auth.Rider, "rider-1"}, {auth.Driver, "driver-1"}} {
		if code := e.cancel(id, who.role, who.subject, `{}`); code != http.StatusConflict {
			t.Errorf("%s: status %d, want 409", who.role, code)
		}
	}
	if tr, _ := getTripByID(context.Background(), e.db, id); tr.Status != events.TripStatusCompleted {
		t.Errorf("trip is %s, want COMPLETED", tr.Status)
	}
	if evs := e.statusEvents(t); len(evs) != 0 {
		t.Errorf("queued %+v, want nothing", evs)
	}
}

func TestCancellingTwiceIsIdempotentAndBadInputIsRefused(t *testing.T) {
	e := newCancelEnv(t)
	id := e.trip(t, "o1", events.TripStatusAssigned)

	if code := e.cancel(id, auth.Rider, "rider-1", `nope`); code != http.StatusBadRequest {
		t.Errorf("a bad body got %d, want 400", code)
	}
	if code := e.cancel(id, auth.Rider, "rider-1", `{"reason":"`+strings.Repeat("x", 201)+`"}`); code != http.StatusBadRequest {
		t.Errorf("a too-long reason got %d, want 400", code)
	}
	for i := 0; i < 2; i++ {
		if code := e.cancel(id, auth.Rider, "rider-1", `{"reason":"a"}`); code != 200 {
			t.Fatalf("cancel #%d: status %d, want 200", i+1, code)
		}
	}
	// A repeat by the other side doesn't rewrite who cancelled.
	if code := e.cancel(id, auth.Driver, "driver-1", `{"reason":"b"}`); code != 200 {
		t.Errorf("a repeat by the driver got %d, want 200", code)
	}
	if tr, _ := getTripByID(context.Background(), e.db, id); tr.CancelledBy != events.CancelledByRider || tr.CancelReason != "a" {
		t.Errorf("trip = %+v, want the first cancellation kept", tr)
	}
	if evs := e.statusEvents(t); len(evs) != 1 {
		t.Errorf("%d events, want 1", len(evs))
	}
}

func orderCancelled(orderID string) events.OrderCancelled {
	return events.OrderCancelled{
		Envelope: events.Envelope{EventID: uuid.NewString()},
		OrderID:  orderID, RiderID: "rider-1", CancelledBy: events.CancelledByRider, Reason: "changed my mind",
	}
}

func TestAnOrderCancelledEventCancelsItsExistingTrip(t *testing.T) {
	e := newCancelEnv(t)
	ctx := context.Background()
	id := e.trip(t, "o1", events.TripStatusAssigned)

	if err := e.s.handleOrderCancelled(ctx, orderCancelled("o1")); err != nil {
		t.Fatal(err)
	}
	tr, _ := getTripByID(ctx, e.db, id)
	if tr.Status != events.TripStatusCancelled || tr.CancelledBy != events.CancelledByRider || tr.CancelReason != "changed my mind" {
		t.Fatalf("trip = %+v, want CANCELLED by the RIDER", tr)
	}
	evs := e.statusEvents(t)
	if len(evs) != 1 || evs[0].TripID != id || evs[0].Status != events.TripStatusCancelled || evs[0].CancelledBy != events.CancelledByRider || evs[0].DriverID != "driver-1" || evs[0].RiderID != "rider-1" {
		t.Errorf("events = %+v, want one CANCELLED status event for the trip (the driver is released from it)", evs)
	}
}

func TestRedeliveringOrderCancelledChangesNothing(t *testing.T) {
	e := newCancelEnv(t)
	ctx := context.Background()
	e.trip(t, "o1", events.TripStatusAssigned)

	evt := orderCancelled("o1")
	for i := 0; i < 3; i++ {
		if err := e.s.handleOrderCancelled(ctx, evt); err != nil {
			t.Fatal(err)
		}
	}
	if evs := e.statusEvents(t); len(evs) != 1 {
		t.Errorf("%d status events after redelivery, want 1", len(evs))
	}
}

func TestOrderCancelledLeavesFinishedTripsAlone(t *testing.T) {
	for _, status := range []events.TripStatus{events.TripStatusCompleted, events.TripStatusCancelled} {
		e := newCancelEnv(t)
		id := e.trip(t, "o1", status)
		if _, err := e.db.Exec(context.Background(), `UPDATE trips SET cancelled_by = 'DRIVER' WHERE id = $1 AND status = 'CANCELLED'`, id); err != nil {
			t.Fatal(err)
		}

		if err := e.s.handleOrderCancelled(context.Background(), orderCancelled("o1")); err != nil {
			t.Fatal(err)
		}
		tr, _ := getTripByID(context.Background(), e.db, id)
		if tr.Status != status {
			t.Errorf("%s trip became %s", status, tr.Status)
		}
		if status == events.TripStatusCancelled && tr.CancelledBy != events.CancelledByDriver {
			t.Errorf("an earlier cancellation was rewritten to %q", tr.CancelledBy)
		}
		if evs := e.statusEvents(t); len(evs) != 0 {
			t.Errorf("%s: queued %+v, want nothing", status, evs)
		}
	}
}

func TestOrderCancelledBeforeTheTripExistsIsRemembered(t *testing.T) {
	e := newCancelEnv(t)
	ctx := context.Background()

	if err := e.s.handleOrderCancelled(ctx, orderCancelled("o1")); err != nil {
		t.Fatal(err)
	}
	if got, err := orderIsCancelled(ctx, e.db, "o1"); err != nil || !got {
		t.Errorf("orderIsCancelled = %v, %v; want true (so a late order.matched creates no trip)", got, err)
	}
	if got, _ := orderIsCancelled(ctx, e.db, "o2"); got {
		t.Error("an order nobody cancelled is reported cancelled")
	}
	if evs := e.statusEvents(t); len(evs) != 0 {
		t.Errorf("queued %+v, want nothing: there was no trip", evs)
	}
}

// If the cancellation was seen first and a trip got created anyway, the
// redelivered order.cancelled still cancels it.
func TestARedeliveredOrderCancelledCancelsATripThatCameLate(t *testing.T) {
	e := newCancelEnv(t)
	ctx := context.Background()

	if err := e.s.handleOrderCancelled(ctx, orderCancelled("o1")); err != nil { // no trip yet
		t.Fatal(err)
	}
	id := e.trip(t, "o1", events.TripStatusAssigned)
	if err := e.s.handleOrderCancelled(ctx, orderCancelled("o1")); err != nil { // the redelivery
		t.Fatal(err)
	}
	if tr, _ := getTripByID(ctx, e.db, id); tr.Status != events.TripStatusCancelled {
		t.Errorf("trip is %s, want CANCELLED", tr.Status)
	}
}

// Needs Redis too: the driver Matching claimed goes back to the pool.
func TestALateMatchedEventForACancelledOrderCreatesNoTrip(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	if _, err := db.Exec(ctx, `DELETE FROM cancelled_orders; INSERT INTO cancelled_orders (order_id) VALUES ('o1')`); err != nil {
		t.Fatal(err)
	}
	driverstatus.SetVehicle(ctx, rdb, "d1", "CAR")
	driverstatus.Set(ctx, rdb, "d1", driverstatus.Available)
	if r, _ := driverstatus.ClaimForOrder(ctx, rdb, "o1", "d1", "CAR", time.Minute); r != driverstatus.Claimed {
		t.Fatal("setup claim failed")
	}

	if err := handleOrderMatched(ctx, db, rdb, matched("o1", "d1")); err != nil {
		t.Fatal(err)
	}
	if _, err := getTripByOrderID(ctx, db, "o1"); err == nil {
		t.Error("a trip was created for a cancelled order")
	}
	if s, _ := driverstatus.Get(ctx, rdb, "d1"); s != driverstatus.Available {
		t.Errorf("driver is %s, want AVAILABLE", s)
	}
}
