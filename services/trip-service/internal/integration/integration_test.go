package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/auth/authtest"
	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/events"
	tripmetrics "grab-mini/trip-service/internal/adapters/metrics"
	"grab-mini/trip-service/internal/adapters/postgres"
	"grab-mini/trip-service/internal/app"
	"grab-mini/trip-service/internal/domain"
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
	db, err := postgres.Connect(ctx, url, migrationFiles)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(ctx, `DELETE FROM outbox; DELETE FROM processed_events; DELETE FROM cancelled_orders; DELETE FROM trips`); err != nil {
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
		OrderID:  orderID, DriverID: driverID, RiderID: "rider-1", VehicleType: events.VehicleCar,
	}
}

func TestMatchedOrderCreatesATripAndConfirmsTheDriver(t *testing.T) {
	db, rdb := testEnv(t)
	s := newStack(t, db, rdb)
	ctx := context.Background()
	driverstatus.SetVehicle(ctx, rdb, "d1", "CAR")
	driverstatus.Set(ctx, rdb, "d1", driverstatus.Available)
	if r, _ := driverstatus.ClaimForOrder(ctx, rdb, "o1", "d1", "CAR", 1); r != driverstatus.Claimed {
		t.Fatal("setup claim failed")
	}

	if err := s.events.OnOrderMatched(ctx, matched("o1", "d1")); err != nil {
		t.Fatal(err)
	}

	if tr, err := s.store.ByOrderID(ctx, "o1"); err != nil || tr.Status != domain.Assigned || tr.DriverID != "d1" || tr.RiderID != "rider-1" {
		t.Fatalf("trip = %+v, %v; want an ASSIGNED trip for d1 and rider-1 (status events name the rider from this)", tr, err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "d1"); s != driverstatus.OnTrip {
		t.Errorf("driver is %s, want ON_TRIP (this is what ends Matching's lease)", s)
	}
}

func TestReplayingAMatchedEventDoesNotRevertAFinishedTripsDriver(t *testing.T) {
	db, rdb := testEnv(t)
	s := newStack(t, db, rdb)
	ctx := context.Background()
	evt := matched("o1", "d1")
	if err := s.events.OnOrderMatched(ctx, evt); err != nil {
		t.Fatal(err)
	}
	// The trip finished and the driver was released; then the topic is replayed.
	driverstatus.Set(ctx, rdb, "d1", driverstatus.Available)

	if err := s.events.OnOrderMatched(ctx, evt); err != nil {
		t.Fatal(err)
	}
	if err := s.events.OnOrderMatched(ctx, matched("o1", "d1")); err != nil { // same order, new eventId (Matching re-announced)
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

func TestATransitionQueuesItsEventInTheSameTransaction(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	s := newStack(t, db, rdb)
	s.rewire(proximityOff(), nil)
	s.events.OnOrderMatched(ctx, matched("o1", "d1"))
	tr, _ := s.store.ByOrderID(ctx, "o1")

	got, err := s.svc.Pickup(ctx, driverOf("d1"), tr.ID)
	if err != nil || got.Status != domain.InProgress {
		t.Fatalf("pickup = %+v, %v; want IN_PROGRESS", got, err)
	}
	if got, _ := s.store.ByID(ctx, tr.ID); got.Status != domain.InProgress {
		t.Errorf("trip is %s, want IN_PROGRESS", got.Status)
	}
	if n, err := s.relay.Flush(ctx); err != nil || n != 1 {
		t.Fatalf("Flush = %d, %v; want the status event", n, err)
	}
	if m := s.w.sent[0]; m.Topic != events.TopicTripStatusChanged || string(m.Key) != tr.ID {
		t.Errorf("published topic=%s key=%s", m.Topic, m.Key)
	}
}

func TestARejectedTransitionQueuesNothing(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	s := newStack(t, db, rdb)
	s.rewire(proximityOff(), nil)
	s.events.OnOrderMatched(ctx, matched("o1", "d1"))
	tr, _ := s.store.ByOrderID(ctx, "o1")

	// Completing needs IN_PROGRESS; the trip is still ASSIGNED.
	_, err := s.svc.Complete(ctx, driverOf("d1"), tr.ID)
	var cannot *app.CannotTransitionError
	if !errors.As(err, &cannot) {
		t.Fatalf("complete from ASSIGNED = %v; want a refused transition", err)
	}
	if n, _ := s.relay.Flush(ctx); n != 0 || len(s.w.sent) != 0 {
		t.Errorf("a rejected transition queued %d event(s)", n)
	}
}

// staleTrips keeps answering with the first trip it read, as a request does
// that read the trip just before another one rated it.
type staleTrips struct {
	app.TripReader
	seen map[string]domain.Trip
}

func (s *staleTrips) ByID(ctx context.Context, id string) (domain.Trip, error) {
	if t, ok := s.seen[id]; ok {
		return t, nil
	}
	t, err := s.TripReader.ByID(ctx, id)
	if err == nil {
		s.seen[id] = t
	}
	return t, err
}

func TestARatingQueuesItsEventAndASecondOneDoesNot(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	s := newStack(t, db, rdb)
	s.events.OnOrderMatched(ctx, matched("o1", "d1"))
	tr, _ := s.store.ByOrderID(ctx, "o1")
	db.Exec(ctx, `UPDATE trips SET status = 'COMPLETED' WHERE id = $1`, tr.ID)
	// Both submissions saw the trip unrated: the second one is stopped by the
	// UPDATE's own condition, not by the check that precedes it.
	s.wire(domain.DefaultProximity(), nil, &staleTrips{TripReader: s.store, seen: map[string]domain.Trip{}})
	in := app.RateInput{RiderID: "rider-1", Stars: 5, Tip: 20000}

	if _, err := s.svc.RateTrip(ctx, rider1, tr.ID, in); err != nil {
		t.Fatalf("first rating = %v; want it saved", err)
	}
	if _, err := s.svc.RateTrip(ctx, rider1, tr.ID, in); !errors.Is(err, app.ErrAlreadyRated) {
		t.Fatalf("second rating = %v; want ErrAlreadyRated", err)
	}

	var tip int64
	if err := db.QueryRow(ctx, `SELECT tip_amount FROM trips WHERE id = $1`, tr.ID).Scan(&tip); err != nil || tip != 20000 {
		t.Errorf("stored tip = %d, %v; want 20000", tip, err)
	}
	if n, _ := s.relay.Flush(ctx); n != 1 {
		t.Fatalf("%d trip.rated events queued, want exactly 1", n)
	}
	if m := s.w.sent[0]; m.Topic != events.TopicTripRated || string(m.Key) != "d1" {
		t.Errorf("published topic=%s key=%s, want trip.rated keyed by the driver", m.Topic, m.Key)
	}
}

func ended(tripID, orderID, driverID string, status events.TripStatus) events.TripStatusChanged {
	return events.TripStatusChanged{Envelope: events.Envelope{EventID: uuid.NewString()}, TripID: tripID, OrderID: orderID, DriverID: driverID, Status: status}
}

func TestAFinishedTripReleasesItsDriver(t *testing.T) {
	for _, status := range []events.TripStatus{events.TripStatusCompleted, events.TripStatusCancelled} {
		db, rdb := testEnv(t)
		s := newStack(t, db, rdb)
		ctx := context.Background()
		s.events.OnOrderMatched(ctx, matched("o1", "d1")) // driver ON_TRIP
		tr, _ := s.store.ByOrderID(ctx, "o1")
		db.Exec(ctx, `UPDATE trips SET status = $2 WHERE id = $1`, tr.ID, status)

		if err := s.events.OnTripStatusChanged(ctx, ended(tr.ID, "o1", "d1", status)); err != nil {
			t.Fatal(err)
		}
		if s, _ := driverstatus.Get(ctx, rdb, "d1"); s != driverstatus.Available {
			t.Errorf("%s: driver is %s, want AVAILABLE", status, s)
		}
	}
}

func TestEventsThatAreNotTheEndOfATripReleaseNobody(t *testing.T) {
	db, rdb := testEnv(t)
	s := newStack(t, db, rdb)
	ctx := context.Background()
	s.events.OnOrderMatched(ctx, matched("o1", "d1"))
	tr, _ := s.store.ByOrderID(ctx, "o1")

	for _, status := range []events.TripStatus{events.TripStatusAssigned, events.TripStatusInProgress} {
		if err := s.events.OnTripStatusChanged(ctx, ended(tr.ID, "o1", "d1", status)); err != nil {
			t.Fatal(err)
		}
	}
	if s, _ := driverstatus.Get(ctx, rdb, "d1"); s != driverstatus.OnTrip {
		t.Errorf("driver is %s, want ON_TRIP", s)
	}
}

// A late or replayed "completed" for an old trip must not free a driver who is
// already driving someone else.
func TestAnOldTripEndingDoesNotReleaseADriverOnANewTrip(t *testing.T) {
	db, rdb := testEnv(t)
	s := newStack(t, db, rdb)
	ctx := context.Background()
	s.events.OnOrderMatched(ctx, matched("old", "d1"))
	old, _ := s.store.ByOrderID(ctx, "old")
	db.Exec(ctx, `UPDATE trips SET status = 'COMPLETED' WHERE id = $1`, old.ID)
	driverstatus.Set(ctx, rdb, "d1", driverstatus.Available) // released by the request path
	s.events.OnOrderMatched(ctx, matched("new", "d1"))       // next ride: ON_TRIP, trip ASSIGNED

	if err := s.events.OnTripStatusChanged(ctx, ended(old.ID, "old", "d1", events.TripStatusCompleted)); err != nil {
		t.Fatal(err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "d1"); s != driverstatus.OnTrip {
		t.Errorf("driver is %s after an old trip's event, want ON_TRIP (still on the new trip)", s)
	}
}

// Matching has claimed the driver for a new order but no trip exists yet, so
// the database shows nothing active. Only the compare-and-set protects that claim.
func TestAnOldTripEndingDoesNotUndoANewClaim(t *testing.T) {
	db, rdb := testEnv(t)
	s := newStack(t, db, rdb)
	ctx := context.Background()
	s.events.OnOrderMatched(ctx, matched("old", "d1"))
	old, _ := s.store.ByOrderID(ctx, "old")
	db.Exec(ctx, `UPDATE trips SET status = 'COMPLETED' WHERE id = $1`, old.ID)
	driverstatus.SetVehicle(ctx, rdb, "d1", "CAR")
	driverstatus.Set(ctx, rdb, "d1", driverstatus.Available)
	if r, _ := driverstatus.ClaimForOrder(ctx, rdb, "new", "d1", "CAR", time.Minute); r != driverstatus.Claimed {
		t.Fatal("setup claim failed")
	}

	if err := s.events.OnTripStatusChanged(ctx, ended(old.ID, "old", "d1", events.TripStatusCompleted)); err != nil {
		t.Fatal(err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "d1"); s != driverstatus.Assigned {
		t.Errorf("driver is %s, want ASSIGNED: the release undid Matching's new claim", s)
	}
}

func TestReleasingTwiceIsHarmless(t *testing.T) {
	db, rdb := testEnv(t)
	s := newStack(t, db, rdb)
	ctx := context.Background()
	s.events.OnOrderMatched(ctx, matched("o1", "d1"))
	tr, _ := s.store.ByOrderID(ctx, "o1")
	db.Exec(ctx, `UPDATE trips SET status = 'COMPLETED' WHERE id = $1`, tr.ID)
	evt := ended(tr.ID, "o1", "d1", events.TripStatusCompleted)

	for i := 0; i < 2; i++ {
		if err := s.events.OnTripStatusChanged(ctx, evt); err != nil {
			t.Fatal(err)
		}
	}
	if s, _ := driverstatus.Get(ctx, rdb, "d1"); s != driverstatus.Available {
		t.Errorf("driver is %s, want AVAILABLE", s)
	}
}

// The API as a client sees it: who may do what to a trip.
func TestTripAccessFollowsTheTokenAndWhoIsPartyToTheTrip(t *testing.T) {
	db, rdb := testEnv(t)
	s := newStack(t, db, rdb)
	ctx := context.Background()
	s.events.OnOrderMatched(ctx, matched("o1", "driver-1")) // rider-1's trip, ASSIGNED
	tr, _ := s.store.ByOrderID(ctx, "o1")
	// the transitions below are refused before they would encode

	issuer, verifier := authtest.Pair(t, time.Hour)
	mux := s.api.Routes(verifier)
	do := func(method, path string, role auth.Role, subject, body string) int {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if role != "" {
			tok, _, _ := issuer.Issue(role, subject)
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Code
	}

	id := tr.ID
	cases := []struct {
		name          string
		method, path  string
		role          auth.Role
		subject, body string
		want          int
	}{
		{"no token", "GET", "/trips/" + id, "", "", "", 401},
		{"its rider reads it", "GET", "/trips/" + id, auth.Rider, "rider-1", "", 200},
		{"its driver reads it", "GET", "/trips/" + id, auth.Driver, "driver-1", "", 200},
		{"another rider can't", "GET", "/trips/" + id, auth.Rider, "rider-2", "", 404},
		{"another driver can't", "GET", "/trips/" + id, auth.Driver, "driver-2", "", 404},
		{"by order, the rider", "GET", "/trips/by-order/o1", auth.Rider, "rider-1", "", 200},
		{"by order, a stranger", "GET", "/trips/by-order/o1", auth.Rider, "rider-2", "", 404},
		{"a driver's own active trip", "GET", "/drivers/driver-1/active-trip", auth.Driver, "driver-1", "", 200},
		{"another driver's active trip", "GET", "/drivers/driver-1/active-trip", auth.Driver, "driver-2", "", 403},
		{"a rider can't ask for an active trip", "GET", "/drivers/driver-1/active-trip", auth.Rider, "rider-1", "", 403},
		{"a rider can't pick up", "POST", "/trips/" + id + "/pickup", auth.Rider, "rider-1", "", 403},
		{"another driver can't pick up", "POST", "/trips/" + id + "/pickup", auth.Driver, "driver-2", "", 404},
		{"another driver can't complete", "POST", "/trips/" + id + "/complete", auth.Driver, "driver-2", "", 404},
		{"a stranger can't cancel", "POST", "/trips/" + id + "/cancel", auth.Rider, "rider-2", "", 404},
		{"a driver can't rate", "POST", "/trips/" + id + "/rating", auth.Driver, "driver-1", `{"stars":5}`, 403},
		{"another rider can't rate", "POST", "/trips/" + id + "/rating", auth.Rider, "rider-2", `{"stars":5}`, 404},
		{"the rider names someone else", "POST", "/trips/" + id + "/rating", auth.Rider, "rider-1", `{"riderId":"rider-2","stars":5}`, 403},
	}
	for _, c := range cases {
		if got := do(c.method, c.path, c.role, c.subject, c.body); got != c.want {
			t.Errorf("%s: status %d, want %d", c.name, got, c.want)
		}
	}

	// And nothing above changed the trip.
	if got, _ := s.store.ByID(ctx, id); got.Status != domain.Assigned {
		t.Errorf("a refused request moved the trip to %s", got.Status)
	}
}

// The rider on the trip may rate it once it is completed, whatever the body says about who they are.
func TestTheTripsRiderCanRateItAndTheEventNamesThem(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	s := newStack(t, db, rdb)
	s.events.OnOrderMatched(ctx, matched("o1", "driver-1"))
	tr, _ := s.store.ByOrderID(ctx, "o1")
	db.Exec(ctx, `UPDATE trips SET status = 'COMPLETED' WHERE id = $1`, tr.ID)

	if _, err := s.svc.RateTrip(ctx, rider1, tr.ID, app.RateInput{Stars: 5}); err != nil {
		t.Fatalf("the trip's rider could not rate it: %v", err)
	}
	if n, _ := s.relay.Flush(ctx); n != 1 {
		t.Fatalf("%d events queued", n)
	}
	var evt events.TripRated
	if err := s.codec.Decode(ctx, events.TopicTripRated, s.w.sent[0].Value, &evt); err != nil {
		t.Fatal(err)
	}
	if evt.RiderID != "rider-1" || evt.DriverID != "driver-1" || evt.TripID != tr.ID || evt.OrderID != "o1" || evt.Stars != 5 || evt.Currency != "VND" {
		t.Errorf("trip.rated = %+v, want it to name the rider from the token", evt)
	}
}

func TestOnlyCommittedTransitionsAreCounted(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	s := newStack(t, db, rdb)
	s.rewire(proximityOff(), nil)
	s.events.OnOrderMatched(ctx, matched("o1", "d1"))
	tr, _ := s.store.ByOrderID(ctx, "o1")
	counted := func(status string) float64 {
		return testutil.ToFloat64(tripmetrics.Transitions.WithLabelValues(status))
	}
	moved, completed := counted("IN_PROGRESS"), counted("COMPLETED")

	// Refused (wrong starting status): not counted.
	if _, err := s.svc.Complete(ctx, driverOf("d1"), tr.ID); err == nil {
		t.Fatal("completing an ASSIGNED trip succeeded")
	}
	if counted("COMPLETED") != completed {
		t.Error("a refused transition was counted")
	}
	if _, err := s.svc.Pickup(ctx, driverOf("d1"), tr.ID); err != nil {
		t.Fatal(err)
	}
	if counted("IN_PROGRESS")-moved != 1 {
		t.Errorf("a committed transition moved the counter by %v, want 1", counted("IN_PROGRESS")-moved)
	}
	// Repeating it is a no-op: not counted again.
	if _, err := s.svc.Pickup(ctx, driverOf("d1"), tr.ID); err != nil {
		t.Fatal(err)
	}
	if counted("IN_PROGRESS")-moved != 1 {
		t.Errorf("a repeated pickup was counted: %v", counted("IN_PROGRESS")-moved)
	}
}

func TestCompletedAtIsSetOnceByTheFirstCompletionAndServedByTheAPI(t *testing.T) {
	db, rdb := testEnv(t)
	s := newStack(t, db, rdb)
	ctx := context.Background()
	s.events.OnOrderMatched(ctx, matched("o1", "driver-1"))
	tr, _ := s.store.ByOrderID(ctx, "o1")
	if tr.CompletedAt != nil {
		t.Fatalf("a new trip has completedAt %v", tr.CompletedAt)
	}
	s.rewire(proximityOff(), nil)

	if _, err := s.svc.Pickup(ctx, driver1, tr.ID); err != nil {
		t.Fatalf("pickup = %v", err)
	}
	if got, _ := s.store.ByID(ctx, tr.ID); got.CompletedAt != nil {
		t.Errorf("an in-progress trip has completedAt %v", got.CompletedAt)
	}
	if _, err := s.svc.Complete(ctx, driver1, tr.ID); err != nil {
		t.Fatalf("complete = %v", err)
	}
	first, _ := s.store.ByID(ctx, tr.ID)
	if first.CompletedAt == nil || time.Since(*first.CompletedAt) > time.Minute {
		t.Fatalf("completedAt = %v, want about now", first.CompletedAt)
	}

	// Completing again is a no-op: no transition, and the time does not move.
	time.Sleep(20 * time.Millisecond)
	if _, err := s.svc.Complete(ctx, driver1, tr.ID); err != nil {
		t.Fatalf("second complete = %v; want the no-op success", err)
	}
	if n, _ := s.relay.Flush(ctx); n != 2 {
		t.Fatalf("%d events queued for a pickup and a complete (the repeated complete queues none), want 2", n)
	}
	second, _ := s.store.ByID(ctx, tr.ID)
	if !second.CompletedAt.Equal(*first.CompletedAt) {
		t.Errorf("completedAt moved from %v to %v", first.CompletedAt, second.CompletedAt)
	}

	// The HTTP endpoints serve it, and the repeat call answers with the same time.
	issuer, verifier := authtest.Pair(t, time.Hour)
	mux := s.api.Routes(verifier)
	call := func(method, path string, role auth.Role, subject string) map[string]any {
		req := httptest.NewRequest(method, path, nil)
		tok, _, _ := issuer.Issue(role, subject)
		req.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("%s %s = %d", method, path, rec.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	want := first.CompletedAt.UTC().Format(time.RFC3339Nano)
	for name, body := range map[string]map[string]any{
		"by id":       call("GET", "/trips/"+tr.ID, auth.Rider, "rider-1"),
		"by order":    call("GET", "/trips/by-order/o1", auth.Driver, "driver-1"),
		"repeat done": call("POST", "/trips/"+tr.ID+"/complete", auth.Driver, "driver-1"),
	} {
		if body["completedAt"] != want {
			t.Errorf("%s: completedAt = %v, want %s", name, body["completedAt"], want)
		}
	}
}

func TestACancelledTripHasNoCompletedAt(t *testing.T) {
	db, rdb := testEnv(t)
	ctx := context.Background()
	s := newStack(t, db, rdb)
	s.events.OnOrderMatched(ctx, matched("o1", "d1"))
	tr, _ := s.store.ByOrderID(ctx, "o1")
	if _, err := s.svc.Cancel(ctx, rider1, tr.ID, ""); err != nil {
		t.Fatalf("cancel = %v", err)
	}
	if got, _ := s.store.ByID(ctx, tr.ID); got.CompletedAt != nil {
		t.Errorf("cancelled trip has completedAt %v", got.CompletedAt)
	}
}

func TestClosingAReceiptIsRiderOnlyCompletedOnlyAndKeepsTheFirstTime(t *testing.T) {
	db, rdb := testEnv(t)
	s := newStack(t, db, rdb)
	ctx := context.Background()
	s.events.OnOrderMatched(ctx, matched("o1", "driver-1"))
	tr, _ := s.store.ByOrderID(ctx, "o1")
	issuer, verifier := authtest.Pair(t, time.Hour)
	mux := s.api.Routes(verifier)
	call := func(path string, role auth.Role, subject string) (int, map[string]any) {
		req := httptest.NewRequest("POST", path, nil)
		tok, _, _ := issuer.Issue(role, subject)
		req.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		var body map[string]any
		json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body
	}
	path := "/trips/" + tr.ID + "/receipt/close"

	// Not finished yet: 409 not_completed, nothing stored.
	if code, body := call(path, auth.Rider, "rider-1"); code != 409 || body["error"] != "not_completed" {
		t.Fatalf("close on ASSIGNED = %d %v", code, body)
	}
	db.Exec(ctx, `UPDATE trips SET status = 'COMPLETED', completed_at = now() WHERE id = $1`, tr.ID)

	// Anyone but the trip's rider gets the same 404 as GET /trips/{id}.
	if code, _ := call(path, auth.Rider, "rider-2"); code != 404 {
		t.Errorf("other rider: close = %d, want 404", code)
	}
	if code, _ := call(path, auth.Driver, "driver-1"); code != 403 { // rider-only route, like rating
		t.Errorf("driver token: close = %d, want 403", code)
	}
	if got, _ := s.store.ByID(ctx, tr.ID); got.ReceiptClosedAt != nil {
		t.Fatalf("refused calls still closed the receipt: %v", got.ReceiptClosedAt)
	}

	code, first := call(path, auth.Rider, "rider-1")
	if code != 200 {
		t.Fatalf("close = %d", code)
	}
	stored, _ := s.store.ByID(ctx, tr.ID)
	want := stored.ReceiptClosedAt.UTC().Format(time.RFC3339Nano)
	if stored.ReceiptClosedAt == nil || first["receiptClosedAt"] != want {
		t.Fatalf("closed at %v, response %v", stored.ReceiptClosedAt, first["receiptClosedAt"])
	}

	time.Sleep(20 * time.Millisecond)
	if code, again := call(path, auth.Rider, "rider-1"); code != 200 || again["receiptClosedAt"] != want {
		t.Errorf("second close = %d %v, want 200 %s", code, again, want)
	}
	if got, _ := s.store.ByID(ctx, tr.ID); !got.ReceiptClosedAt.Equal(*stored.ReceiptClosedAt) {
		t.Errorf("receiptClosedAt moved from %v to %v", stored.ReceiptClosedAt, got.ReceiptClosedAt)
	}
}

func TestRatingATripClosesItsReceiptWithoutMovingAnEarlierClose(t *testing.T) {
	db, rdb := testEnv(t)
	s := newStack(t, db, rdb)
	ctx := context.Background()
	rate := func(orderID string, preclose bool) (domain.Trip, domain.Trip) {
		s.events.OnOrderMatched(ctx, matched(orderID, "d1"))
		tr, _ := s.store.ByOrderID(ctx, orderID)
		db.Exec(ctx, `UPDATE trips SET status = 'COMPLETED' WHERE id = $1`, tr.ID)
		if preclose {
			db.Exec(ctx, `UPDATE trips SET receipt_closed_at = now() - interval '1 hour' WHERE id = $1`, tr.ID)
		}
		before, _ := s.store.ByID(ctx, tr.ID)
		if _, err := s.svc.RateTrip(ctx, rider1, tr.ID, app.RateInput{RiderID: "rider-1", Stars: 4}); err != nil {
			t.Fatalf("rating = %v", err)
		}
		after, _ := s.store.ByID(ctx, tr.ID)
		return before, after
	}

	before, after := rate("o1", false)
	if before.ReceiptClosedAt != nil {
		t.Fatalf("receipt closed before rating: %v", before.ReceiptClosedAt)
	}
	if after.ReceiptClosedAt == nil || time.Since(*after.ReceiptClosedAt) > time.Minute {
		t.Errorf("after rating receiptClosedAt = %v, want about now", after.ReceiptClosedAt)
	}

	before, after = rate("o2", true)
	if !after.ReceiptClosedAt.Equal(*before.ReceiptClosedAt) {
		t.Errorf("rating moved receiptClosedAt from %v to %v", before.ReceiptClosedAt, after.ReceiptClosedAt)
	}
}
