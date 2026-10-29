package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/auth/authtest"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
)

// fakeRegistry answers the one Schema Registry call Encode makes.
func fakeRegistry(t *testing.T) *eventcodec.Codec {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int{"id": 1})
	}))
	t.Cleanup(srv.Close)
	return eventcodec.New(srv.URL)
}

type cancelEnv struct {
	s      *server
	db     *pgxpool.Pool
	mux    http.Handler
	issuer *auth.Issuer
}

func newCancelEnv(t *testing.T) *cancelEnv {
	t.Helper()
	db := testDB(t)
	s := &server{db: db, relay: outbox.NewRelay(db, &recordingWriter{}, outboxSource), codec: fakeRegistry(t)}
	issuer, verifier := authtest.Pair(t, time.Hour)
	return &cancelEnv{s, db, s.routes(verifier), issuer}
}

// order saves an order for rider-1 in the given status.
func (e *cancelEnv) order(t *testing.T, status string) string {
	t.Helper()
	id := uuid.NewString()
	err := e.s.insertOrder(context.Background(), id, createOrderRequest{
		RiderID: "rider-1", Pickup: &events.LatLng{Lat: 1, Lng: 2}, Dropoff: &events.LatLng{Lat: 3, Lng: 4}, VehicleType: events.VehicleCar,
	}, []byte("created"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.Exec(context.Background(), `DELETE FROM outbox`); err != nil { // forget order.created
		t.Fatal(err)
	}
	if status != "PENDING" {
		if _, err := e.db.Exec(context.Background(), `UPDATE orders SET status = $2 WHERE id = $1`, id, status); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func (e *cancelEnv) cancel(id string, role auth.Role, subject, body string) (int, map[string]any) {
	req := httptest.NewRequest("POST", "/orders/"+id+"/cancel", strings.NewReader(body))
	tok, _, _ := e.issuer.Issue(role, subject)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, req)
	var out map[string]any
	json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func (e *cancelEnv) queued(t *testing.T) []string {
	t.Helper()
	rows, err := e.db.Query(context.Background(), `SELECT topic FROM outbox ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var topics []string
	for rows.Next() {
		var topic string
		rows.Scan(&topic)
		topics = append(topics, topic)
	}
	return topics
}

func TestARiderCancelsAPendingOrder(t *testing.T) {
	e := newCancelEnv(t)
	id := e.order(t, "PENDING")

	code, out := e.cancel(id, auth.Rider, "rider-1", `{"reason":"changed my mind"}`)
	if code != 200 || out["status"] != "CANCELLED" || out["cancelledBy"] != "RIDER" || out["cancelReason"] != "changed my mind" || out["cancelledAt"] == nil {
		t.Fatalf("cancel = %d %v; want 200 and a CANCELLED order cancelled by RIDER", code, out)
	}
	if topics := e.queued(t); len(topics) != 1 || topics[0] != events.TopicOrderCancelled {
		t.Errorf("queued %v, want exactly one order.cancelled", topics)
	}
	if o, err := queryOrder(context.Background(), e.db, "id = $1", id); err != nil || o.Status != "CANCELLED" || o.CancelledBy != "RIDER" {
		t.Errorf("stored order = %+v, %v", o, err)
	}
}

func TestCancellingNeedsNoBody(t *testing.T) {
	e := newCancelEnv(t)
	id := e.order(t, "PENDING")
	if code, out := e.cancel(id, auth.Rider, "rider-1", ``); code != 200 || out["status"] != "CANCELLED" {
		t.Errorf("cancel with no body = %d %v, want 200", code, out)
	}
}

func TestCancellingAnOrderTwiceIsAnIdempotentSuccess(t *testing.T) {
	e := newCancelEnv(t)
	id := e.order(t, "PENDING")

	for i := 0; i < 3; i++ {
		if code, out := e.cancel(id, auth.Rider, "rider-1", `{"reason":"x"}`); code != 200 || out["status"] != "CANCELLED" {
			t.Fatalf("cancel #%d = %d %v, want 200 CANCELLED", i+1, code, out)
		}
	}
	if topics := e.queued(t); len(topics) != 1 {
		t.Errorf("queued %v: a repeat must not announce the cancellation again", topics)
	}
}

func TestAMatchedOrderCannotBeCancelledHere(t *testing.T) {
	e := newCancelEnv(t)
	id := e.order(t, "MATCHED")

	code, out := e.cancel(id, auth.Rider, "rider-1", `{}`)
	if code != http.StatusConflict || out["status"] != "MATCHED" || !strings.Contains(out["error"].(string), "cancel the trip") {
		t.Errorf("cancel = %d %v; want 409 pointing the client at the trip", code, out)
	}
	if o, _ := queryOrder(context.Background(), e.db, "id = $1", id); o.Status != "MATCHED" {
		t.Errorf("order is %s, want it left MATCHED", o.Status)
	}
	if topics := e.queued(t); len(topics) != 0 {
		t.Errorf("queued %v, want nothing", topics)
	}
}

func TestAnOrderWithNoDriverCannotBeCancelled(t *testing.T) {
	e := newCancelEnv(t)
	id := e.order(t, "NO_DRIVER")
	if code, _ := e.cancel(id, auth.Rider, "rider-1", `{}`); code != http.StatusConflict {
		t.Errorf("status %d, want 409", code)
	}
}

func TestOnlyTheOrdersRiderMayCancelIt(t *testing.T) {
	e := newCancelEnv(t)
	id := e.order(t, "PENDING")

	if code, _ := e.cancel(id, auth.Rider, "rider-2", `{}`); code != http.StatusNotFound {
		t.Errorf("a stranger got %d, want 404 (it doesn't exist for them)", code)
	}
	if code, _ := e.cancel(id, auth.Driver, "driver-1", `{}`); code != http.StatusForbidden {
		t.Errorf("a driver got %d, want 403", code)
	}
	if code, _ := e.cancel("not-a-uuid", auth.Rider, "rider-1", `{}`); code != http.StatusNotFound {
		t.Errorf("a malformed id got %d, want 404", code)
	}
	if code, _ := e.cancel(uuid.NewString(), auth.Rider, "rider-1", `{}`); code != http.StatusNotFound {
		t.Errorf("an unknown order got %d, want 404", code)
	}
	if code, _ := e.cancel(id, auth.Rider, "rider-1", `nope`); code != http.StatusBadRequest {
		t.Errorf("a bad body got %d, want 400", code)
	}
	if code, _ := e.cancel(id, auth.Rider, "rider-1", `{"reason":"`+strings.Repeat("x", 201)+`"}`); code != http.StatusBadRequest {
		t.Errorf("a too-long reason got %d, want 400", code)
	}
	if o, _ := queryOrder(context.Background(), e.db, "id = $1", id); o.Status != "PENDING" {
		t.Errorf("a refused request moved the order to %s", o.Status)
	}
	if topics := e.queued(t); len(topics) != 0 {
		t.Errorf("queued %v, want nothing", topics)
	}
}

// Matching reports back while the rider taps cancel. Whichever wins, the order
// ends in exactly one of two consistent states: CANCELLED with its event, or
// MATCHED with no event (and the rider told to cancel the trip).
func TestMatchAndCancelRacingLeaveOneConsistentOutcome(t *testing.T) {
	// One rider fires these requests back to back on purpose; the per-caller rate limit
	// (tested on its own) would answer 429 and hide what this test is about.
	t.Setenv(auth.RateLimitScaleEnv, "0")
	e := newCancelEnv(t)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		id := e.order(t, "PENDING")
		var wg sync.WaitGroup
		var code int
		start := make(chan struct{})
		wg.Add(2)
		go func() { defer wg.Done(); <-start; code, _ = e.cancel(id, auth.Rider, "rider-1", `{}`) }()
		go func() { defer wg.Done(); <-start; setOutcome(ctx, e.db, id, "MATCHED") }()
		close(start)
		wg.Wait()

		o, _ := queryOrder(ctx, e.db, "id = $1", id)
		topics := e.queued(t)
		switch o.Status {
		case "CANCELLED":
			if code != 200 || len(topics) != 1 {
				t.Fatalf("round %d: CANCELLED but code=%d events=%v", i, code, topics)
			}
		case "MATCHED":
			if code != http.StatusConflict || len(topics) != 0 {
				t.Fatalf("round %d: MATCHED but code=%d events=%v", i, code, topics)
			}
		default:
			t.Fatalf("round %d: order ended %s", i, o.Status)
		}
		e.db.Exec(ctx, `DELETE FROM outbox`)
	}
}

func TestAMatchedOutcomeNeverOverwritesACancelledOrder(t *testing.T) {
	e := newCancelEnv(t)
	ctx := context.Background()
	id := e.order(t, "PENDING")
	e.cancel(id, auth.Rider, "rider-1", `{}`)

	for _, status := range []string{"MATCHED", "NO_DRIVER"} {
		if err := setOutcome(ctx, e.db, id, status); err != nil {
			t.Fatal(err)
		}
	}
	if o, _ := queryOrder(ctx, e.db, "id = $1", id); o.Status != "CANCELLED" {
		t.Errorf("order is %s, want CANCELLED to stick", o.Status)
	}
}

func TestACancelledTripCancelsItsMatchedOrder(t *testing.T) {
	e := newCancelEnv(t)
	ctx := context.Background()
	matched := e.order(t, "MATCHED")
	pending := e.order(t, "PENDING")

	for i := 0; i < 2; i++ { // the second is a redelivery
		if err := cancelMatchedOrder(ctx, e.db, matched, events.CancelledByDriver, "flat tyre"); err != nil {
			t.Fatal(err)
		}
	}
	o, _ := queryOrder(ctx, e.db, "id = $1", matched)
	if o.Status != "CANCELLED" || o.CancelledBy != "DRIVER" || o.CancelReason != "flat tyre" || o.CancelledAt == nil {
		t.Errorf("order = %+v, want CANCELLED by the DRIVER", o)
	}

	// Only MATCHED orders move this way; a PENDING one keeps searching.
	if err := cancelMatchedOrder(ctx, e.db, pending, events.CancelledByDriver, ""); err != nil {
		t.Fatal(err)
	}
	if o, _ := queryOrder(ctx, e.db, "id = $1", pending); o.Status != "PENDING" {
		t.Errorf("a PENDING order became %s", o.Status)
	}

	// A later event never rewrites who cancelled.
	cancelMatchedOrder(ctx, e.db, matched, events.CancelledByRider, "again")
	if o, _ := queryOrder(ctx, e.db, "id = $1", matched); o.CancelledBy != "DRIVER" {
		t.Errorf("cancelledBy rewritten to %s", o.CancelledBy)
	}
}

func TestACancelledTripWithABadOrderIdIsPermanentlyUnusable(t *testing.T) {
	if err := cancelMatchedOrder(context.Background(), nil, "not-a-uuid", "RIDER", ""); err == nil {
		t.Error("want a permanent error for a non-UUID order id")
	}
}
