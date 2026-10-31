package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
)

// These tests need Postgres: set TEST_DATABASE_URL to a throwaway database
// (they truncate the chat tables and delete this service's outbox rows).

// jsonEncoder stands in for the Avro codec: the tests read back what was queued.
type jsonEncoder struct{}

func (jsonEncoder) Encode(_ context.Context, _ string, v any) ([]byte, error) { return json.Marshal(v) }

type fixture struct {
	t      *testing.T
	db     *pgxpool.Pool
	svc    *service
	issuer *auth.Issuer
	api    http.Handler
	now    time.Time
}

func newFixture(t *testing.T) *fixture {
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
	if _, err := db.Exec(ctx, `
		TRUNCATE chat_messages, chat_trips, chat_events;
		DELETE FROM outbox WHERE source = 'chat-service'`); err != nil {
		t.Fatal(err)
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	issuer, err := auth.NewIssuer(priv, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// Generous by default; the rate-limit tests set their own.
	svc := newService(db, jsonEncoder{}, nil, Env{Grace: 10 * time.Minute, RatePerMinute: 6000})
	f := &fixture{t: t, db: db, svc: svc, issuer: issuer, now: time.Now().UTC().Truncate(time.Second)}
	svc.now = func() time.Time { return f.now }
	svc.limiter = nil
	f.api = svc.routes(auth.NewVerifier(auth.NewKeySet(pub)))
	return f
}

func newID() string { return uuid.NewString() }

// event builds a trip.status.changed.
func event(trip, rider, driver string, status events.TripStatus, at time.Time) events.TripStatusChanged {
	return events.TripStatusChanged{
		Envelope: events.Envelope{EventID: newID(), OccurredAt: at.Format(time.RFC3339Nano)},
		TripID:   trip, OrderID: "o-" + trip, RiderID: rider, DriverID: driver, Status: status,
	}
}

func (f *fixture) apply(evt events.TripStatusChanged) {
	f.t.Helper()
	if err := f.svc.onTripStatusChanged(context.Background(), evt); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) tripRow(trip string) (chatTrip, error) {
	return findTrip(context.Background(), f.db, trip)
}

// startTrip makes a trip that is on, with rider-1 and driver-1.
func (f *fixture) startTrip() string {
	f.t.Helper()
	trip := newID()
	f.apply(event(trip, "rider-1", "driver-1", events.TripStatusAssigned, f.now))
	return trip
}

type result struct {
	status int
	header http.Header
	body   map[string]any
}

func (f *fixture) call(method, path string, role auth.Role, subject string, body any, headers ...string) result {
	f.t.Helper()
	var rd *bytes.Reader
	switch b := body.(type) {
	case nil:
		rd = bytes.NewReader(nil)
	case string:
		rd = bytes.NewReader([]byte(b))
	default:
		raw, _ := json.Marshal(b)
		rd = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, rd)
	if subject != "" {
		tok, _, err := f.issuer.Issue(role, subject)
		if err != nil {
			f.t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	f.api.ServeHTTP(rec, req)
	res := result{status: rec.Code, header: rec.Header(), body: map[string]any{}}
	json.Unmarshal(rec.Body.Bytes(), &res.body)
	return res
}

func (f *fixture) send(trip string, role auth.Role, subject, text string, headers ...string) result {
	f.t.Helper()
	return f.call("POST", "/trips/"+trip+"/messages", role, subject, map[string]string{"text": text}, headers...)
}

func (f *fixture) count(query string, args ...any) int {
	f.t.Helper()
	var n int
	if err := f.db.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

func (f *fixture) outboxEvents() []events.ChatMessageSent {
	f.t.Helper()
	rows, err := f.db.Query(context.Background(), `SELECT payload FROM outbox WHERE source = 'chat-service' AND topic = $1 ORDER BY id`, events.TopicChatMessageSent)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var out []events.ChatMessageSent
	for rows.Next() {
		var p []byte
		if err := rows.Scan(&p); err != nil {
			f.t.Fatal(err)
		}
		var e events.ChatMessageSent
		if err := json.Unmarshal(p, &e); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func wantStatus(t *testing.T, what string, r result, want int) {
	t.Helper()
	if r.status != want {
		t.Fatalf("%s: status %d, want %d (%v)", what, r.status, want, r.body)
	}
}

func wantCode(t *testing.T, what string, r result, status int, code string) {
	t.Helper()
	wantStatus(t, what, r, status)
	if r.body["code"] != code {
		t.Fatalf("%s: code %v, want %q", what, r.body["code"], code)
	}
}

// ------------------------------------------------------------- trip events ----

func TestATripEventTeachesWhoIsOnTheTrip(t *testing.T) {
	f := newFixture(t)
	f.apply(event("trip-a", "rider-1", "driver-1", events.TripStatusAssigned, f.now))

	got, err := f.tripRow("trip-a")
	if err != nil {
		t.Fatal(err)
	}
	if got.RiderID != "rider-1" || got.DriverID != "driver-1" || got.Status != events.TripStatusAssigned {
		t.Errorf("got %+v", got)
	}
}

func TestTheStatusOnlyMovesForwardWhateverTheArrivalOrder(t *testing.T) {
	f := newFixture(t)
	end := f.now.Add(-time.Hour)
	// The events of one trip, delivered in the worst order: the end first.
	f.apply(event("trip-a", "rider-1", "driver-1", events.TripStatusCompleted, end))
	f.apply(event("trip-a", "rider-1", "driver-1", events.TripStatusInProgress, end.Add(-time.Minute)))
	f.apply(event("trip-a", "rider-1", "driver-1", events.TripStatusAssigned, end.Add(-2*time.Minute)))

	got, _ := f.tripRow("trip-a")
	if got.Status != events.TripStatusCompleted || !got.UpdatedAt.Equal(end) {
		t.Errorf("got %s at %v, want COMPLETED at %v", got.Status, got.UpdatedAt, end)
	}

	// An end never moves, not even to the other end.
	f.apply(event("trip-a", "rider-1", "driver-1", events.TripStatusCancelled, f.now))
	got, _ = f.tripRow("trip-a")
	if got.Status != events.TripStatusCompleted || !got.UpdatedAt.Equal(end) {
		t.Errorf("after a late CANCELLED: %s at %v", got.Status, got.UpdatedAt)
	}

	// And forward in the normal order works.
	f.apply(event("trip-b", "rider-1", "driver-1", events.TripStatusAssigned, end))
	f.apply(event("trip-b", "rider-1", "driver-1", events.TripStatusInProgress, end.Add(time.Minute)))
	f.apply(event("trip-b", "rider-1", "driver-1", events.TripStatusCancelled, end.Add(2*time.Minute)))
	got, _ = f.tripRow("trip-b")
	if got.Status != events.TripStatusCancelled || !got.UpdatedAt.Equal(end.Add(2*time.Minute)) {
		t.Errorf("trip-b: %s at %v", got.Status, got.UpdatedAt)
	}
}

func TestAnEventIsAppliedOnceByEventId(t *testing.T) {
	f := newFixture(t)
	start := event("trip-a", "rider-1", "driver-1", events.TripStatusAssigned, f.now)
	f.apply(start)
	f.apply(event("trip-a", "rider-1", "driver-1", events.TripStatusInProgress, f.now.Add(time.Minute)))
	f.apply(start) // redelivered

	got, _ := f.tripRow("trip-a")
	if got.Status != events.TripStatusInProgress {
		t.Errorf("status %s", got.Status)
	}
	if n := f.count(`SELECT count(*) FROM chat_events`); n != 2 {
		t.Errorf("%d dedupe rows, want 2", n)
	}
}

func TestARiderIsLearnedFromALaterEventForATripThatHadNone(t *testing.T) {
	f := newFixture(t)
	f.apply(event("trip-a", "", "driver-1", events.TripStatusAssigned, f.now)) // from before events carried the rider
	got, _ := f.tripRow("trip-a")
	if got.RiderID != "" {
		t.Fatalf("rider %q", got.RiderID)
	}
	f.apply(event("trip-a", "rider-1", "driver-1", events.TripStatusInProgress, f.now))
	got, _ = f.tripRow("trip-a")
	if got.RiderID != "rider-1" {
		t.Errorf("rider %q, want rider-1", got.RiderID)
	}
	// Once known it is not replaced.
	f.apply(event("trip-a", "rider-9", "driver-1", events.TripStatusCompleted, f.now))
	got, _ = f.tripRow("trip-a")
	if got.RiderID != "rider-1" {
		t.Errorf("rider replaced by %q", got.RiderID)
	}
}

func TestEventsThatCanNeverBeAppliedAreDeadLettered(t *testing.T) {
	f := newFixture(t)
	for name, evt := range map[string]events.TripStatusChanged{
		"no trip":        event("", "rider-1", "driver-1", events.TripStatusAssigned, f.now),
		"no driver":      event("trip-a", "rider-1", "", events.TripStatusAssigned, f.now),
		"bad event id":   {Envelope: events.Envelope{EventID: "nope"}, TripID: "trip-a", DriverID: "driver-1", Status: events.TripStatusAssigned},
		"empty event id": {TripID: "trip-a", DriverID: "driver-1", Status: events.TripStatusAssigned},
	} {
		err := f.svc.onTripStatusChanged(context.Background(), evt)
		var perm interface{ Permanent() bool }
		if !errors.As(err, &perm) || !perm.Permanent() {
			t.Errorf("%s: %v, want a permanent error", name, err)
		}
	}
	if n := f.count(`SELECT count(*) FROM chat_trips`); n != 0 {
		t.Errorf("%d trips saved", n)
	}
}

func TestAStatusThisServiceDoesNotKnowIsIgnoredNotDeadLettered(t *testing.T) {
	f := newFixture(t)
	if err := f.svc.onTripStatusChanged(context.Background(), event("trip-a", "rider-1", "driver-1", "TELEPORTED", f.now)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.tripRow("trip-a"); !errors.Is(err, errNoTrip) {
		t.Errorf("a trip was saved for an unknown status: %v", err)
	}
}

func TestTwoEventsOfOneTripAtTheSameMomentAreBothApplied(t *testing.T) {
	f := newFixture(t)
	var wg sync.WaitGroup
	for _, st := range []events.TripStatus{events.TripStatusAssigned, events.TripStatusInProgress, events.TripStatusCompleted, events.TripStatusCancelled} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.svc.onTripStatusChanged(context.Background(), event("trip-a", "rider-1", "driver-1", st, f.now)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	got, err := f.tripRow("trip-a")
	if err != nil {
		t.Fatal(err)
	}
	if !finished(got.Status) {
		t.Errorf("status %s, want an end", got.Status)
	}
}

// ---------------------------------------------------------------- sending ----

func TestARiderSendsAndThePublishedEventNamesBothPeople(t *testing.T) {
	f := newFixture(t)
	trip := f.startTrip()

	r := f.send(trip, auth.Rider, "rider-1", "  I am at the gate  ")
	wantStatus(t, "send", r, http.StatusCreated)
	if r.body["text"] != "I am at the gate" || r.body["senderId"] != "rider-1" || r.body["senderRole"] != "rider" || r.body["tripId"] != trip {
		t.Errorf("body %v", r.body)
	}
	id, _ := r.body["id"].(string)

	evts := f.outboxEvents()
	if len(evts) != 1 {
		t.Fatalf("%d events queued, want 1", len(evts))
	}
	e := evts[0]
	if e.MessageID != id || e.TripID != trip || e.SenderID != "rider-1" || e.SenderRole != "rider" ||
		e.RecipientID != "driver-1" || e.RecipientRole != "driver" || e.Text != "I am at the gate" ||
		e.EventID == "" || e.OccurredAt == "" {
		t.Errorf("event %+v", e)
	}
	var key string
	if err := f.db.QueryRow(context.Background(), `SELECT convert_from(key, 'UTF8') FROM outbox WHERE source = 'chat-service'`).Scan(&key); err != nil || key != trip {
		t.Errorf("outbox key %q (%v), want the trip id", key, err)
	}

	// The driver answers: the other way round.
	r = f.send(trip, auth.Driver, "driver-1", "On my way")
	wantStatus(t, "reply", r, http.StatusCreated)
	e = f.outboxEvents()[1]
	if e.SenderRole != "driver" || e.RecipientID != "rider-1" || e.RecipientRole != "rider" {
		t.Errorf("reply event %+v", e)
	}
}

func TestOnlyTheRiderAndDriverOfTheTripCanSend(t *testing.T) {
	f := newFixture(t)
	trip := f.startTrip()

	wantCode(t, "another rider", f.send(trip, auth.Rider, "rider-2", "hi"), 403, "not_participant")
	wantCode(t, "another driver", f.send(trip, auth.Driver, "driver-2", "hi"), 403, "not_participant")
	// The right id on the wrong side of the trip.
	wantCode(t, "a driver token carrying the rider's id", f.send(trip, auth.Driver, "rider-1", "hi"), 403, "not_participant")
	wantCode(t, "a rider token carrying the driver's id", f.send(trip, auth.Rider, "driver-1", "hi"), 403, "not_participant")
	wantStatus(t, "an admin", f.send(trip, auth.Admin, "ops-1", "hi"), 403)
	wantStatus(t, "no token", f.call("POST", "/trips/"+trip+"/messages", "", "", map[string]string{"text": "hi"}), 401)
	wantCode(t, "unknown trip", f.send("no-such-trip", auth.Rider, "rider-1", "hi"), 404, "unknown_trip")

	if n := f.count(`SELECT count(*) FROM chat_messages`); n != 0 {
		t.Errorf("%d messages saved by people who may not send", n)
	}
	if n := len(f.outboxEvents()); n != 0 {
		t.Errorf("%d events queued", n)
	}
}

func TestATripWithoutAKnownRiderCannotBeMessagedByTheDriver(t *testing.T) {
	f := newFixture(t)
	f.apply(event("trip-old", "", "driver-1", events.TripStatusAssigned, f.now))
	wantCode(t, "driver to nobody", f.send("trip-old", auth.Driver, "driver-1", "hi"), 409, "no_recipient")
}

func TestTextIsValidated(t *testing.T) {
	f := newFixture(t)
	trip := f.startTrip()

	for name, text := range map[string]string{
		"empty": "", "spaces": "   ", "too long": strings.Repeat("a", 501), "newline": "a\nb", "escape": "a\x1bb",
	} {
		wantCode(t, name, f.send(trip, auth.Rider, "rider-1", text), 400, "invalid_text")
	}
	wantCode(t, "not json", f.call("POST", "/trips/"+trip+"/messages", auth.Rider, "rider-1", "{nope"), 400, "invalid_request")
	wantCode(t, "no body", f.call("POST", "/trips/"+trip+"/messages", auth.Rider, "rider-1", ""), 400, "invalid_request")
	wantStatus(t, "exactly 500 characters of Vietnamese", f.send(trip, auth.Rider, "rider-1", strings.Repeat("ế", 500)), 201)
	if n := f.count(`SELECT count(*) FROM chat_messages`); n != 1 {
		t.Errorf("%d messages saved, want 1", n)
	}
}

func TestTheChatClosesAfterTheGracePeriod(t *testing.T) {
	f := newFixture(t)
	trip := newID()
	f.apply(event(trip, "rider-1", "driver-1", events.TripStatusAssigned, f.now))
	f.apply(event(trip, "rider-1", "driver-1", events.TripStatusCompleted, f.now))

	f.now = f.now.Add(9 * time.Minute)
	wantStatus(t, "9 minutes after the end", f.send(trip, auth.Rider, "rider-1", "thanks!"), 201)
	f.now = f.now.Add(time.Minute)
	wantStatus(t, "10 minutes after the end", f.send(trip, auth.Driver, "driver-1", "welcome"), 201)
	f.now = f.now.Add(time.Second)
	wantCode(t, "just over 10 minutes", f.send(trip, auth.Rider, "rider-1", "one more"), 409, "chat_closed")
	wantCode(t, "the driver too", f.send(trip, auth.Driver, "driver-1", "one more"), 409, "chat_closed")

	// The history stays readable, and says the chat is closed.
	r := f.call("GET", "/trips/"+trip+"/messages", auth.Rider, "rider-1", nil)
	wantStatus(t, "history", r, 200)
	if r.body["open"] != false || r.body["tripStatus"] != "COMPLETED" || r.body["closesAt"] == nil {
		t.Errorf("body %v", r.body)
	}
	if msgs := r.body["messages"].([]any); len(msgs) != 2 {
		t.Errorf("%d messages, want 2", len(msgs))
	}
}

func TestACancelledTripClosesTheSameWayAndAnActiveTripNever(t *testing.T) {
	f := newFixture(t)
	cancelled := newID()
	f.apply(event(cancelled, "rider-1", "driver-1", events.TripStatusCancelled, f.now))
	active := f.startTrip()

	f.now = f.now.Add(11 * time.Minute)
	wantCode(t, "cancelled trip", f.send(cancelled, auth.Rider, "rider-1", "hi"), 409, "chat_closed")
	f.now = f.now.Add(72 * time.Hour)
	wantStatus(t, "a trip still on three days later", f.send(active, auth.Rider, "rider-1", "still here"), 201)
}

func TestAnEarlierEndDoesNotReopenAndALateEventDoesNotShortenTheGrace(t *testing.T) {
	f := newFixture(t)
	trip := newID()
	// Completed 30 minutes ago; its ASSIGNED event is only now handled (a retry).
	f.apply(event(trip, "rider-1", "driver-1", events.TripStatusCompleted, f.now.Add(-30*time.Minute)))
	f.apply(event(trip, "rider-1", "driver-1", events.TripStatusAssigned, f.now))
	wantCode(t, "closed", f.send(trip, auth.Rider, "rider-1", "hi"), 409, "chat_closed")
}

func TestSendingIsRateLimitedPerSender(t *testing.T) {
	f := newFixture(t)
	trip := f.startTrip()
	other := newID()
	f.apply(event(other, "rider-2", "driver-2", events.TripStatusAssigned, f.now))
	f.svc.limiter = newLimiter(20) // 20 a minute, a burst of 5

	for i := range 5 {
		wantStatus(t, fmt.Sprintf("message %d of the burst", i+1), f.send(trip, auth.Rider, "rider-1", "hi"), 201)
	}
	r := f.send(trip, auth.Rider, "rider-1", "hi")
	wantCode(t, "past the burst", r, 429, "rate_limited")
	if r.body["retryAfter"] != float64(3) {
		t.Errorf("retryAfter in the body %v, want 3", r.body["retryAfter"])
	}
	if r.header.Get("Retry-After") != "3" {
		t.Errorf("Retry-After %q, want 3 (one message every 3 seconds)", r.header.Get("Retry-After"))
	}
	// Another sender, and the other side of the same trip, are not affected.
	wantStatus(t, "the driver of the same trip", f.send(trip, auth.Driver, "driver-1", "hi"), 201)
	wantStatus(t, "another rider", f.send(other, auth.Rider, "rider-2", "hi"), 201)
	if n := f.count(`SELECT count(*) FROM chat_messages WHERE sender_id = 'rider-1'`); n != 5 {
		t.Errorf("%d messages from the limited rider, want 5", n)
	}
	// Reading is not limited.
	for range 10 {
		wantStatus(t, "history", f.call("GET", "/trips/"+trip+"/messages", auth.Rider, "rider-1", nil), 200)
	}
}

// ------------------------------------------------------------ idempotency ----

func TestARetryWithTheSameKeyReturnsTheFirstMessage(t *testing.T) {
	f := newFixture(t)
	trip := f.startTrip()

	first := f.send(trip, auth.Rider, "rider-1", "at the gate", "Idempotency-Key", "k-1")
	wantStatus(t, "first", first, 201)
	again := f.send(trip, auth.Rider, "rider-1", "  at the gate ", "Idempotency-Key", "k-1")
	wantStatus(t, "retry", again, 200)
	if again.header.Get("Idempotent-Replayed") != "true" || again.body["id"] != first.body["id"] || again.body["createdAt"] != first.body["createdAt"] {
		t.Errorf("first %v, retry %v (%v)", first.body, again.body, again.header)
	}
	if n := f.count(`SELECT count(*) FROM chat_messages`); n != 1 {
		t.Errorf("%d messages, want 1", n)
	}
	if n := len(f.outboxEvents()); n != 1 {
		t.Errorf("%d events, want 1", n)
	}

	wantCode(t, "same key, other text", f.send(trip, auth.Rider, "rider-1", "something else", "Idempotency-Key", "k-1"), 422, "key_mismatch")
	// The key is the sender's own: the driver may use the same string.
	wantStatus(t, "same key from the driver", f.send(trip, auth.Driver, "driver-1", "ok", "Idempotency-Key", "k-1"), 201)
	// And per trip.
	other := f.startTrip()
	wantStatus(t, "same key on another trip", f.send(other, auth.Rider, "rider-1", "at the gate", "Idempotency-Key", "k-1"), 201)
	// Without a key every send is a new message.
	wantStatus(t, "no key", f.send(trip, auth.Rider, "rider-1", "at the gate"), 201)
	wantStatus(t, "no key again", f.send(trip, auth.Rider, "rider-1", "at the gate"), 201)

	wantCode(t, "bad key", f.send(trip, auth.Rider, "rider-1", "x", "Idempotency-Key", strings.Repeat("k", 129)), 400, "invalid_request")
}

func TestARetryAfterTheChatClosedStillGetsTheOriginalAnswer(t *testing.T) {
	f := newFixture(t)
	trip := f.startTrip()
	first := f.send(trip, auth.Rider, "rider-1", "ok", "Idempotency-Key", "k-1")
	wantStatus(t, "first", first, 201)

	f.apply(event(trip, "rider-1", "driver-1", events.TripStatusCompleted, f.now))
	f.now = f.now.Add(time.Hour)
	again := f.send(trip, auth.Rider, "rider-1", "ok", "Idempotency-Key", "k-1")
	wantStatus(t, "retry", again, 200)
	if again.body["id"] != first.body["id"] {
		t.Errorf("got %v", again.body)
	}
	wantCode(t, "a new message", f.send(trip, auth.Rider, "rider-1", "new", "Idempotency-Key", "k-2"), 409, "chat_closed")
}

func TestRequestsWithOneKeyAtTheSameMomentSaveOneMessage(t *testing.T) {
	f := newFixture(t)
	trip := f.startTrip()
	var wg sync.WaitGroup
	statuses := make(chan int, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses <- f.send(trip, auth.Rider, "rider-1", "double tap", "Idempotency-Key", "tap").status
		}()
	}
	wg.Wait()
	close(statuses)
	created := 0
	for s := range statuses {
		switch s {
		case 201:
			created++
		case 200:
		default:
			t.Errorf("status %d", s)
		}
	}
	if created != 1 {
		t.Errorf("%d requests created a message, want 1", created)
	}
	if n := f.count(`SELECT count(*) FROM chat_messages`); n != 1 {
		t.Errorf("%d messages", n)
	}
	if n := len(f.outboxEvents()); n != 1 {
		t.Errorf("%d events", n)
	}
}

// ---------------------------------------------------------------- history ----

func TestHistoryIsNewestFirstAndPagesWithoutGapsOrRepeats(t *testing.T) {
	f := newFixture(t)
	trip := f.startTrip()
	// Seven messages, three of them at one instant, to prove the id breaks the tie.
	for i := range 7 {
		if i != 3 && i != 4 {
			f.now = f.now.Add(time.Second)
		}
		who, role := "rider-1", auth.Rider
		if i%2 == 1 {
			who, role = "driver-1", auth.Driver
		}
		wantStatus(t, "send", f.send(trip, role, who, fmt.Sprintf("m%d", i)), 201)
	}

	var seen []string
	before := ""
	for page := 0; ; page++ {
		path := "/trips/" + trip + "/messages?limit=3"
		if before != "" {
			path += "&before=" + before
		}
		r := f.call("GET", path, auth.Rider, "rider-1", nil)
		wantStatus(t, "page", r, 200)
		msgs := r.body["messages"].([]any)
		for _, m := range msgs {
			seen = append(seen, m.(map[string]any)["text"].(string))
		}
		next, _ := r.body["nextBefore"].(string)
		if next == "" {
			if len(msgs) > 3 || page != 2 {
				t.Fatalf("the last page had %d messages on page %d", len(msgs), page)
			}
			break
		}
		if len(msgs) != 3 {
			t.Fatalf("a full page was expected, got %d", len(msgs))
		}
		before = next
		if page > 5 {
			t.Fatal("paging does not end")
		}
	}
	if len(seen) != 7 {
		t.Fatalf("saw %v", seen)
	}
	// Newest first: m6 ... m0 except the tie, whose order is by id but still stable and complete.
	got := map[string]bool{}
	for _, s := range seen {
		got[s] = true
	}
	for i := range 7 {
		if !got[fmt.Sprintf("m%d", i)] {
			t.Errorf("m%d missing from %v", i, seen)
		}
	}
	if seen[0] != "m6" || seen[1] != "m5" || seen[6] != "m0" {
		t.Errorf("order %v", seen)
	}

	// A message that arrives between two pages does not shift the next one.
	r := f.call("GET", "/trips/"+trip+"/messages?limit=3", auth.Rider, "rider-1", nil)
	next := r.body["nextBefore"].(string)
	f.now = f.now.Add(time.Second)
	wantStatus(t, "new message", f.send(trip, auth.Driver, "driver-1", "late"), 201)
	r = f.call("GET", "/trips/"+trip+"/messages?limit=3&before="+next, auth.Rider, "rider-1", nil)
	msgs := r.body["messages"].([]any)
	if len(msgs) != 3 || msgs[0].(map[string]any)["text"] == "late" {
		t.Errorf("page after a new arrival: %v", msgs)
	}
}

func TestOnlyParticipantsCanReadAndEachSeesTheWholeConversation(t *testing.T) {
	f := newFixture(t)
	trip := f.startTrip()
	f.send(trip, auth.Rider, "rider-1", "hello")
	f.now = f.now.Add(time.Second)
	f.send(trip, auth.Driver, "driver-1", "hi")

	for _, who := range []struct {
		role auth.Role
		id   string
	}{{auth.Rider, "rider-1"}, {auth.Driver, "driver-1"}} {
		r := f.call("GET", "/trips/"+trip+"/messages", who.role, who.id, nil)
		wantStatus(t, "read", r, 200)
		msgs := r.body["messages"].([]any)
		if len(msgs) != 2 || msgs[0].(map[string]any)["text"] != "hi" || msgs[1].(map[string]any)["text"] != "hello" {
			t.Errorf("%s sees %v", who.id, msgs)
		}
		if r.body["open"] != true || r.body["closesAt"] != nil || r.body["nextBefore"] != nil {
			t.Errorf("%s: %v", who.id, r.body)
		}
	}
	wantCode(t, "another rider", f.call("GET", "/trips/"+trip+"/messages", auth.Rider, "rider-2", nil), 403, "not_participant")
	wantCode(t, "another driver", f.call("GET", "/trips/"+trip+"/messages", auth.Driver, "driver-2", nil), 403, "not_participant")
	wantStatus(t, "admin", f.call("GET", "/trips/"+trip+"/messages", auth.Admin, "ops-1", nil), 403)
	wantStatus(t, "anonymous", f.call("GET", "/trips/"+trip+"/messages", "", "", nil), 401)
	wantCode(t, "unknown trip", f.call("GET", "/trips/nope/messages", auth.Rider, "rider-1", nil), 404, "unknown_trip")

	// Another trip's messages never show up.
	other := newID()
	f.apply(event(other, "rider-1", "driver-3", events.TripStatusAssigned, f.now))
	r := f.call("GET", "/trips/"+other+"/messages", auth.Rider, "rider-1", nil)
	if msgs := r.body["messages"].([]any); len(msgs) != 0 {
		t.Errorf("%v", msgs)
	}
}

func TestHistoryParametersAreChecked(t *testing.T) {
	f := newFixture(t)
	trip := f.startTrip()
	for _, q := range []string{"limit=0", "limit=101", "limit=abc", "limit=-1", "before=junk", "before=1.junk", "before=x.6f1e2d3c-1111-4222-8333-444455556666"} {
		wantCode(t, q, f.call("GET", "/trips/"+trip+"/messages?"+q, auth.Rider, "rider-1", nil), 400, "invalid_request")
	}
	wantStatus(t, "limit=100", f.call("GET", "/trips/"+trip+"/messages?limit=100", auth.Rider, "rider-1", nil), 200)
}

// ---------------------------------------------------------------- privacy ----

func TestTheTextOfAMessageIsNeverLogged(t *testing.T) {
	f := newFixture(t)
	trip := f.startTrip()

	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	secret := "my-secret-phrase-4421"
	wantStatus(t, "send", f.send(trip, auth.Rider, "rider-1", secret, "Idempotency-Key", "k"), 201)
	wantStatus(t, "replay", f.send(trip, auth.Rider, "rider-1", secret, "Idempotency-Key", "k"), 200)
	wantStatus(t, "mismatch", f.send(trip, auth.Rider, "rider-1", secret+"x", "Idempotency-Key", "k"), 422)
	wantStatus(t, "invalid", f.send(trip, auth.Rider, "rider-1", secret+"\n", "Idempotency-Key", "k2"), 201) // trimmed: valid
	wantStatus(t, "read", f.call("GET", "/trips/"+trip+"/messages", auth.Rider, "rider-1", nil), 200)
	f.apply(event(trip, "rider-1", "driver-1", events.TripStatusCompleted, f.now))

	if strings.Contains(buf.String(), "secret-phrase") {
		t.Errorf("the message text reached the log:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "characters)") {
		t.Errorf("expected a log line with the length, got:\n%s", buf.String())
	}
}

// -------------------------------------------------------------- retention ----

func TestRetentionDeletesOldMessagesAndFinishedTripsOnly(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	old, recent := f.now.AddDate(0, 0, -40), f.now.AddDate(0, 0, -5)

	f.apply(event("old-done", "r", "d", events.TripStatusCompleted, old))
	f.apply(event("old-active", "r", "d", events.TripStatusInProgress, old)) // never reached an end: kept
	f.apply(event("recent-done", "r", "d", events.TripStatusCancelled, recent))
	for _, m := range []struct {
		trip string
		at   time.Time
	}{{"old-done", old}, {"old-done", old.Add(time.Hour)}, {"old-active", old}, {"recent-done", recent}} {
		if _, err := f.db.Exec(ctx, `INSERT INTO chat_messages (id, trip_id, sender_id, sender_role, text, created_at) VALUES ($1, $2, 'r', 'rider', 'x', $3)`, newID(), m.trip, m.at); err != nil {
			t.Fatal(err)
		}
	}

	messages, trips, err := pruneOnce(ctx, f.db, 30)
	if err != nil {
		t.Fatal(err)
	}
	if messages != 3 || trips != 1 {
		t.Errorf("pruned %d messages and %d trips, want 3 and 1", messages, trips)
	}
	if n := f.count(`SELECT count(*) FROM chat_messages`); n != 1 {
		t.Errorf("%d messages left, want the recent one", n)
	}
	var left []string
	rows, _ := f.db.Query(ctx, `SELECT trip_id FROM chat_trips ORDER BY trip_id`)
	for rows.Next() {
		var id string
		rows.Scan(&id)
		left = append(left, id)
	}
	rows.Close()
	if strings.Join(left, ",") != "old-active,recent-done" {
		t.Errorf("trips left: %v", left)
	}
	// A second run finds nothing.
	if m, tr, err := pruneOnce(ctx, f.db, 30); err != nil || m != 0 || tr != 0 {
		t.Errorf("second run: %d %d %v", m, tr, err)
	}
}
