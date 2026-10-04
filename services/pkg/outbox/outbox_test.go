package outbox

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/tracing"
)

// These tests need a real Postgres: set TEST_DATABASE_URL to run them.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM outbox`); err != nil {
		t.Fatal(err)
	}
	return db
}

type fakeWriter struct {
	sent []kafka.Message
	err  error
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, msgs...)
	return nil
}

func count(t *testing.T, db *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRolledBackTransactionPublishesNothing(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	w := &fakeWriter{}

	tx, _ := db.Begin(ctx)
	if err := Add(ctx, tx, "svc", "t", []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	tx.Rollback(ctx)

	n, err := NewRelay(db, w, "svc").Flush(ctx)
	if err != nil || n != 0 || len(w.sent) != 0 {
		t.Fatalf("Flush = %d, %v, sent %d; want nothing", n, err, len(w.sent))
	}
}

func TestCommittedEventsArePublishedInOrderThenRemoved(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	w := &fakeWriter{}

	tx, _ := db.Begin(ctx)
	for _, v := range []string{"one", "two", "three"} {
		if err := Add(ctx, tx, "svc", "topic.a", []byte("key"), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	n, err := NewRelay(db, w, "svc").Flush(ctx)
	if err != nil || n != 3 {
		t.Fatalf("Flush = %d, %v; want 3, nil", n, err)
	}
	for i, want := range []string{"one", "two", "three"} {
		m := w.sent[i]
		if string(m.Value) != want || m.Topic != "topic.a" || string(m.Key) != "key" {
			t.Errorf("message %d = %s %s %s", i, m.Topic, m.Key, m.Value)
		}
	}
	if got := count(t, db); got != 0 {
		t.Errorf("%d rows left after publishing", got)
	}
}

func TestAFailedPublishLeavesTheEventsQueuedForRetry(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if err := Add(ctx, db, "svc", "t", nil, []byte("v")); err != nil {
		t.Fatal(err)
	}

	w := &fakeWriter{err: errors.New("broker down")}
	r := NewRelay(db, w, "svc")
	if _, err := r.Flush(ctx); err == nil {
		t.Fatal("Flush hid the publish error")
	}
	if got := count(t, db); got != 1 {
		t.Fatalf("%d rows queued after a failed publish, want 1", got)
	}

	w.err = nil
	if n, err := r.Flush(ctx); err != nil || n != 1 {
		t.Fatalf("retry Flush = %d, %v; want 1, nil", n, err)
	}
}

func TestARelayOnlyPublishesItsOwnSource(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	Add(ctx, db, "orders", "t", nil, []byte("o"))
	Add(ctx, db, "trips", "t", nil, []byte("t"))

	w := &fakeWriter{}
	if n, err := NewRelay(db, w, "orders").Flush(ctx); err != nil || n != 1 {
		t.Fatalf("Flush = %d, %v; want 1, nil", n, err)
	}
	if string(w.sent[0].Value) != "o" || count(t, db) != 1 {
		t.Errorf("published %q, %d rows left; want only the orders row published", w.sent[0].Value, count(t, db))
	}
}

func TestOnlyOneRelayPublishesASourceAtATime(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	Add(ctx, db, "svc", "t", nil, []byte("v"))

	// Hold the lock the way a relay in the middle of a batch would.
	holder, _ := db.Begin(ctx)
	defer holder.Rollback(ctx)
	var got bool
	holder.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext($1))`, "outbox:svc").Scan(&got)
	if !got {
		t.Fatal("setup: could not take the lock")
	}

	w := &fakeWriter{}
	n, err := NewRelay(db, w, "svc").Flush(ctx)
	if err != nil || n != 0 || len(w.sent) != 0 {
		t.Fatalf("a second relay published %d event(s), err %v; want it to stand aside", n, err)
	}
}

func TestTheBacklogGaugeReportsTheOldestWaitingEvent(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	r := NewRelay(db, &fakeWriter{}, "gauge-svc")

	r.observeBacklog(ctx)
	if got := testutil.ToFloat64(oldestAge.WithLabelValues("gauge-svc")); got != 0 {
		t.Fatalf("an empty outbox reports an age of %v, want 0", got)
	}

	// An event that has waited 90 seconds.
	if _, err := db.Exec(ctx, `INSERT INTO outbox (source, topic, payload, created_at) VALUES ('gauge-svc', 't', 'x', now() - interval '90 seconds')`); err != nil {
		t.Fatal(err)
	}
	r.observeBacklog(ctx)
	if got := testutil.ToFloat64(oldestAge.WithLabelValues("gauge-svc")); got < 89 || got > 95 {
		t.Errorf("age = %v, want about 90", got)
	}

	// Published, it goes back to zero, and the published counter moves.
	before := testutil.ToFloat64(publishedTotal.WithLabelValues("gauge-svc", "t"))
	if n, err := r.Flush(ctx); err != nil || n != 1 {
		t.Fatalf("Flush = %d, %v", n, err)
	}
	r.observeBacklog(ctx)
	if got := testutil.ToFloat64(oldestAge.WithLabelValues("gauge-svc")); got != 0 {
		t.Errorf("age after publishing = %v, want 0", got)
	}
	if got := testutil.ToFloat64(publishedTotal.WithLabelValues("gauge-svc", "t")) - before; got != 1 {
		t.Errorf("published counter moved by %v, want 1", got)
	}
}

// The trace of whatever queued an event follows it through the table and onto
// the Kafka message, so the consumer's span can be a child of the request's.
func TestTheTraceOfTheCallerRidesWithTheEvent(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	stop := tracing.Setup(ctx, "outbox-test")
	defer stop()

	reqCtx, span := tracing.Tracer().Start(ctx, "POST /orders")
	if err := Add(reqCtx, db, "svc", "order.created", []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	span.End()

	w := &fakeWriter{}
	if n, err := NewRelay(db, w, "svc").Flush(ctx); err != nil || n != 1 {
		t.Fatalf("Flush = %d, %v", n, err)
	}
	got := tracing.SpanContextOf(tracing.ExtractKafka(ctx, w.sent[0]))
	if got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() {
		t.Errorf("the published message carries trace %s span %s, want the request's (%s %s)",
			got.TraceID(), got.SpanID(), span.SpanContext().TraceID(), span.SpanContext().SpanID())
	}
}

func TestAnEventQueuedWithoutATraceStillPublishes(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	// A row from before the headers column existed has NULL headers.
	if _, err := db.Exec(ctx, `INSERT INTO outbox (source, topic, payload) VALUES ('svc', 't', 'x')`); err != nil {
		t.Fatal(err)
	}
	w := &fakeWriter{}
	if n, err := NewRelay(db, w, "svc").Flush(ctx); err != nil || n != 1 {
		t.Fatalf("Flush = %d, %v", n, err)
	}
	if len(w.sent[0].Headers) != 0 {
		t.Errorf("headers %v, want none", w.sent[0].Headers)
	}
}
