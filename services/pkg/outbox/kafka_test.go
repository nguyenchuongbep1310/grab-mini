package outbox

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Runs the relay through a real kafka.Writer: set TEST_DATABASE_URL and
// TEST_KAFKA_BROKERS. The writer has no Topic, as in the services, so each
// outbox row must carry its own.
func TestRelayPublishesToARealBroker(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	db := testDB(t)
	ctx := context.Background()
	topic := "outbox-" + uuid.NewString()[:8]

	for _, v := range []string{"first", "second", "third"} {
		if err := Add(ctx, db, "svc", topic, []byte("same-key"), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	w := &kafka.Writer{Addr: kafka.TCP(brokers), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true}
	defer w.Close()
	relay := NewRelay(db, w, "svc")
	// The first write to a topic that doesn't exist yet can fail while the broker creates it.
	var err error
	for i := 0; i < 5; i++ {
		if _, err = relay.Flush(ctx); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if got := count(t, db); got != 0 {
		t.Fatalf("%d rows left after publishing", got)
	}

	// One key means one partition, so they must come back in the order committed.
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{brokers}, Topic: topic, GroupID: "g-" + topic, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for _, want := range []string{"first", "second", "third"} {
		m, err := r.ReadMessage(rctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(m.Value) != want || string(m.Key) != "same-key" {
			t.Errorf("read key=%s value=%s, want %s", m.Key, m.Value, want)
		}
	}
}

// With a real broker and a real writer: an event over the size limit is
// quarantined instead of blocking the events around it.
func TestARealBrokerRejectingOneEventDoesNotBlockTheRest(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	db := testDB(t)
	ctx := context.Background()
	db.Exec(ctx, `DELETE FROM outbox_dead`)
	topic := "poison-" + uuid.NewString()[:8]

	Add(ctx, db, "svc", topic, []byte("k"), []byte("good-1"))
	Add(ctx, db, "svc", topic, []byte("k"), make([]byte, 2<<20)) // 2 MB: over what a broker accepts by default
	Add(ctx, db, "svc", topic, []byte("k"), []byte("good-2"))

	w := &kafka.Writer{Addr: kafka.TCP(brokers), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true}
	defer w.Close()
	relay := NewRelay(db, w, "svc")
	var n int
	var err error
	for i := 0; i < 5; i++ { // the first write to a new topic can race its creation
		if n, err = relay.Flush(ctx); err == nil && n == 3 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil || n != 3 {
		t.Fatalf("Flush = %d, %v; want all 3 finished", n, err)
	}
	if count(t, db) != 0 || deadCount(t, db) != 1 {
		t.Fatalf("%d queued, %d quarantined; want 0 and 1", count(t, db), deadCount(t, db))
	}

	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{brokers}, Topic: topic, GroupID: "g-" + topic, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for _, want := range []string{"good-1", "good-2"} {
		m, err := r.ReadMessage(rctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(m.Value) != want {
			t.Errorf("read %q, want %q", m.Value, want)
		}
	}
}
