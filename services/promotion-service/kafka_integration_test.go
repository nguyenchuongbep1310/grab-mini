package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconn"
	"grab-mini/pkg/outbox"
)

// The full path through a real broker and Postgres: set TEST_KAFKA_BROKERS and
// TEST_DATABASE_URL. Events are Avro-encoded against an in-process Schema
// Registry; the service consumes them, records the redemption, and its outbox
// relay publishes promotion.redeemed back to the broker.

// fakeRegistry implements the two Schema Registry endpoints the codec uses.
type fakeRegistry struct {
	mu      sync.Mutex
	schemas []string
}

func (f *fakeRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/subjects/"):
		var body struct{ Schema string }
		json.NewDecoder(r.Body).Decode(&body)
		for i, s := range f.schemas {
			if s == body.Schema {
				json.NewEncoder(w).Encode(map[string]int{"id": i + 1})
				return
			}
		}
		f.schemas = append(f.schemas, body.Schema)
		json.NewEncoder(w).Encode(map[string]int{"id": len(f.schemas)})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/schemas/ids/"):
		var id int
		fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/schemas/ids/"), "%d", &id)
		if id < 1 || id > len(f.schemas) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"schema": f.schemas[id-1]})
	default:
		http.NotFound(w, r)
	}
}

var (
	registryOnce sync.Once
	registryURL  string
)

func sharedRegistryURL() string {
	registryOnce.Do(func() { registryURL = httptest.NewServer(&fakeRegistry{}).URL })
	return registryURL
}

func createTopics(t *testing.T, brokers string) {
	t.Helper()
	topics := []string{events.TopicOrderCreated, events.TopicOrderCancelled, events.TopicTripStatusChanged, events.TopicOrderNoDriverFound, events.TopicPromotionRedeemed}
	var specs []kafka.TopicConfig
	for _, topic := range topics {
		specs = append(specs, kafka.TopicConfig{Topic: topic, NumPartitions: 3, ReplicationFactor: 1})
	}
	client := &kafka.Client{Addr: kafka.TCP(brokers), Timeout: 10 * time.Second}
	resp, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{Topics: specs})
	if err != nil {
		t.Fatal(err)
	}
	for topic, err := range resp.Errors {
		if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
			t.Fatalf("create %s: %v", topic, err)
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		meta, err := client.Metadata(context.Background(), &kafka.MetadataRequest{Topics: topics})
		ready := err == nil && len(meta.Topics) == len(topics)
		if ready {
			for _, tp := range meta.Topics {
				if tp.Error != nil || len(tp.Partitions) == 0 {
					ready = false
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
			t.Fatalf("topics not ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type kafkaFixture struct {
	*fixture
	brokers string
	codec   *eventcodec.Codec
	w       *kafka.Writer
}

func newKafkaFixture(t *testing.T) *kafkaFixture {
	t.Helper()
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	f := newFixture(t)
	createTopics(t, brokers)

	codec := eventcodec.New(sharedRegistryURL())
	relayWriter := kafkaconn.Writer(&kafka.Writer{Addr: kafka.TCP(brokers), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll})
	t.Cleanup(func() { relayWriter.Close() })
	relay := outbox.NewRelay(f.db, relayWriter, outboxSource)
	f.svc.enc, f.svc.relay = codec, relay

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { relay.Run(ctx) })
	wg.Go(func() {
		f.svc.runConsumer(ctx, Env{KafkaBrokers: []string{brokers}}, "promotion-test-"+newID(), codec)
	})
	t.Cleanup(func() { cancel(); wg.Wait() })

	w := kafkaconn.Writer(&kafka.Writer{Addr: kafka.TCP(brokers), RequiredAcks: kafka.RequireAll})
	t.Cleanup(func() { w.Close() })
	return &kafkaFixture{fixture: f, brokers: brokers, codec: codec, w: w}
}

func (k *kafkaFixture) publish(topic, key string, evt any) []byte {
	k.t.Helper()
	payload, err := k.codec.Encode(context.Background(), topic, evt)
	if err != nil {
		k.t.Fatal(err)
	}
	k.publishRaw(topic, key, payload)
	return payload
}

func (k *kafkaFixture) publishRaw(topic, key string, payload []byte) {
	k.t.Helper()
	var err error
	for range 10 { // the first write to a fresh topic can race its leader election
		if err = k.w.WriteMessages(context.Background(), kafka.Message{Topic: topic, Key: []byte(key), Value: payload}); err == nil {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	k.t.Fatal(err)
}

func eventually(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (k *kafkaFixture) status(code, orderID string) string {
	var s string
	k.db.QueryRow(context.Background(), `SELECT status FROM redemptions WHERE code = $1 AND order_id = $2`, code, orderID).Scan(&s)
	return s
}

func TestKafkaOrderRedeemsAnnouncesAndCancelReleases(t *testing.T) {
	k := newKafkaFixture(t)
	k.promotion("WELCOME10", func(p *Promotion) { p.PerRider = 5 })
	// Ids unique to this run: the broker keeps the events of earlier runs.
	rider := "rider-" + newID()
	order, plain := newID(), newID()

	// An order with no code goes by untouched; the one with a code is redeemed.
	k.publish(events.TopicOrderCreated, plain, created(plain, rider, "", 0))
	redeemed := k.publish(events.TopicOrderCreated, order, created(order, rider, "WELCOME10", 8700))
	eventually(t, "the redemption", 30*time.Second, func() bool { return k.status("WELCOME10", order) == "REDEEMED" })

	// The same bytes delivered again, twice: nothing more is recorded.
	k.publishRaw(events.TopicOrderCreated, order, redeemed)
	k.publishRaw(events.TopicOrderCreated, order, redeemed)

	// promotion.redeemed reaches the broker (Avro), once, keyed by the order.
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{k.brokers}, Topic: events.TopicPromotionRedeemed, GroupID: "reader-" + newID(), StartOffset: kafka.FirstOffset, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("reading promotion.redeemed: %v", err)
		}
		var evt events.PromotionRedeemed
		if err := k.codec.Decode(ctx, m.Topic, m.Value, &evt); err != nil {
			t.Fatal(err)
		}
		if evt.OrderID != order {
			continue
		}
		if string(m.Key) != order || evt.RiderID != rider || evt.Code != "WELCOME10" || evt.Discount != 8700 || evt.Status != events.PromotionStatusRedeemed {
			t.Errorf("key %q event %+v", m.Key, evt)
		}
		break
	}

	// Cancelling the order gives the redemption back.
	k.publish(events.TopicOrderCancelled, order, cancelled(order, rider))
	eventually(t, "the release", 30*time.Second, func() bool { return k.status("WELCOME10", order) == "RELEASED" })
	time.Sleep(2 * time.Second) // let the redeliveries be consumed
	if n := k.scalar(`SELECT count(*) FROM redemptions WHERE order_id = $1`, order); n != 1 {
		t.Errorf("%d redemptions for the order, want 1", n)
	}
	if n := k.scalar(`SELECT count(*) FROM redemptions WHERE order_id = $1`, plain); n != 0 {
		t.Errorf("%d redemptions for the order that had no code", n)
	}
}

// The v4 fields survive the Avro round trip through the broker into the redemption.
func TestKafkaRedemptionDecodesTheV4Fields(t *testing.T) {
	k := newKafkaFixture(t)
	k.promotion("FLAT15K", func(p *Promotion) { p.Kind = kindFixed; p.Value = 15000; p.MaxDiscount = nil })
	order := newID()
	k.publish(events.TopicOrderCreated, order, created(order, "rider-"+newID(), "FLAT15K", 15000))
	eventually(t, "the redemption", 30*time.Second, func() bool { return k.status("FLAT15K", order) == "REDEEMED" })
	var discount int64
	k.db.QueryRow(context.Background(), `SELECT discount FROM redemptions WHERE order_id = $1`, order).Scan(&discount)
	if discount != 15000 {
		t.Errorf("discount = %d, want 15000", discount)
	}
}

// A trip cancelled after the match and an order that ends NO_DRIVER release the
// redemption through the one consumer group; other trip statuses do not.
func TestKafkaTripCancelledAndNoDriverReleaseTheRedemption(t *testing.T) {
	k := newKafkaFixture(t)
	k.promotion("WELCOME10", func(p *Promotion) { p.PerRider = 5 })
	rider := "rider-" + newID()
	trip, none, done := newID(), newID(), newID()
	for _, order := range []string{trip, none, done} {
		k.publish(events.TopicOrderCreated, order, created(order, rider, "WELCOME10", 8700))
	}
	for _, order := range []string{trip, none, done} {
		eventually(t, "the redemption", 30*time.Second, func() bool { return k.status("WELCOME10", order) == "REDEEMED" })
	}

	completed := tripCancelled(done, rider)
	completed.Status, completed.CancelledBy = events.TripStatusCompleted, ""
	k.publish(events.TopicTripStatusChanged, done, completed)
	k.publish(events.TopicTripStatusChanged, trip, tripCancelled(trip, rider))
	k.publish(events.TopicOrderNoDriverFound, none, noDriver(none, rider))

	eventually(t, "the release of the cancelled trip", 30*time.Second, func() bool { return k.status("WELCOME10", trip) == "RELEASED" })
	eventually(t, "the release of the no-driver order", 30*time.Second, func() bool { return k.status("WELCOME10", none) == "RELEASED" })
	if got := k.status("WELCOME10", done); got != "REDEEMED" {
		t.Errorf("the completed trip's redemption = %s, want REDEEMED", got)
	}
}
