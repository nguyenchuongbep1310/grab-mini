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
// Registry; the service consumes them, writes the ledger, and its outbox relay
// publishes payment.recorded back to the broker.

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

// One registry for the whole test binary: a later test's consumer replays the
// topics from the start, so it must resolve the schema ids earlier tests wrote.
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
	topics := []string{events.TopicOrderCreated, events.TopicTripStatusChanged, events.TopicTripRated, events.TopicPaymentRecorded}
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
				if tp.Error != nil || len(tp.Partitions) != 3 {
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

func newKafkaFixture(t *testing.T, policy Policy) *kafkaFixture {
	t.Helper()
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	f := newFixture(t, policy)
	createTopics(t, brokers)

	codec := eventcodec.New(sharedRegistryURL())

	relayWriter := kafkaconn.Writer(&kafka.Writer{Addr: kafka.TCP(brokers), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll})
	t.Cleanup(func() { relayWriter.Close() })
	relay := outbox.NewRelay(f.db, relayWriter, outboxSource)
	f.svc = newService(f.db, f.provider, policy, codec, relay)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { relay.Run(ctx) })
	wg.Go(func() {
		f.svc.runConsumer(ctx, Env{KafkaBrokers: []string{brokers}}, "payment-test-"+newID(), codec)
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
	for i := 0; i < 10; i++ { // the first write to a fresh topic can race its leader election
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

func TestKafkaFlowFareTipAndRedelivery(t *testing.T) {
	k := newKafkaFixture(t, defaultPolicy)
	orderID, tripID := newID(), newID()
	// Ids unique to this run: the broker keeps the events of earlier runs and a
	// fresh consumer group replays them, so balances must be per-run.
	rider, driver := "rider-"+newID(), "driver-"+newID()

	k.publish(events.TopicOrderCreated, orderID, events.OrderCreated{
		Envelope: events.Envelope{EventID: newID()}, OrderID: orderID, RiderID: rider, VehicleType: events.VehicleCar,
		SurgeMultiplier: 1.5, Price: fl(120000),
	})
	completed := k.publish(events.TopicTripStatusChanged, tripID, status(orderID, tripID, driver, rider, events.TripStatusCompleted, ""))
	eventually(t, "the fare", 30*time.Second, func() bool { return k.payments(tripID) == 1 })

	// The same bytes delivered again, twice: nothing more is charged.
	k.publishRaw(events.TopicTripStatusChanged, tripID, completed)
	k.publishRaw(events.TopicTripStatusChanged, tripID, completed)

	k.publish(events.TopicTripRated, tripID, rated(orderID, tripID, driver, rider, 30000))
	eventually(t, "the tip", 30*time.Second, func() bool { return k.payments(tripID) == 2 })
	time.Sleep(2 * time.Second) // let the redeliveries be consumed

	if n := k.payments(tripID); n != 2 {
		t.Errorf("%d payments, want fare + tip only", n)
	}
	if got := k.balance(riderAccount(rider)); got != -150000 {
		t.Errorf("rider balance = %d, want -150000", got)
	}
	if got := k.balance(driverPayableAccount(driver)); got != 126000 {
		t.Errorf("driver payable = %d, want 96000 + 30000", got)
	}
	k.assertLedgerBalances()

	// payment.recorded reaches the broker (Avro), once per payment.
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{k.brokers}, Topic: events.TopicPaymentRecorded, GroupID: "reader-" + newID(), StartOffset: kafka.FirstOffset, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got := map[string]events.PaymentRecorded{}
	for len(got) < 2 {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("reading payment.recorded: %v (have %d of 2)", err, len(got))
		}
		var rec events.PaymentRecorded
		if err := k.codec.Decode(ctx, m.Topic, m.Value, &rec); err != nil {
			t.Fatal(err)
		}
		if rec.TripID == tripID {
			if _, dup := got[rec.Kind]; dup {
				t.Fatalf("two payment.recorded events for %s", rec.Kind)
			}
			got[rec.Kind] = rec
		}
	}
	if f := got["FARE"]; f.Amount != 120000 || f.DriverAmount != 96000 || f.PlatformAmount != 24000 || f.Status != "CAPTURED" {
		t.Errorf("fare event = %+v", f)
	}
	if tip := got["TIP"]; tip.Amount != 30000 || tip.DriverAmount != 30000 || tip.PlatformAmount != 0 {
		t.Errorf("tip event = %+v", tip)
	}
}

// A trip event can overtake the order.created it needs, because they are on
// different topics. The consumer retries it through the retry topic.
func TestKafkaTripEventBeforeOrderCreatedIsRetried(t *testing.T) {
	k := newKafkaFixture(t, defaultPolicy)
	orderID, tripID := newID(), newID()
	rider, driver := "rider-"+newID(), "driver-"+newID()

	k.publish(events.TopicTripStatusChanged, tripID, status(orderID, tripID, driver, rider, events.TripStatusCompleted, ""))
	time.Sleep(time.Second)
	if n := k.payments(tripID); n != 0 {
		t.Fatalf("%d payments before the order is known", n)
	}
	k.publish(events.TopicOrderCreated, orderID, events.OrderCreated{
		Envelope: events.Envelope{EventID: newID()}, OrderID: orderID, RiderID: rider, VehicleType: events.VehicleBike, Price: fl(45000),
	})
	eventually(t, "the retried fare", 40*time.Second, func() bool { return k.payments(tripID) == 1 })
	k.assertLedgerBalances()
}
