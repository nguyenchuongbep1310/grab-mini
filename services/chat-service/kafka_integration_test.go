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

	"grab-mini/pkg/auth"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconn"
	"grab-mini/pkg/outbox"
)

// The full path through a real broker and Postgres: set TEST_KAFKA_BROKERS and
// TEST_DATABASE_URL. Events are Avro-encoded against an in-process Schema
// Registry; the service consumes trip.status.changed, learns the trip, and its
// outbox relay publishes chat.message.sent back to the broker.

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

func createTopics(t *testing.T, brokers string) {
	t.Helper()
	topics := []string{events.TopicTripStatusChanged, events.TopicChatMessageSent}
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

func TestKafkaTripEventsOpenTheChatAndMessagesAreAnnounced(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	f := newFixture(t)
	f.now = time.Now().UTC() // the service and the broker share a clock here
	f.svc.now = time.Now
	createTopics(t, brokers)

	registry := httptest.NewServer(&fakeRegistry{})
	defer registry.Close()
	codec := eventcodec.New(registry.URL)
	relayWriter := kafkaconn.Writer(&kafka.Writer{Addr: kafka.TCP(brokers), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll})
	defer relayWriter.Close()
	relay := outbox.NewRelay(f.db, relayWriter, outboxSource)
	f.svc.enc, f.svc.relay = codec, relay

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { relay.Run(ctx) })
	wg.Go(func() { f.svc.runConsumer(ctx, Env{KafkaBrokers: []string{brokers}}, "chat-test-"+newID(), codec) })
	defer func() { cancel(); wg.Wait() }()

	w := kafkaconn.Writer(&kafka.Writer{Addr: kafka.TCP(brokers), RequiredAcks: kafka.RequireAll})
	defer w.Close()
	publish := func(evt events.TripStatusChanged) {
		t.Helper()
		payload, err := codec.Encode(context.Background(), events.TopicTripStatusChanged, evt)
		if err != nil {
			t.Fatal(err)
		}
		var werr error
		for range 10 { // the first write to a fresh topic can race its leader election
			if werr = w.WriteMessages(context.Background(), kafka.Message{Topic: events.TopicTripStatusChanged, Key: []byte(evt.TripID), Value: payload}); werr == nil {
				return
			}
			time.Sleep(300 * time.Millisecond)
		}
		t.Fatal(werr)
	}

	// Ids unique to this run: the broker keeps the events of earlier runs.
	trip, rider, driver := newID(), "rider-"+newID(), "driver-"+newID()
	wantStatus(t, "before any event", f.send(trip, auth.Rider, rider, "hi"), 404)

	publish(event(trip, rider, driver, events.TripStatusAssigned, time.Now()))
	eventually(t, "the trip to be learned", 30*time.Second, func() bool { _, err := f.tripRow(trip); return err == nil })

	r := f.send(trip, auth.Rider, rider, "Mình ở cổng số 2")
	wantStatus(t, "send", r, 201)

	// chat.message.sent reaches the broker as Avro, keyed by the trip.
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{brokers}, Topic: events.TopicChatMessageSent, GroupID: "reader-" + newID(), StartOffset: kafka.FirstOffset, MaxWait: 200 * time.Millisecond})
	defer reader.Close()
	rctx, rcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer rcancel()
	for {
		m, err := reader.ReadMessage(rctx)
		if err != nil {
			t.Fatalf("reading chat.message.sent: %v", err)
		}
		var evt events.ChatMessageSent
		if err := codec.Decode(rctx, m.Topic, m.Value, &evt); err != nil {
			t.Fatal(err)
		}
		if evt.TripID != trip {
			continue
		}
		if string(m.Key) != trip || evt.MessageID != r.body["id"] || evt.SenderID != rider || evt.SenderRole != "rider" ||
			evt.RecipientID != driver || evt.RecipientRole != "driver" || evt.Text != "Mình ở cổng số 2" {
			t.Errorf("key %q event %+v", m.Key, evt)
		}
		break
	}

	// The trip ends, and an old event redelivered later does not reopen it.
	publish(event(trip, rider, driver, events.TripStatusCompleted, time.Now().Add(-time.Hour)))
	eventually(t, "the end to be learned", 30*time.Second, func() bool {
		got, err := f.tripRow(trip)
		return err == nil && got.Status == events.TripStatusCompleted
	})
	wantStatus(t, "an hour after the end", f.send(trip, auth.Rider, rider, "late"), 409)
	publish(event(trip, rider, driver, events.TripStatusInProgress, time.Now().Add(-90*time.Minute)))
	time.Sleep(2 * time.Second)
	if got, _ := f.tripRow(trip); got.Status != events.TripStatusCompleted {
		t.Errorf("status %s after a late IN_PROGRESS", got.Status)
	}
}
