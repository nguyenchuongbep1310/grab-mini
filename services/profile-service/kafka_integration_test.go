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
// TEST_DATABASE_URL. Events are Avro-encoded against an in-process Schema Registry;
// the outbox relay publishes driver.application.reviewed to the broker.

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

func createTopic(t *testing.T, brokers, topic string) {
	t.Helper()
	client := &kafka.Client{Addr: kafka.TCP(brokers), Timeout: 10 * time.Second}
	resp, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{
		{Topic: topic, NumPartitions: 3, ReplicationFactor: 1}}})
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
		meta, err := client.Metadata(context.Background(), &kafka.MetadataRequest{Topics: []string{topic}})
		ready := err == nil && len(meta.Topics) == 1 && meta.Topics[0].Error == nil && len(meta.Topics[0].Partitions) > 0
		if ready {
			for _, p := range meta.Topics[0].Partitions {
				if p.Error != nil || p.Leader.ID < 0 {
					ready = false
				}
			}
		}
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("topic not ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestKafkaDecisionsAreAnnouncedAsAvro(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	f := newFixture(t)
	f.now = time.Now().UTC()
	f.svc.now = time.Now
	createTopic(t, brokers, events.TopicDriverApplicationReviewed)

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
	defer func() { cancel(); wg.Wait() }()

	// An id unique to this run: the broker keeps the events of earlier runs.
	id := fmt.Sprintf("driver-%d", time.Now().UnixNano())
	f.approved(id)
	wantStatus(t, "suspend", f.decide("suspend", id, "Complaint: unsafe driving, Mình đã kiểm tra"), 200)
	wantStatus(t, "approve", f.decide("approve", id, ""), 200)
	body := vehicle("51A-123.45")
	body["vehicleType"] = "BIKE"
	wantStatus(t, "change the vehicle", f.putDriver(id, body), 200) // APPROVED -> PENDING, SUSPENDED by the system

	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{brokers}, Topic: events.TopicDriverApplicationReviewed, GroupID: "reader-" + fmt.Sprint(time.Now().UnixNano()), StartOffset: kafka.FirstOffset, MaxWait: 200 * time.Millisecond})
	defer reader.Close()
	rctx, rcancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer rcancel()
	var got []events.DriverApplicationReviewed
	for len(got) < 4 {
		m, err := reader.ReadMessage(rctx)
		if err != nil {
			t.Fatalf("reading driver.application.reviewed (got %d of 4): %v", len(got), err)
		}
		var evt events.DriverApplicationReviewed
		if err := codec.Decode(rctx, m.Topic, m.Value, &evt); err != nil {
			t.Fatal(err)
		}
		if evt.DriverID != id {
			continue
		}
		if string(m.Key) != id {
			t.Errorf("key %q, want the driver id", m.Key)
		}
		got = append(got, evt)
	}
	want := []struct{ status, by string }{{"APPROVED", "ops-1"}, {"SUSPENDED", "ops-1"}, {"APPROVED", "ops-1"}, {"SUSPENDED", "system"}}
	var last time.Time
	for i, evt := range got {
		if evt.Status != want[i].status || evt.ReviewedBy != want[i].by {
			t.Errorf("event %d = %+v, want %s by %s", i, evt, want[i].status, want[i].by)
		}
		at, err := time.Parse(time.RFC3339Nano, evt.OccurredAt)
		if err != nil || !at.After(last) {
			t.Errorf("event %d: occurredAt %q not after the previous", i, evt.OccurredAt)
		}
		last = at
	}
	if got[1].Reason != "Complaint: unsafe driving, Mình đã kiểm tra" || got[3].VehicleType != events.VehicleBike {
		t.Errorf("reason or vehicle lost on the way: %+v %+v", got[1], got[3])
	}
}
