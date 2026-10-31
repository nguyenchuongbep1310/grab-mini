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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconn"
)

// These need a real Redis like the rest (see setup); the Kafka test also needs
// TEST_KAFKA_BROKERS.

var reviewBase = time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

func review(driver, status string, vehicle events.VehicleType, at time.Time) events.DriverApplicationReviewed {
	return events.DriverApplicationReviewed{
		Envelope: events.Envelope{EventID: "e-" + at.Format(time.RFC3339Nano), OccurredAt: at.Format(time.RFC3339Nano)},
		DriverID: driver, Status: status, VehicleType: vehicle, ReviewedBy: "ops-1",
	}
}

func TestAnApprovedDriverWithNoRedisStateIsMatchedLikeASeededOne(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	// A new driver sends locations (the geo set) but has never been made eligible.
	driverlocation.Update(ctx, rdb, "driver-new", 10.7769, 106.7009, 1)
	// Far enough from the two seeded cars to be the only candidate: take those out.
	driverstatus.Set(ctx, rdb, "driver-1", driverstatus.OnTrip)
	driverstatus.Set(ctx, rdb, "driver-2", driverstatus.OnTrip)

	p := &fakeProducer{}
	if err := matchOrder(ctx, env, rdb, p, fakeEncoder{}, order("o1")); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || p.sent[0].Topic != events.TopicOrderNoDriverFound {
		t.Fatalf("an unreviewed driver was matched: %v", p.sent)
	}

	if err := applyDriverReview(ctx, env, rdb, review("driver-new", events.ReviewApproved, events.VehicleCar, reviewBase)); err != nil {
		t.Fatal(err)
	}
	p = &fakeProducer{}
	if err := matchOrder(ctx, env, rdb, p, fakeEncoder{}, order("o2")); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || string(p.sent[0].Value) != "matched:driver-new:rider-1" {
		t.Fatalf("sent %v, want the approved driver matched", p.sent)
	}
}

func TestASuspendedDriverIsSkippedAndTheNextOneIsMatched(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	if err := applyDriverReview(ctx, env, rdb, review("driver-1", events.ReviewSuspended, events.VehicleCar, reviewBase)); err != nil {
		t.Fatal(err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.Offline {
		t.Fatalf("driver-1 = %s, want OFFLINE", s)
	}
	p := &fakeProducer{}
	if err := matchOrder(ctx, env, rdb, p, fakeEncoder{}, order("o1")); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || string(p.sent[0].Value) != "matched:driver-2:rider-1" {
		t.Fatalf("sent %v, want driver-2 (driver-1 is suspended and nearer)", p.sent)
	}
}

// Approval never touches a driver who is on an order or a trip.
func TestApprovalDoesNotMakeABusyDriverAvailable(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	if res, _ := driverstatus.ClaimForOrder(ctx, rdb, "o1", "driver-1", "CAR", time.Minute); res != driverstatus.Claimed {
		t.Fatal("setup claim failed")
	}
	if err := applyDriverReview(ctx, env, rdb, review("driver-1", events.ReviewApproved, events.VehicleCar, reviewBase)); err != nil {
		t.Fatal(err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.Assigned {
		t.Errorf("driver-1 = %s, want ASSIGNED untouched", s)
	}
	driverstatus.Set(ctx, rdb, "driver-2", driverstatus.OnTrip)
	if err := applyDriverReview(ctx, env, rdb, review("driver-2", events.ReviewApproved, events.VehicleCar, reviewBase)); err != nil {
		t.Fatal(err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-2"); s != driverstatus.OnTrip {
		t.Errorf("driver-2 = %s, want ON_TRIP untouched", s)
	}
}

func TestADriverSuspendedOnATripFinishesItAndIsThenNotOffered(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	driverstatus.Set(ctx, rdb, "driver-1", driverstatus.OnTrip)
	if err := applyDriverReview(ctx, env, rdb, review("driver-1", events.ReviewSuspended, events.VehicleCar, reviewBase)); err != nil {
		t.Fatal(err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.OnTrip {
		t.Fatalf("driver-1 = %s: the trip in progress must not be cut short", s)
	}
	// Trip Service releases the driver when the trip ends.
	if ok, err := driverstatus.Transition(ctx, rdb, "driver-1", driverstatus.OnTrip, driverstatus.Available); err != nil || !ok {
		t.Fatalf("release = %v, %v", ok, err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.Offline {
		t.Errorf("after the trip: %s, want OFFLINE", s)
	}
}

func TestReviewResultsOutOfOrderAndRepeatedConverge(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	suspend := review("driver-1", events.ReviewSuspended, events.VehicleCar, reviewBase.Add(2*time.Second))
	approve := review("driver-1", events.ReviewApproved, events.VehicleCar, reviewBase.Add(time.Second))
	stale := func() float64 { return testutil.ToFloat64(driverReviews.WithLabelValues("stale")) }
	s0 := stale()

	// The suspension is handled first, then the approval that was decided before it
	// (a retried message is handled after the ones behind it), then a redelivery.
	for _, evt := range []events.DriverApplicationReviewed{suspend, approve, suspend} {
		if err := applyDriverReview(ctx, env, rdb, evt); err != nil {
			t.Fatal(err)
		}
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.Offline {
		t.Errorf("driver-1 = %s, want OFFLINE: the late approval must lose", s)
	}
	if got := stale() - s0; got != 2 {
		t.Errorf("stale results = %v, want 2", got)
	}
	// A later approval does bring them back.
	if err := applyDriverReview(ctx, env, rdb, review("driver-1", events.ReviewApproved, events.VehicleCar, reviewBase.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.Available {
		t.Errorf("driver-1 = %s, want AVAILABLE", s)
	}
}

func TestAnUnusableReviewIsPermanentAndChangesNothing(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	for name, evt := range map[string]events.DriverApplicationReviewed{
		"no driver":              review("", events.ReviewApproved, events.VehicleCar, reviewBase),
		"unknown status":         review("driver-1", "PENDING", events.VehicleCar, reviewBase),
		"approved without a car": review("driver-1", events.ReviewApproved, "", reviewBase),
		"bad time":               {Envelope: events.Envelope{OccurredAt: "yesterday"}, DriverID: "driver-1", Status: events.ReviewRejected},
	} {
		err := applyDriverReview(ctx, env, rdb, evt)
		var p interface{ Permanent() bool }
		if err == nil || !errors.As(err, &p) || !p.Permanent() {
			t.Errorf("%s: err = %v, want a permanent error", name, err)
		}
	}
	if on, _ := driverstatus.Suspended(ctx, rdb, "driver-1"); on {
		t.Error("an invalid review suspended a driver")
	}
}

// A real Redis: many claims race a suspension. When the suspension has been applied, nobody
// who claims afterwards gets the driver; the driver either got an order first (and keeps it)
// or none.
func TestOrdersRacingASuspensionNeverReachASuspendedDriver(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	for round := 0; round < 100; round++ {
		id := fmt.Sprintf("race-%d", round)
		driverlocation.Update(ctx, rdb, id, 10.7769, 106.7009, 1)
		driverstatus.SetVehicle(ctx, rdb, id, "CAR")
		driverstatus.Set(ctx, rdb, id, driverstatus.Available)

		var wg sync.WaitGroup
		results := make([]driverstatus.ClaimResult, 8)
		start := make(chan struct{})
		for i := range results {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results[i], _ = driverstatus.ClaimForOrder(ctx, rdb, fmt.Sprintf("o-%d-%d", round, i), id, "CAR", time.Minute)
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			applyDriverReview(ctx, env, rdb, review(id, events.ReviewSuspended, events.VehicleCar, reviewBase))
		}()
		close(start)
		wg.Wait()

		won := 0
		for _, r := range results {
			if r == driverstatus.Claimed {
				won++
			}
		}
		s, _ := driverstatus.Get(ctx, rdb, id)
		if won > 1 || (won == 1 && s != driverstatus.Assigned) || (won == 0 && s != driverstatus.Offline) {
			t.Fatalf("round %d: %d claims won, status %s", round, won, s)
		}
		// Whatever happened, nothing claims them now.
		if r, _ := driverstatus.ClaimForOrder(ctx, rdb, fmt.Sprintf("late-%d", round), id, "CAR", time.Minute); r == driverstatus.Claimed {
			t.Fatalf("round %d: a claim after the suspension succeeded", round)
		}
	}
}

// ---------------------------------------------------------------- through Kafka

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

func TestKafkaReviewResultsReachRedis(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	rdb, env := setup(t)
	env.KafkaBrokers = []string{brokers}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &kafka.Client{Addr: kafka.TCP(brokers), Timeout: 10 * time.Second}
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{
		{Topic: events.TopicDriverApplicationReviewed, NumPartitions: 3, ReplicationFactor: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	for topic, err := range resp.Errors {
		if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
			t.Fatalf("create %s: %v", topic, err)
		}
	}

	registry := httptest.NewServer(&fakeRegistry{})
	defer registry.Close()
	codec := eventcodec.New(registry.URL)

	// A group of its own, so earlier runs' offsets do not matter.
	var wg sync.WaitGroup
	wg.Go(func() {
		cfg := reviewConsumerConfig(env)
		cfg.GroupID = "matching-review-test-" + fmt.Sprint(time.Now().UnixNano())
		runReviewConsumerWith(ctx, cfg, env, rdb, codec)
	})
	defer func() { cancel(); wg.Wait() }()

	w := kafkaconn.Writer(&kafka.Writer{Addr: kafka.TCP(brokers), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll})
	defer w.Close()
	publish := func(evt events.DriverApplicationReviewed) {
		t.Helper()
		payload, err := codec.Encode(ctx, events.TopicDriverApplicationReviewed, evt)
		if err != nil {
			t.Fatal(err)
		}
		var werr error
		for range 10 {
			if werr = w.WriteMessages(ctx, kafka.Message{Topic: events.TopicDriverApplicationReviewed, Key: []byte(evt.DriverID), Value: payload}); werr == nil {
				return
			}
			time.Sleep(300 * time.Millisecond)
		}
		t.Fatal(werr)
	}
	eventually := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	id := fmt.Sprintf("kafka-driver-%d", time.Now().UnixNano())
	status := func() driverstatus.Status { s, _ := driverstatus.Get(ctx, rdb, id); return s }
	now := time.Now().UTC()
	publish(review(id, events.ReviewApproved, events.VehicleBike, now))
	eventually("the approval", func() bool { return status() == driverstatus.Available })
	// An older suspension arriving afterwards is ignored; a newer one takes effect.
	publish(review(id, events.ReviewSuspended, events.VehicleBike, now.Add(-time.Minute)))
	publish(review(id, events.ReviewSuspended, events.VehicleBike, now.Add(time.Minute)))
	eventually("the suspension", func() bool { return status() == driverstatus.Offline })
	if on, _ := driverstatus.Suspended(ctx, rdb, id); !on {
		t.Error("not marked suspended")
	}
}
