package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/events"
)

type fakeProducer struct {
	sent []kafka.Message
	err  error
}

func (f *fakeProducer) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, msgs...)
	return nil
}

// fakeEncoder turns an event into a readable string: "matched:<driver>:<rider>"
// or "none:<rider>".
type fakeEncoder struct{}

func (fakeEncoder) Encode(_ context.Context, _ string, v any) ([]byte, error) {
	switch e := v.(type) {
	case events.OrderMatched:
		return []byte("matched:" + e.DriverID + ":" + e.RiderID), nil
	case events.OrderNoDriverFound:
		return []byte("none:" + e.RiderID), nil
	}
	return nil, errors.New("unexpected event")
}

// These tests need a real Redis (miniredis rejects the duplicated options
// go-redis sends with GEOSEARCH): set TEST_REDIS_ADDR, e.g. localhost:6379.
// They FLUSH that Redis, so point it at a throwaway instance.
func setup(t *testing.T) (*redis.Client, Env) {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	ctx := context.Background()
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	// Two cars near the pickup point; driver-1 is the nearer.
	for _, d := range []struct {
		id       string
		lat, lng float64
	}{{"driver-1", 10.7769, 106.7009}, {"driver-2", 10.7800, 106.7100}} {
		if _, err := driverlocation.Update(ctx, rdb, d.id, d.lat, d.lng, 1); err != nil {
			t.Fatal(err)
		}
		driverstatus.SetVehicle(ctx, rdb, d.id, "CAR")
		driverstatus.Set(ctx, rdb, d.id, driverstatus.Available)
	}
	return rdb, Env{InstanceID: "test", MatchRadiusKm: 5, MatchRetries: 0, AssignLease: time.Minute}
}

func order(id string) events.OrderCreated {
	return events.OrderCreated{
		OrderID:     id,
		RiderID:     "rider-1",
		Pickup:      events.LatLng{Lat: 10.7769, Lng: 106.7009},
		VehicleType: events.VehicleCar,
	}
}

func TestMatchOrderClaimsTheNearestDriverAndAnnouncesIt(t *testing.T) {
	rdb, env := setup(t)
	p := &fakeProducer{}

	if err := matchOrder(context.Background(), env, rdb, p, fakeEncoder{}, order("o1")); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || p.sent[0].Topic != events.TopicOrderMatched || string(p.sent[0].Value) != "matched:driver-1:rider-1" {
		t.Fatalf("sent %v, want one order.matched for driver-1", p.sent)
	}
}

// The crash this guards against: the matcher published order.matched but died
// before committing the offset, so order.created arrives again.
func TestARedeliveredOrderKeepsItsDriverInsteadOfClaimingAnother(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	p := &fakeProducer{}

	if err := matchOrder(ctx, env, rdb, p, fakeEncoder{}, order("o1")); err != nil {
		t.Fatal(err)
	}
	if err := matchOrder(ctx, env, rdb, p, fakeEncoder{}, order("o1")); err != nil {
		t.Fatal(err)
	}

	for i, m := range p.sent {
		if string(m.Value) != "matched:driver-1:rider-1" {
			t.Errorf("announcement %d = %s, want driver-1 both times", i, m.Value)
		}
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-2"); s != driverstatus.Available {
		t.Errorf("driver-2 is %s: the duplicate took a second driver", s)
	}
}

// The other crash: claimed the driver, died before publishing. The redelivery
// must finish the job rather than leave the driver ASSIGNED.
func TestAMatchThatWasNeverAnnouncedIsAnnouncedOnRedelivery(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()

	res, _ := driverstatus.ClaimForOrder(ctx, rdb, "o1", "driver-1", "CAR", time.Minute)
	if res != driverstatus.Claimed {
		t.Fatal("setup claim failed")
	}

	p := &fakeProducer{}
	if err := matchOrder(ctx, env, rdb, p, fakeEncoder{}, order("o1")); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || string(p.sent[0].Value) != "matched:driver-1:rider-1" {
		t.Fatalf("sent %v, want the pending match announced", p.sent)
	}
}

func TestAFailedPublishReleasesTheDriverAndTheMatchRecord(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()

	if err := matchOrder(ctx, env, rdb, &fakeProducer{err: errors.New("down")}, fakeEncoder{}, order("o1")); err == nil {
		t.Fatal("publish error was swallowed")
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.Available {
		t.Errorf("driver-1 is %s after a failed publish, want AVAILABLE", s)
	}

	p := &fakeProducer{}
	if err := matchOrder(ctx, env, rdb, p, fakeEncoder{}, order("o1")); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 {
		t.Fatalf("retry sent %d events, want 1", len(p.sent))
	}
}

func TestNoDriverNearbyReportsNoDriverFound(t *testing.T) {
	rdb, env := setup(t)
	far := order("o1")
	far.Pickup = events.LatLng{Lat: 21.0, Lng: 105.8} // Hanoi, nowhere near the seeded drivers
	p := &fakeProducer{}

	if err := matchOrder(context.Background(), env, rdb, p, fakeEncoder{}, far); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || p.sent[0].Topic != events.TopicOrderNoDriverFound || string(p.sent[0].Value) != "none:rider-1" {
		t.Fatalf("sent %v, want one order.no_driver_found naming the rider", p.sent)
	}
}

func TestMatchOutcomesAndLatencyAreRecorded(t *testing.T) {
	rdb, env := setup(t)
	matched := func() float64 { return testutil.ToFloat64(matchOutcomes.WithLabelValues("matched")) }
	none := func() float64 { return testutil.ToFloat64(matchOutcomes.WithLabelValues("no_driver")) }
	m0, n0 := matched(), none()

	o := order("o-metrics")
	o.OccurredAt = time.Now().Add(-2 * time.Second).UTC().Format(time.RFC3339)
	if err := matchOrder(context.Background(), env, rdb, &fakeProducer{}, fakeEncoder{}, o); err != nil {
		t.Fatal(err)
	}
	far := order("o-far")
	far.Pickup = events.LatLng{Lat: 21.0, Lng: 105.8}
	if err := matchOrder(context.Background(), env, rdb, &fakeProducer{}, fakeEncoder{}, far); err != nil {
		t.Fatal(err)
	}

	if matched()-m0 != 1 || none()-n0 != 1 {
		t.Errorf("matched moved by %v and no_driver by %v, want 1 and 1", matched()-m0, none()-n0)
	}
	if testutil.CollectAndCount(matchLatency) == 0 {
		t.Error("no match latency was recorded")
	}
}
