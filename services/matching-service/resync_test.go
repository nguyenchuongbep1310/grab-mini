package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
	"grab-mini/pkg/events"
)

// Profile Service's resync re-publishes the current status of every APPROVED, REJECTED or
// SUSPENDED driver as driver.application.reviewed (reviewedBy "resync"), dated after the
// driver's last result. These tests show what Matching does with such an event, with
// Redis intact and after Redis lost everything. They need TEST_REDIS_ADDR (a throwaway
// Redis, like the rest of this package: setup flushes it).

func resyncEvent(driver, status string, vehicle events.VehicleType, at time.Time) events.DriverApplicationReviewed {
	e := review(driver, status, vehicle, at)
	e.ReviewedBy, e.Reason = "resync", "resync"
	return e
}

func claimable(t *testing.T, rdb *redis.Client, driver, vehicle string) bool {
	t.Helper()
	res, err := driverstatus.ClaimForOrder(context.Background(), rdb, "probe-"+driver, driver, vehicle, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return res == driverstatus.Claimed
}

func TestAResyncWithRedisIntactChangesNothingForAnAlreadyEligibleDriver(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	if err := applyDriverReview(ctx, env, rdb, review("driver-1", events.ReviewApproved, events.VehicleCar, reviewBase)); err != nil {
		t.Fatal(err)
	}
	if err := applyDriverReview(ctx, env, rdb, resyncEvent("driver-1", events.ReviewApproved, events.VehicleCar, reviewBase.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.Available {
		t.Errorf("driver-1 = %s, want AVAILABLE as before", s)
	}
	if !claimable(t, rdb, "driver-1", "CAR") {
		t.Error("driver-1 is not claimable after an idempotent resync")
	}
}

func TestAResyncNeverMakesABusyDriverAvailable(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	driverstatus.ClaimForOrder(ctx, rdb, "o1", "driver-1", "CAR", time.Minute) // ASSIGNED
	driverstatus.Set(ctx, rdb, "driver-2", driverstatus.OnTrip)
	for _, id := range []string{"driver-1", "driver-2"} {
		if err := applyDriverReview(ctx, env, rdb, resyncEvent(id, events.ReviewApproved, events.VehicleCar, reviewBase)); err != nil {
			t.Fatal(err)
		}
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.Assigned {
		t.Errorf("driver-1 = %s, want ASSIGNED untouched", s)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-2"); s != driverstatus.OnTrip {
		t.Errorf("driver-2 = %s, want ON_TRIP untouched", s)
	}
	if claimable(t, rdb, "driver-2", "CAR") {
		t.Error("an ON_TRIP driver was claimed after the resync")
	}
}

func TestAResyncNeverUnsuspendsAndAnOlderOneIsIgnored(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	if err := applyDriverReview(ctx, env, rdb, review("driver-1", events.ReviewSuspended, events.VehicleCar, reviewBase)); err != nil {
		t.Fatal(err)
	}
	if err := applyDriverReview(ctx, env, rdb, resyncEvent("driver-1", events.ReviewSuspended, events.VehicleCar, reviewBase.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.Offline {
		t.Errorf("driver-1 = %s, want OFFLINE", s)
	}
	if claimable(t, rdb, "driver-1", "CAR") {
		t.Error("a suspended driver was claimed after the resync")
	}
	// A resync event older than a later real decision (the admin approved after the resync
	// was queued) must not take that decision back.
	if err := applyDriverReview(ctx, env, rdb, review("driver-1", events.ReviewApproved, events.VehicleCar, reviewBase.Add(2*time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := applyDriverReview(ctx, env, rdb, resyncEvent("driver-1", events.ReviewSuspended, events.VehicleCar, reviewBase.Add(90*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if !claimable(t, rdb, "driver-1", "CAR") {
		t.Error("an older resync event undid a newer approval")
	}
}

// A resync for a driver who is busy and suspended lets them finish the trip and stay out.
func TestAResyncOfASuspensionLetsABusyDriverFinishTheTrip(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	driverstatus.Set(ctx, rdb, "driver-1", driverstatus.OnTrip)
	if err := applyDriverReview(ctx, env, rdb, resyncEvent("driver-1", events.ReviewSuspended, events.VehicleCar, reviewBase)); err != nil {
		t.Fatal(err)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.OnTrip {
		t.Errorf("driver-1 = %s, want ON_TRIP", s)
	}
	driverstatus.Set(ctx, rdb, "driver-1", driverstatus.Available) // the trip ends
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.Offline {
		t.Errorf("driver-1 = %s after the trip, want OFFLINE", s)
	}
}

// The case the resync exists for: Redis loses everything (a restore, a failover without
// the last writes). Afterwards the drivers send locations again, but nobody is eligible
// until Profile Service's resync events are applied.
func TestAfterRedisLosesEverythingAResyncRestoresEligibility(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	// Before the loss: driver-1 approved; driver-2 suspended; driver-3 rejected; driver-4 approved bike.
	for _, e := range []events.DriverApplicationReviewed{
		review("driver-1", events.ReviewApproved, events.VehicleCar, reviewBase),
		review("driver-2", events.ReviewSuspended, events.VehicleCar, reviewBase),
		review("driver-3", events.ReviewRejected, events.VehicleCar, reviewBase),
		review("driver-4", events.ReviewApproved, events.VehicleBike, reviewBase),
	} {
		if err := applyDriverReview(ctx, env, rdb, e); err != nil {
			t.Fatal(err)
		}
	}

	// setup flushed a throwaway Redis already; do it again here, and only there.
	addr := rdb.Options().Addr
	if addr == "localhost:6379" || addr == "127.0.0.1:6379" || strings.HasSuffix(addr, ":6379") {
		t.Fatalf("refusing to flush %s: use a throwaway Redis", addr)
	}
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"driver:driver-2:suspended", "driver:driver-1:review_at", "driver:driver-1:vehicle"} {
		if n, _ := rdb.Exists(ctx, k).Result(); n != 0 {
			t.Fatalf("%s survived the flush", k)
		}
	}
	// Drivers reconnect and report where they are.
	for _, d := range []struct {
		id       string
		lat, lng float64
	}{{"driver-1", 10.7769, 106.7009}, {"driver-2", 10.7770, 106.7010}, {"driver-3", 10.7771, 106.7011}, {"driver-4", 10.7772, 106.7012}} {
		if _, err := driverlocation.Update(ctx, rdb, d.id, d.lat, d.lng, 1); err != nil {
			t.Fatal(err)
		}
	}
	// The state a flushed Redis would let through: the suspended driver has no flag,
	// but also no status or vehicle, so nobody is matched.
	p := &fakeProducer{}
	if err := matchOrder(ctx, env, rdb, p, fakeEncoder{}, order("o-before")); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || p.sent[0].Topic != events.TopicOrderNoDriverFound {
		t.Fatalf("before the resync: %v, want no driver found", p.sent)
	}

	// The resync, dated after the earlier results.
	at := reviewBase.Add(time.Hour)
	for _, e := range []events.DriverApplicationReviewed{
		resyncEvent("driver-1", events.ReviewApproved, events.VehicleCar, at),
		resyncEvent("driver-2", events.ReviewSuspended, events.VehicleCar, at),
		resyncEvent("driver-3", events.ReviewRejected, events.VehicleCar, at),
		resyncEvent("driver-4", events.ReviewApproved, events.VehicleBike, at),
	} {
		if err := applyDriverReview(ctx, env, rdb, e); err != nil {
			t.Fatal(err)
		}
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-1"); s != driverstatus.Available {
		t.Errorf("driver-1 = %s, want AVAILABLE", s)
	}
	if s, _ := driverstatus.Get(ctx, rdb, "driver-4"); s != driverstatus.Available {
		t.Errorf("driver-4 = %s, want AVAILABLE", s)
	}
	if v, _ := rdb.Get(ctx, "driver:driver-1:vehicle").Result(); v != "CAR" {
		t.Errorf("driver-1 vehicle = %q, want CAR", v)
	}
	if v, _ := rdb.Get(ctx, "driver:driver-4:vehicle").Result(); v != "BIKE" {
		t.Errorf("driver-4 vehicle = %q, want BIKE", v)
	}
	for _, id := range []string{"driver-2", "driver-3"} {
		if n, _ := rdb.Exists(ctx, "driver:"+id+":suspended").Result(); n != 1 {
			t.Errorf("%s has no suspended flag after the resync", id)
		}
		// Even if something later records a vehicle and AVAILABLE for them (a location
		// update path, a hand-set status), no claim goes through.
		driverstatus.SetVehicle(ctx, rdb, id, "CAR")
		driverstatus.Set(ctx, rdb, id, driverstatus.Available)
		if s, _ := driverstatus.Get(ctx, rdb, id); s != driverstatus.Offline {
			t.Errorf("%s = %s, want OFFLINE", id, s)
		}
		if claimable(t, rdb, id, "CAR") {
			t.Errorf("%s was claimed", id)
		}
	}

	// And the order is matched to the approved car driver, not to the nearer ones that are out.
	p = &fakeProducer{}
	if err := matchOrder(ctx, env, rdb, p, fakeEncoder{}, order("o-after")); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || string(p.sent[0].Value) != "matched:driver-1:rider-1" {
		t.Errorf("after the resync: %v, want driver-1 matched", p.sent)
	}
}
