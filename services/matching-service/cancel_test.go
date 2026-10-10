package main

import (
	"context"
	"testing"

	"grab-mini/pkg/driverstatus"
)

// These need a real Redis, like the rest of the matching tests (see setup).

func TestACancelledOrderIsNotMatchedAndNoDriverIsClaimed(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	p := &fakeProducer{}

	if err := markOrderCancelled(ctx, rdb, "o1"); err != nil {
		t.Fatal(err)
	}
	if err := matchOrder(ctx, env, rdb, p, fakeEncoder{}, order("o1")); err != nil {
		t.Fatal(err)
	}

	if len(p.sent) != 0 {
		t.Errorf("sent %v, want nothing: neither order.matched nor order.no_driver_found for a cancelled order", p.sent)
	}
	for _, d := range []string{"driver-1", "driver-2"} {
		if s, _ := driverstatus.Get(ctx, rdb, d); s != driverstatus.Available {
			t.Errorf("%s is %s, want AVAILABLE (nobody should be claimed for a cancelled order)", d, s)
		}
	}
	if d, _ := driverstatus.MatchedDriver(ctx, rdb, "o1"); d != "" {
		t.Errorf("order recorded as matched to %s", d)
	}
}

func TestCancellingOneOrderDoesNotStopOthers(t *testing.T) {
	rdb, env := setup(t)
	ctx := context.Background()
	p := &fakeProducer{}

	markOrderCancelled(ctx, rdb, "o1")
	if err := matchOrder(ctx, env, rdb, p, fakeEncoder{}, order("o2")); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || string(p.sent[0].Value) != "matched:driver-1:rider-1" {
		t.Errorf("sent %v, want o2 matched as usual", p.sent)
	}
}

func TestTheCancelledMarkIsPerOrderAndExpires(t *testing.T) {
	rdb, _ := setup(t)
	ctx := context.Background()

	if got, err := orderIsCancelled(ctx, rdb, "o1"); err != nil || got {
		t.Fatalf("orderIsCancelled = %v, %v before any cancellation", got, err)
	}
	markOrderCancelled(ctx, rdb, "o1")
	if got, _ := orderIsCancelled(ctx, rdb, "o1"); !got {
		t.Error("the cancellation wasn't recorded")
	}
	if got, _ := orderIsCancelled(ctx, rdb, "o2"); got {
		t.Error("another order reads as cancelled")
	}
	if ttl := rdb.TTL(ctx, cancelledKey("o1")).Val(); ttl <= 0 {
		t.Errorf("ttl = %v, want the key to expire", ttl)
	}
}
