package app

import (
	"context"
	"errors"
	"testing"

	"grab-mini/promotion-service/internal/domain"
)

const evRelease = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

// redeemed seeds a REDEEMED row for order under code, as an earlier order.created would have.
func (f *fakeDB) redeemed(code, order string) {
	f.state.redemptions = append(f.state.redemptions, redemptionRow{NewRedemption: NewRedemption{
		OrderID: order, Code: code, RiderID: "rider-1", Discount: 10000, StoredStatus: domain.StatusRedeemed, SourceEventID: ev1}})
}

func TestTheReleaseLocksBeforeItReleasesAndRemembersOnlyWhenNothingWasReleased(t *testing.T) {
	r := newEventRig(t)
	capture(t)
	ctx := context.Background()
	r.db.redeemed("WELCOME10", "order-1")
	if err := r.svc.OnOrderCancelled(ctx, ReleaseCommand{EventID: evRelease, OrderID: "order-1"}); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, r.db.calls, "begin", "MarkEventProcessed", "LockOrder", "ReleaseRedemptions", "commit")
	if r.db.state.cancelled["order-1"] {
		t.Error("an order whose redemption was released is remembered as cancelled")
	}

	r.db.calls = nil
	if err := r.svc.OnOrderCancelled(ctx, ReleaseCommand{EventID: ev2, OrderID: "order-2"}); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, r.db.calls, "begin", "MarkEventProcessed", "LockOrder", "ReleaseRedemptions", "RememberCancelled", "commit")
	if !r.db.state.cancelled["order-2"] {
		t.Error("order-2 not remembered as cancelled")
	}
	if r.relay.wakes != 0 || r.clock.reads != 0 {
		t.Errorf("%d wakes, %d clock reads: a release announces nothing and uses the database clock", r.relay.wakes, r.clock.reads)
	}
}

func TestEveryEndWithoutARideReleasesTheRedemption(t *testing.T) {
	ctx := context.Background()
	for name, c := range map[string]struct {
		run func(*Service, context.Context, ReleaseCommand) error
		cmd ReleaseCommand
		log string
	}{
		"order cancelled": {(*Service).OnOrderCancelled, ReleaseCommand{EventID: evRelease, OrderID: "order-4"},
			"order order-4 cancelled (order.cancelled): 1 redemption(s) released\n"},
		"trip cancelled": {(*Service).OnTripStatusChanged, ReleaseCommand{EventID: evRelease, OrderID: "order-4", Status: domain.TripStatusCancelled},
			"order order-4 trip cancelled (trip.status.changed): 1 redemption(s) released\n"},
		"no driver": {(*Service).OnNoDriverFound, ReleaseCommand{EventID: evRelease, OrderID: "order-4"},
			"order order-4 found no driver (order.no_driver_found): 1 redemption(s) released\n"},
	} {
		t.Run(name, func(t *testing.T) {
			r := newEventRig(t)
			logs := capture(t)
			r.db.redeemed("WELCOME10", "order-4")
			if err := c.run(r.svc, ctx, c.cmd); err != nil {
				t.Fatal(err)
			}
			row := r.db.row("WELCOME10", "order-4")
			if row.StoredStatus != domain.StatusReleased || row.DBReleasedAt == nil || !row.DBReleasedAt.Equal(r.db.dbClock) {
				t.Errorf("row = %+v, want RELEASED at the database clock", row)
			}
			if !r.db.state.markers[evRelease] {
				t.Error("event not marked")
			}
			wantMetrics(t, r.m, "released:1")
			if logs.String() != c.log {
				t.Errorf("log = %q, want %q", logs, c.log)
			}
			if r.relay.wakes != 0 || len(r.db.state.outbox) != 0 {
				t.Errorf("%d wakes, %d queued", r.relay.wakes, len(r.db.state.outbox))
			}

			// Redelivered: a counted duplicate, nothing else.
			logs.Reset()
			r.db.calls = nil
			if err := c.run(r.svc, ctx, c.cmd); err != nil {
				t.Fatal(err)
			}
			wantCalls(t, r.db.calls, "begin", "MarkEventProcessed", "commit")
			wantMetrics(t, r.m, "released:1", "duplicate")
			if logs.String() != "duplicate event "+evRelease+", skipping\n" {
				t.Errorf("log = %q", logs)
			}
		})
	}
}

func TestOtherTripStatusesLeaveTheRedemptionAlone(t *testing.T) {
	r := newEventRig(t)
	logs := capture(t)
	r.db.redeemed("WELCOME10", "order-1")
	for _, status := range []string{"COMPLETED", "ACCEPTED", "cancelled", ""} {
		for _, orderID := range []string{"order-1", ""} {
			if err := r.svc.OnTripStatusChanged(context.Background(), ReleaseCommand{EventID: "not-a-uuid", OrderID: orderID, Status: status}); err != nil {
				t.Fatalf("%q/%q: %v", status, orderID, err)
			}
		}
	}
	wantCalls(t, r.db.calls)
	wantMetrics(t, r.m)
	if logs.Len() != 0 || r.db.row("WELCOME10", "order-1").StoredStatus != domain.StatusRedeemed {
		t.Errorf("log %q", logs)
	}
}

func TestReleaseEventsWithoutAnOrderIdArePermanentErrors(t *testing.T) {
	ctx := context.Background()
	for topic, run := range map[string]func(*Service) error{
		"order.cancelled": func(s *Service) error { return s.OnOrderCancelled(ctx, ReleaseCommand{EventID: evRelease}) },
		"trip.status.changed": func(s *Service) error {
			return s.OnTripStatusChanged(ctx, ReleaseCommand{EventID: evRelease, Status: domain.TripStatusCancelled})
		},
		"order.no_driver_found": func(s *Service) error { return s.OnNoDriverFound(ctx, ReleaseCommand{EventID: evRelease}) },
	} {
		r := newEventRig(t)
		logs := capture(t)
		err := run(r.svc)
		var inv *domain.InvalidEvent
		if !errors.As(err, &inv) || !inv.Permanent() || inv.Reason != topic+" without orderId" {
			t.Errorf("%s: err = %v", topic, err)
		}
		if len(r.db.calls) != 0 || logs.Len() != 0 || len(r.m.events) != 0 {
			t.Errorf("%s: calls %v, log %q, metrics %v", topic, r.db.calls, logs, r.m.events)
		}
	}
}

func TestAReleaseWithABadEventIdIsPermanentAndLeavesNoMarker(t *testing.T) {
	r := newEventRig(t)
	logs := capture(t)
	r.db.redeemed("WELCOME10", "order-1")
	err := r.svc.OnNoDriverFound(context.Background(), ReleaseCommand{EventID: "nope", OrderID: "order-1"})
	var inv *domain.InvalidEvent
	if !errors.As(err, &inv) || inv.Reason != "eventId nope is not a UUID" {
		t.Fatalf("err = %v", err)
	}
	wantCalls(t, r.db.calls, "begin", "rollback")
	if len(r.db.state.markers) != 0 || r.db.row("WELCOME10", "order-1").StoredStatus != domain.StatusRedeemed {
		t.Errorf("state = %+v", r.db.state)
	}
	wantMetrics(t, r.m)
	if logs.Len() != 0 || r.relay.wakes != 0 {
		t.Errorf("log %q, %d wakes", logs, r.relay.wakes)
	}
}

func TestAReleaseCountsAndLogsEveryReleasedRow(t *testing.T) {
	r := newEventRig(t)
	logs := capture(t)
	ctx := context.Background()
	r.db.redeemed("WELCOME10", "order-9")
	r.db.redeemed("OTHER", "order-9")
	if err := r.svc.OnOrderCancelled(ctx, ReleaseCommand{EventID: evRelease, OrderID: "order-9"}); err != nil {
		t.Fatal(err)
	}
	wantMetrics(t, r.m, "released:2")
	if logs.String() != "order order-9 cancelled (order.cancelled): 2 redemption(s) released\n" {
		t.Errorf("log = %q", logs)
	}

	// The same order named again by another event: nothing left to release, no counter, no log.
	logs.Reset()
	if err := r.svc.OnTripStatusChanged(ctx, ReleaseCommand{EventID: ev2, OrderID: "order-9", Status: domain.TripStatusCancelled}); err != nil {
		t.Fatal(err)
	}
	wantMetrics(t, r.m, "released:2")
	if logs.Len() != 0 {
		t.Errorf("log = %q", logs)
	}
}

func TestAReleasePortFailureRollsBackAndIsRetried(t *testing.T) {
	for _, call := range []string{"MarkEventProcessed", "LockOrder", "ReleaseRedemptions", "RememberCancelled"} {
		t.Run(call, func(t *testing.T) {
			r := newEventRig(t)
			logs := capture(t)
			r.db.fail[call] = errBoom
			if err := r.svc.OnOrderCancelled(context.Background(), ReleaseCommand{EventID: evRelease, OrderID: "order-1"}); err != errBoom {
				t.Fatalf("err = %v", err)
			}
			if len(r.db.state.markers)+len(r.db.state.cancelled) != 0 {
				t.Errorf("not rolled back: %+v", r.db.state)
			}
			wantMetrics(t, r.m)
			if logs.Len() != 0 {
				t.Errorf("log = %q", logs)
			}
		})
	}
}
