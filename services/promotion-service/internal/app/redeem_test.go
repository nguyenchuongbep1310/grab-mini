package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"grab-mini/promotion-service/internal/domain"
)

const (
	ev1 = "11111111-1111-4111-8111-111111111111"
	ev2 = "22222222-2222-4222-8222-222222222222"
)

func order(eventID, orderID, code string, discount int64) RedeemOrderCommand {
	return RedeemOrderCommand{EventID: eventID, OrderID: orderID, RiderID: "rider-1", PromoCode: code, Discount: discount}
}

// newEventRig is a rig whose database knows WELCOME10.
func newEventRig(t *testing.T) *rig {
	t.Helper()
	r := newRig(t)
	r.db.promos["WELCOME10"] = percent10("WELCOME10")
	return r
}

func wantCalls(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("calls\n got %v\nwant %v", got, want)
	}
}

func wantMetrics(t *testing.T, m *fakeMetrics, want ...string) {
	t.Helper()
	if len(m.events) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(m.events, want) {
		t.Fatalf("metrics = %v, want %v", m.events, want)
	}
}

func TestAnOrderWithACodeIsRedeemedOnceAndAnnounced(t *testing.T) {
	r := newEventRig(t)
	logs := capture(t)
	if err := r.svc.RedeemOrder(context.Background(), order(ev1, "order-1", "WELCOME10", 10000)); err != nil {
		t.Fatal(err)
	}
	row := r.db.row("WELCOME10", "order-1")
	if row == nil {
		t.Fatal("no redemption recorded")
	}
	want := NewRedemption{OrderID: "order-1", Code: "WELCOME10", RiderID: "rider-1", Discount: 10000,
		StoredStatus: domain.StatusRedeemed, SourceEventID: ev1}
	if !reflect.DeepEqual(row.NewRedemption, want) {
		t.Errorf("row = %+v, want %+v", row.NewRedemption, want)
	}
	if len(r.enc.events) != 1 {
		t.Fatalf("%d events encoded", len(r.enc.events))
	}
	evt := r.enc.events[0]
	if _, err := uuid.Parse(evt.EventID); err != nil || evt.EventID == ev1 {
		t.Errorf("event id %q: a fresh UUID is wanted", evt.EventID)
	}
	evt.EventID = ""
	wantEvt := RedeemedEvent{OccurredAt: t0, OrderID: "order-1", RiderID: "rider-1", Code: "WELCOME10", Discount: 10000,
		Status: domain.StatusRedeemed, Currency: domain.CurrencyVND}
	if !reflect.DeepEqual(evt, wantEvt) {
		t.Errorf("event = %+v, want %+v", evt, wantEvt)
	}
	if len(r.db.state.outbox) != 1 || r.db.state.outbox[0].OrderID != "order-1" || string(r.db.state.outbox[0].Payload) != "avro:order-1:REDEEMED" {
		t.Errorf("outbox = %+v", r.db.state.outbox)
	}
	if !r.db.state.markers[ev1] {
		t.Error("event not marked as processed")
	}
	if r.relay.wakes != 1 {
		t.Errorf("%d wakes, want 1", r.relay.wakes)
	}
	wantMetrics(t, r.m, "redemption:REDEEMED:")
	if logs.String() != "order order-1 redeemed WELCOME10 for 10000 VND\n" {
		t.Errorf("log = %q", logs)
	}
}

func TestOrdersWithoutACodeAreNotRecordedAtAll(t *testing.T) {
	r := newEventRig(t)
	logs := capture(t)
	// Not even the order id is checked: an order without a code is none of our business.
	for _, c := range []RedeemOrderCommand{order(ev1, "order-1", "", 0), {EventID: "x"}} {
		if err := r.svc.RedeemOrder(context.Background(), c); err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
	}
	wantCalls(t, r.db.calls)
	wantMetrics(t, r.m)
	if logs.Len() != 0 || r.clock.reads != 0 {
		t.Errorf("log %q, clock reads %d", logs, r.clock.reads)
	}
}

func TestRedeliveryAndReplayDoNotRedeemTwice(t *testing.T) {
	r := newEventRig(t)
	ctx := context.Background()
	if err := r.svc.RedeemOrder(ctx, order(ev1, "order-1", "WELCOME10", 10000)); err != nil {
		t.Fatal(err)
	}
	logs := capture(t)
	r.db.calls = nil

	// The same event again: the marker says so, nothing else is touched.
	if err := r.svc.RedeemOrder(ctx, order(ev1, "order-1", "WELCOME10", 10000)); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, r.db.calls, "begin", "MarkEventProcessed", "commit")
	if logs.String() != "duplicate event "+ev1+", skipping\n" {
		t.Errorf("log = %q", logs)
	}
	logs.Reset()
	r.db.calls = nil

	// Another event for the same order: (code, order_id) says so; the marker is kept.
	if err := r.svc.RedeemOrder(ctx, order(ev2, "order-1", "WELCOME10", 10000)); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, r.db.calls, "begin", "MarkEventProcessed", "LockOrder", "FindPromotionForUpdate", "OrderWasCancelled", "Usage", "InsertRedemption", "commit")
	if logs.Len() != 0 {
		t.Errorf("log = %q", logs)
	}
	if !r.db.state.markers[ev2] {
		t.Error("the second event's marker was not kept")
	}
	if len(r.db.state.redemptions) != 1 || len(r.db.state.outbox) != 1 || len(r.enc.events) != 1 {
		t.Errorf("%d redemptions, %d queued, %d encoded; want 1 each", len(r.db.state.redemptions), len(r.db.state.outbox), len(r.enc.events))
	}
	if r.relay.wakes != 1 {
		t.Errorf("%d wakes, want only the first", r.relay.wakes)
	}
	wantMetrics(t, r.m, "redemption:REDEEMED:", "duplicate", "duplicate")
}

func TestEventsThatCanNeverBeAppliedArePermanentErrors(t *testing.T) {
	ctx := context.Background()
	badID := order("not-a-uuid", "order-1", "WELCOME10", 1000)
	for name, c := range map[string]struct {
		run   func(*Service) error
		msg   string
		began bool
	}{
		"unknown code": {func(s *Service) error { return s.RedeemOrder(ctx, order(ev1, "order-1", "NOSUCHCODE", 1000)) },
			`order order-1 booked with unknown promotion "NOSUCHCODE"`, true},
		"no order id": {func(s *Service) error { return s.RedeemOrder(ctx, order(ev1, "", "WELCOME10", 1000)) },
			"order.created without orderId or riderId", false},
		"no rider": {func(s *Service) error {
			return s.RedeemOrder(ctx, RedeemOrderCommand{EventID: ev1, OrderID: "order-1", PromoCode: "WELCOME10", Discount: 1000})
		}, "order.created without orderId or riderId", false},
		"negative discount": {func(s *Service) error { return s.RedeemOrder(ctx, order(ev1, "order-1", "WELCOME10", -1)) },
			"discount -1 is not an amount of VND", false},
		"bad event id": {func(s *Service) error { return s.RedeemOrder(ctx, badID) },
			"eventId not-a-uuid is not a UUID", true},
		"cancel without order": {func(s *Service) error { return s.OnOrderCancelled(ctx, ReleaseCommand{EventID: ev1}) },
			"order.cancelled without orderId", false},
	} {
		t.Run(name, func(t *testing.T) {
			r := newEventRig(t)
			logs := capture(t)
			err := c.run(r.svc)
			var p interface{ Permanent() bool }
			if !errors.As(err, &p) || !p.Permanent() {
				t.Fatalf("error = %v, want a permanent one", err)
			}
			var inv *domain.InvalidEvent
			if !errors.As(err, &inv) || inv.Reason != c.msg {
				t.Fatalf("error = %v, want reason %q", err, c.msg)
			}
			// Nothing kept: an unknown code must not be marked as seen, so a redrive
			// after the promotion exists can still apply it.
			if len(r.db.state.markers)+len(r.db.state.redemptions)+len(r.db.state.outbox) != 0 {
				t.Errorf("left behind: %+v", r.db.state)
			}
			if c.began {
				if n := len(r.db.calls); n == 0 || r.db.calls[n-1] != "rollback" {
					t.Errorf("calls %v, want a rollback", r.db.calls)
				}
			} else if len(r.db.calls) != 0 {
				t.Errorf("calls %v, want none: refused before the transaction", r.db.calls)
			}
			if name == "bad event id" {
				wantCalls(t, r.db.calls, "begin", "rollback") // checked before the marker
			}
			wantMetrics(t, r.m)
			if logs.Len() != 0 || r.relay.wakes != 0 {
				t.Errorf("log %q, %d wakes", logs, r.relay.wakes)
			}
		})
	}
}

func TestACodeTypedInLowerCaseIsRedeemedAgainstTheRealOne(t *testing.T) {
	r := newEventRig(t)
	logs := capture(t)
	if err := r.svc.RedeemOrder(context.Background(), order(ev1, "order-1", " welcome10 ", 10000)); err != nil {
		t.Fatal(err)
	}
	if row := r.db.row("WELCOME10", "order-1"); row == nil || row.StoredStatus != domain.StatusRedeemed {
		t.Fatalf("row = %+v", row)
	}
	if r.enc.events[0].Code != "WELCOME10" {
		t.Errorf("announced code %q", r.enc.events[0].Code)
	}
	if logs.String() != "order order-1 redeemed WELCOME10 for 10000 VND\n" {
		t.Errorf("log = %q", logs)
	}
}

func TestACancelledOrderIsRecordedAsReleasedWithTheApplicationClockAndNothingIsAnnounced(t *testing.T) {
	r := newEventRig(t)
	logs := capture(t)
	r.db.state.cancelled["order-3"] = true
	if err := r.svc.RedeemOrder(context.Background(), order(ev1, "order-3", "WELCOME10", 10000)); err != nil {
		t.Fatal(err)
	}
	// The usage is not consulted for an order that never took effect.
	wantCalls(t, r.db.calls, "begin", "MarkEventProcessed", "LockOrder", "FindPromotionForUpdate", "OrderWasCancelled", "InsertRedemption", "commit", "Wake")
	row := r.db.row("WELCOME10", "order-3")
	if row == nil || row.StoredStatus != domain.StatusReleased || row.OverLimitReason != "" {
		t.Fatalf("row = %+v", row)
	}
	if row.ReleasedAt == nil || !row.ReleasedAt.Equal(t0) {
		t.Errorf("released_at = %v, want the application clock %v", row.ReleasedAt, t0)
	}
	if r.clock.reads != 1 {
		t.Errorf("clock read %d times, want once", r.clock.reads)
	}
	if len(r.enc.events) != 0 || len(r.db.state.outbox) != 0 {
		t.Errorf("announced: %d encoded, %d queued", len(r.enc.events), len(r.db.state.outbox))
	}
	// Nothing was queued, but the relay is still woken after the commit, as the flat code did.
	if r.relay.wakes != 1 {
		t.Errorf("%d wakes, want 1", r.relay.wakes)
	}
	wantMetrics(t, r.m, "released:1")
	if logs.String() != "order order-3 used WELCOME10 but was already cancelled: recorded as released\n" {
		t.Errorf("log = %q", logs)
	}
}

func TestAnOverLimitRedemptionIsRecordedAndAnnouncedWithItsReason(t *testing.T) {
	r := newEventRig(t)
	logs := capture(t)
	r.db.usage["WELCOME10|rider-1"] = domain.Usage{Total: 1, ByRider: 1}
	if err := r.svc.RedeemOrder(context.Background(), order(ev1, "order-2", "WELCOME10", 10000)); err != nil {
		t.Fatal(err)
	}
	row := r.db.row("WELCOME10", "order-2")
	if row == nil || row.StoredStatus != domain.StatusOverLimit || row.OverLimitReason != domain.ReasonPerRiderLimit || row.ReleasedAt != nil {
		t.Fatalf("row = %+v", row)
	}
	if len(r.enc.events) != 1 || r.enc.events[0].Status != domain.StatusOverLimit || r.enc.events[0].Discount != 10000 {
		t.Fatalf("events = %+v", r.enc.events)
	}
	if len(r.db.state.outbox) != 1 || r.relay.wakes != 1 {
		t.Errorf("%d queued, %d wakes", len(r.db.state.outbox), r.relay.wakes)
	}
	wantMetrics(t, r.m, "redemption:OVER_LIMIT:per_rider_limit")
	if logs.String() != "order order-2 used WELCOME10 past its per_rider_limit: recorded as OVER_LIMIT (discount 10000 VND stays on the order)\n" {
		t.Errorf("log = %q", logs)
	}
}

func TestTheCallOrderInsideTheTransactionIsMarkLockFindCancelledUsageInsertEncodeQueue(t *testing.T) {
	r := newEventRig(t)
	capture(t)
	if err := r.svc.RedeemOrder(context.Background(), order(ev1, "order-1", "WELCOME10", 10000)); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, r.db.calls, "begin", "MarkEventProcessed", "LockOrder", "FindPromotionForUpdate", "OrderWasCancelled",
		"Usage", "InsertRedemption", "Encode", "QueueRedeemed", "commit", "Wake")
}

func TestTheEncodeRunsInsideTheTransaction(t *testing.T) {
	r := newEventRig(t)
	capture(t)
	if err := r.svc.RedeemOrder(context.Background(), order(ev1, "order-1", "WELCOME10", 10000)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.enc.inTx, []bool{true}) {
		t.Errorf("encoder ran in a transaction: %v, want [true]", r.enc.inTx)
	}
}

func TestNoCounterAndNoWakeWhenTheCommitFails(t *testing.T) {
	ctx := context.Background()
	for name, setup := range map[string]func(*rig){
		"redeemed":   func(*rig) {},
		"over limit": func(r *rig) { r.db.usage["WELCOME10|rider-1"] = domain.Usage{ByRider: 1} },
		"cancelled":  func(r *rig) { r.db.state.cancelled["order-1"] = true },
	} {
		t.Run(name, func(t *testing.T) {
			r := newEventRig(t)
			logs := capture(t)
			setup(r)
			r.db.fail["commit"] = errBoom
			err := r.svc.RedeemOrder(ctx, order(ev1, "order-1", "WELCOME10", 10000))
			if !errors.Is(err, errBoom) {
				t.Fatalf("err = %v", err)
			}
			wantMetrics(t, r.m)
			if r.relay.wakes != 0 || logs.Len() != 0 {
				t.Errorf("%d wakes, log %q", r.relay.wakes, logs)
			}
			if len(r.db.state.markers)+len(r.db.state.redemptions)+len(r.db.state.outbox) != 0 {
				t.Errorf("not rolled back: %+v", r.db.state)
			}
		})
	}
	t.Run("release", func(t *testing.T) {
		r := newEventRig(t)
		if err := r.svc.RedeemOrder(ctx, order(ev1, "order-1", "WELCOME10", 10000)); err != nil {
			t.Fatal(err)
		}
		logs := capture(t)
		r.m.events = nil
		r.db.fail["commit"] = errBoom
		if err := r.svc.OnOrderCancelled(ctx, ReleaseCommand{EventID: ev2, OrderID: "order-1"}); !errors.Is(err, errBoom) {
			t.Fatalf("err = %v", err)
		}
		wantMetrics(t, r.m)
		if logs.Len() != 0 || r.db.row("WELCOME10", "order-1").StoredStatus != domain.StatusRedeemed {
			t.Errorf("log %q, row %+v", logs, r.db.row("WELCOME10", "order-1"))
		}
	})
}

func TestWakeAfterEveryRecordingCommit(t *testing.T) {
	r := newEventRig(t)
	capture(t)
	ctx := context.Background()
	steps := []struct {
		name  string
		run   func() error
		wakes int
	}{
		{"redeemed", func() error { return r.svc.RedeemOrder(ctx, order(ev1, "order-1", "WELCOME10", 10000)) }, 1},
		{"redelivered", func() error { return r.svc.RedeemOrder(ctx, order(ev1, "order-1", "WELCOME10", 10000)) }, 1},
		{"replayed", func() error { return r.svc.RedeemOrder(ctx, order(ev2, "order-1", "WELCOME10", 10000)) }, 1},
		{"released", func() error {
			return r.svc.OnOrderCancelled(ctx, ReleaseCommand{EventID: "33333333-3333-4333-8333-333333333333", OrderID: "order-1"})
		}, 1},
		{"cancelled first", func() error {
			return r.svc.OnNoDriverFound(ctx, ReleaseCommand{EventID: "44444444-4444-4444-8444-444444444444", OrderID: "order-2"})
		}, 1},
		{"recorded as released", func() error {
			return r.svc.RedeemOrder(ctx, order("55555555-5555-4555-8555-555555555555", "order-2", "WELCOME10", 10000))
		}, 2},
		{"recorded as released, redelivered", func() error {
			return r.svc.RedeemOrder(ctx, order("55555555-5555-4555-8555-555555555555", "order-2", "WELCOME10", 10000))
		}, 2},
	}
	for _, s := range steps {
		if err := s.run(); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if r.relay.wakes != s.wakes {
			t.Fatalf("after %s: %d wakes, want %d", s.name, r.relay.wakes, s.wakes)
		}
	}
}

func TestAnEncodeErrorRollsBackAndIsNotPermanent(t *testing.T) {
	r := newEventRig(t)
	logs := capture(t)
	r.enc.fail = errBoom
	err := r.svc.RedeemOrder(context.Background(), order(ev1, "order-1", "WELCOME10", 10000))
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v", err)
	}
	var p interface{ Permanent() bool }
	if errors.As(err, &p) && p.Permanent() {
		t.Fatalf("err = %v is permanent; an encode failure is retried", err)
	}
	wantCalls(t, r.db.calls, "begin", "MarkEventProcessed", "LockOrder", "FindPromotionForUpdate", "OrderWasCancelled",
		"Usage", "InsertRedemption", "Encode", "rollback")
	if len(r.db.state.markers)+len(r.db.state.redemptions)+len(r.db.state.outbox) != 0 {
		t.Errorf("not rolled back: %+v", r.db.state)
	}
	wantMetrics(t, r.m)
	if r.relay.wakes != 0 || logs.Len() != 0 {
		t.Errorf("%d wakes, log %q", r.relay.wakes, logs)
	}
}

func TestAPortFailureRollsBackEverythingAndIsRetried(t *testing.T) {
	for _, call := range []string{"MarkEventProcessed", "LockOrder", "FindPromotionForUpdate", "OrderWasCancelled", "Usage", "InsertRedemption", "QueueRedeemed"} {
		t.Run(call, func(t *testing.T) {
			r := newEventRig(t)
			logs := capture(t)
			r.db.fail[call] = errBoom
			err := r.svc.RedeemOrder(context.Background(), order(ev1, "order-1", "WELCOME10", 10000))
			if err != errBoom {
				t.Fatalf("err = %v, want the port's error as it is", err)
			}
			if n := len(r.db.calls); r.db.calls[n-2] != call || r.db.calls[n-1] != "rollback" {
				t.Errorf("calls = %v", r.db.calls)
			}
			if len(r.db.state.markers)+len(r.db.state.redemptions)+len(r.db.state.outbox) != 0 {
				t.Errorf("not rolled back: %+v", r.db.state)
			}
			wantMetrics(t, r.m)
			if r.relay.wakes != 0 || logs.Len() != 0 {
				t.Errorf("%d wakes, log %q", r.relay.wakes, logs)
			}
		})
	}
}

func TestANilWakerIsAccepted(t *testing.T) {
	clock := &fakeClock{at: t0}
	db := newFakeDB(clock)
	db.promos["WELCOME10"] = percent10("WELCOME10")
	m := &fakeMetrics{}
	svc := New(Deps{UoW: db, Encoder: &fakeEncoder{db: db}, Metrics: m, Now: clock.Now, Relay: nil})
	capture(t)
	if err := svc.RedeemOrder(context.Background(), order(ev1, "order-1", "WELCOME10", 10000)); err != nil {
		t.Fatal(err)
	}
	if err := svc.OnOrderCancelled(context.Background(), ReleaseCommand{EventID: ev2, OrderID: "order-1"}); err != nil {
		t.Fatal(err)
	}
	wantMetrics(t, m, "redemption:REDEEMED:", "released:1")
}
