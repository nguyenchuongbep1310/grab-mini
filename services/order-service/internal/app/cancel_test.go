package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"grab-mini/order-service/internal/domain"
)

var rider1 = domain.Actor{Subject: "rider-1", Role: domain.RoleRider}

func (e *testEnv) seedOrder(status domain.Status) string {
	return e.db.seed(domain.Order{
		ID: uuid.NewString(), RiderID: "rider-1", Status: status, VehicleType: domain.VehicleCar,
		Pickup: domain.LatLng{Lat: 1, Lng: 2}, Dropoff: domain.LatLng{Lat: 3, Lng: 4}, SurgeMultiplier: 1,
	})
}

func TestARiderCancelsAPendingOrder(t *testing.T) {
	e := newTestEnv(t)
	id := e.seedOrder(domain.StatusPending)

	res, err := e.svc.CancelOrder(context.Background(), rider1, id, "changed my mind")
	if err != nil || res.Conflict {
		t.Fatalf("CancelOrder = %+v, %v", res, err)
	}
	if o := res.Order; o.Status != domain.StatusCancelled || o.CancelledBy != "RIDER" || o.CancelReason != "changed my mind" || o.CancelledAt == nil {
		t.Errorf("order %+v, want CANCELLED by RIDER", o)
	}
	q := e.db.queued()
	if len(q) != 1 || q[0].topic != "order.cancelled" || q[0].orderID != id {
		t.Errorf("queued %+v, want exactly one order.cancelled", q)
	}
	if got, want := e.enc.cancelled, []OrderCancelled{{OrderID: id, RiderID: "rider-1", CancelledBy: "RIDER", Reason: "changed my mind"}}; len(got) != 1 || got[0] != want[0] {
		t.Errorf("event %+v, want %+v", got, want)
	}
	if e.relay.count() != 1 || e.metrics.cancelled != 1 {
		t.Errorf("wakes=%d cancelled=%d", e.relay.count(), e.metrics.cancelled)
	}
}

func TestCancellingAnOrderTwiceIsAnIdempotentSuccess(t *testing.T) {
	e := newTestEnv(t)
	id := e.seedOrder(domain.StatusPending)

	for i := 0; i < 3; i++ {
		res, err := e.svc.CancelOrder(context.Background(), rider1, id, "x")
		if err != nil || res.Conflict || res.Order.Status != domain.StatusCancelled {
			t.Fatalf("cancel #%d = %+v, %v; want CANCELLED", i+1, res, err)
		}
	}
	if len(e.db.queued()) != 1 || e.relay.count() != 1 || e.metrics.cancelled != 1 {
		t.Errorf("queued=%v wakes=%d cancelled=%d: a repeat must not announce the cancellation again",
			e.db.queued(), e.relay.count(), e.metrics.cancelled)
	}
}

func TestAMatchedOrderCannotBeCancelledHere(t *testing.T) {
	e := newTestEnv(t)
	id := e.seedOrder(domain.StatusMatched)

	res, err := e.svc.CancelOrder(context.Background(), rider1, id, "")
	if err != nil || !res.Conflict || res.Order.Status != domain.StatusMatched ||
		res.Message != "a driver is already assigned: cancel the trip instead (POST /trips/{id}/cancel)" {
		t.Errorf("CancelOrder = %+v, %v; want a conflict pointing at the trip", res, err)
	}
	if e.db.order(id).Status != domain.StatusMatched || len(e.db.queued()) != 0 || len(e.enc.cancelled) != 0 {
		t.Errorf("order %s, queued %v: want it left alone", e.db.order(id).Status, e.db.queued())
	}
}

func TestAnOrderWithNoDriverCannotBeCancelled(t *testing.T) {
	e := newTestEnv(t)
	id := e.seedOrder(domain.StatusNoDriver)

	res, err := e.svc.CancelOrder(context.Background(), rider1, id, "")
	if err != nil || !res.Conflict || res.Order.Status != domain.StatusNoDriver || res.Message != "this order can no longer be cancelled" {
		t.Errorf("CancelOrder = %+v, %v; want a conflict", res, err)
	}
}

func TestOnlyTheOrdersRiderMayCancelIt(t *testing.T) {
	e := newTestEnv(t)
	id := e.seedOrder(domain.StatusPending)
	ctx := context.Background()

	for name, try := range map[string]func() error{
		"a stranger": func() error {
			_, err := e.svc.CancelOrder(ctx, domain.Actor{Subject: "rider-2", Role: domain.RoleRider}, id, "")
			return err
		},
		"a malformed id": func() error { _, err := e.svc.CancelOrder(ctx, rider1, "not-a-uuid", ""); return err },
		"an unknown order": func() error {
			_, err := e.svc.CancelOrder(ctx, rider1, uuid.NewString(), "")
			return err
		},
	} {
		if err := try(); !errors.Is(err, domain.ErrOrderNotFound) {
			t.Errorf("%s: err = %v, want ErrOrderNotFound (it doesn't exist for them)", name, err)
		}
	}
	if e.db.order(id).Status != domain.StatusPending || len(e.db.queued()) != 0 || len(e.enc.cancelled) != 0 {
		t.Errorf("a refused request moved the order to %s or queued %v", e.db.order(id).Status, e.db.queued())
	}

	e.db.failRead = errors.New("db down")
	if _, err := e.svc.CancelOrder(ctx, rider1, id, ""); !errors.Is(err, ErrInternal) {
		t.Errorf("a failed read: err = %v, want ErrInternal", err)
	}
}

// Matching reports back between the read and the conditional UPDATE: the
// cancel loses, queues nothing and tells the rider to cancel the trip.
func TestACancelThatLosesTheRaceQueuesNoEvent(t *testing.T) {
	e := newTestEnv(t)
	id := e.seedOrder(domain.StatusPending)
	e.db.beforeCancel = func() { e.db.setStatus(id, domain.StatusMatched) }

	res, err := e.svc.CancelOrder(context.Background(), rider1, id, "")
	if err != nil || !res.Conflict || res.Order.Status != domain.StatusMatched || !strings.Contains(res.Message, "cancel the trip") {
		t.Fatalf("CancelOrder = %+v, %v; want the MATCHED conflict", res, err)
	}
	if len(e.db.queued()) != 0 || e.relay.count() != 0 || e.metrics.cancelled != 0 {
		t.Errorf("queued=%v wakes=%d cancelled=%d; the loser announces nothing", e.db.queued(), e.relay.count(), e.metrics.cancelled)
	}
	for _, c := range e.rec.list() {
		if c == "QueueOrderCancelled" {
			t.Error("order.cancelled was queued for a cancel that did not happen")
		}
	}
}
