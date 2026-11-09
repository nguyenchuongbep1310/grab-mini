package app

import (
	"context"
	"errors"
	"testing"

	"grab-mini/order-service/internal/domain"
)

func TestAnOutcomeForANonUUIDOrderIsPermanent(t *testing.T) {
	e := newTestEnv(t)
	ctx := context.Background()
	for name, err := range map[string]error{
		"matched":   e.svc.SetMatched(ctx, "not-a-uuid", "driver-1"),
		"no driver": e.svc.SetNoDriver(ctx, "not-a-uuid"),
	} {
		var bad *BadOutcomeError
		if !errors.As(err, &bad) || !bad.Permanent() {
			t.Errorf("%s: err = %v, want a permanent *BadOutcomeError (retrying can't fix it)", name, err)
			continue
		}
		if got, want := err.Error(), "unusable matching outcome: order id not-a-uuid is not a UUID"; got != want {
			t.Errorf("%s: text %q, want %q", name, got, want)
		}
	}
	if calls := e.rec.list(); len(calls) != 0 {
		t.Errorf("calls %v: the guard runs before the database", calls)
	}
}

func TestACancelledTripWithABadOrderIdIsPermanentlyUnusable(t *testing.T) {
	e := newTestEnv(t)
	err := e.svc.CancelMatchedOrder(context.Background(), "not-a-uuid", "RIDER", "")
	var bad *BadOutcomeError
	if !errors.As(err, &bad) || !bad.Permanent() {
		t.Errorf("err = %v, want a permanent *BadOutcomeError", err)
	}
	if calls := e.rec.list(); len(calls) != 0 {
		t.Errorf("calls %v: the guard runs before the database", calls)
	}
}

func TestACancelledTripCancelsItsMatchedOrder(t *testing.T) {
	e := newTestEnv(t)
	ctx := context.Background()
	matched := e.seedOrder(domain.StatusMatched)
	pending := e.seedOrder(domain.StatusPending)

	for i := 0; i < 2; i++ { // the second is a redelivery
		if err := e.svc.CancelMatchedOrder(ctx, matched, "DRIVER", "flat tyre"); err != nil {
			t.Fatal(err)
		}
	}
	if o := e.db.order(matched); o.Status != domain.StatusCancelled || o.CancelledBy != "DRIVER" || o.CancelReason != "flat tyre" || o.CancelledAt == nil {
		t.Errorf("order = %+v, want CANCELLED by the DRIVER", o)
	}

	// Only MATCHED orders move this way; a PENDING one keeps searching.
	if err := e.svc.CancelMatchedOrder(ctx, pending, "DRIVER", ""); err != nil {
		t.Fatal(err)
	}
	if o := e.db.order(pending); o.Status != domain.StatusPending {
		t.Errorf("a PENDING order became %s", o.Status)
	}

	// A later event never rewrites who cancelled.
	e.svc.CancelMatchedOrder(ctx, matched, "RIDER", "again")
	if o := e.db.order(matched); o.CancelledBy != "DRIVER" {
		t.Errorf("cancelledBy rewritten to %s", o.CancelledBy)
	}

	// An outcome only moves a PENDING order, and records the driver.
	if err := e.svc.SetMatched(ctx, pending, "driver-9"); err != nil {
		t.Fatal(err)
	}
	if o := e.db.order(pending); o.Status != domain.StatusMatched || o.DriverID != "driver-9" {
		t.Errorf("order = %+v, want MATCHED to driver-9", o)
	}
	if err := e.svc.SetNoDriver(ctx, matched); err != nil || e.db.order(matched).Status != domain.StatusCancelled {
		t.Errorf("NO_DRIVER overwrote a CANCELLED order (%v)", err)
	}
}
