package app

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"grab-mini/order-service/internal/domain"
)

func TestOrderAccessIsLimitedToTheOwnerAndDrivers(t *testing.T) {
	e := newTestEnv(t)
	ctx := context.Background()
	id := e.db.seed(domain.Order{ID: uuid.NewString(), RiderID: "rider-1", Status: domain.StatusMatched, DriverID: "driver-1"})

	rider := func(s string) domain.Actor { return domain.Actor{Subject: s, Role: domain.RoleRider} }
	driver := func(s string) domain.Actor { return domain.Actor{Subject: s, Role: domain.RoleDriver} }
	for _, c := range []struct {
		name    string
		who     domain.Actor
		id      string
		wantErr error
	}{
		{"the owner reads their order", rider("rider-1"), id, nil},
		{"the matched driver reads it", driver("driver-1"), id, nil},
		{"another rider can't see it (it doesn't exist for them)", rider("rider-2"), id, domain.ErrOrderNotFound},
		{"another driver can't see it", driver("driver-2"), id, domain.ErrOrderNotFound},
		{"a malformed id is not found", rider("rider-1"), "not-a-uuid", domain.ErrOrderNotFound},
		{"an unknown order is not found", rider("rider-1"), uuid.NewString(), domain.ErrOrderNotFound},
	} {
		o, err := e.svc.GetOrder(ctx, c.who, c.id)
		if !errors.Is(err, c.wantErr) || (err == nil && o.ID != id) {
			t.Errorf("%s: GetOrder = %+v, %v; want %v", c.name, o, err, c.wantErr)
		}
	}

	if o, err := e.svc.LatestOrder(ctx, rider("rider-1"), "rider-1"); err != nil || o.ID != id {
		t.Errorf("the owner's latest order = %+v, %v", o, err)
	}
	if _, err := e.svc.LatestOrder(ctx, rider("rider-2"), "rider-1"); !errors.Is(err, ErrForbidden) {
		t.Errorf("someone else's latest order: err = %v, want ErrForbidden", err)
	}
	if _, err := e.svc.LatestOrder(ctx, rider("rider-3"), "rider-3"); !errors.Is(err, domain.ErrOrderNotFound) {
		t.Errorf("a rider with no orders: err = %v, want ErrOrderNotFound", err)
	}

	e.db.failRead = errors.New("db down")
	if _, err := e.svc.GetOrder(ctx, rider("rider-1"), id); !errors.Is(err, ErrInternal) {
		t.Errorf("a failed read: err = %v, want ErrInternal", err)
	}
}
