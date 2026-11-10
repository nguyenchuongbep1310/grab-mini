package app

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"grab-mini/profile-service/internal/domain"
)

func TestADriverWithoutAProfileIsNoneWithNoTimes(t *testing.T) {
	r := newRig(t)
	d, err := r.svc.GetDriver(context.Background(), "d1")
	if err != nil {
		t.Fatal(err)
	}
	if d.ID != "d1" || d.Status != domain.StatusNone || !d.UpdatedAt.IsZero() || !d.StatusAt.IsZero() || d.ReviewedAt != nil {
		t.Fatalf("got %+v", d)
	}
	r.store.drivers["d2"] = domain.Driver{ID: "d2", Status: domain.StatusPending, Name: "x"}
	if d, _ := r.svc.GetDriver(context.Background(), "d2"); d.Status != domain.StatusPending || d.Name != "x" {
		t.Fatalf("an existing profile is returned as it is: %+v", d)
	}
	r.store.loadErr = errors.New("boom")
	if _, err := r.svc.GetDriver(context.Background(), "d1"); !errors.Is(err, r.store.loadErr) {
		t.Fatalf("got %v", err)
	}
}

func TestPublicDriverIsFoundOnlyWhenApproved(t *testing.T) {
	r := newRig(t)
	for _, s := range []string{domain.StatusPending, domain.StatusRejected, domain.StatusSuspended, domain.StatusNone, ""} {
		r.store.drivers["d-"+s] = domain.Driver{ID: "d-" + s, Status: s}
	}
	r.store.drivers["ok"] = domain.Driver{ID: "ok", Status: domain.StatusApproved, Name: "Bo"}

	d, err := r.svc.PublicDriver(context.Background(), "ok")
	if err != nil || d.Name != "Bo" {
		t.Fatalf("got %+v, %v", d, err)
	}
	if len(r.m.public) != 1 || r.m.public[0] != "found" {
		t.Fatalf("counters %v", r.m.public)
	}
	for _, id := range []string{"d-PENDING", "d-REJECTED", "d-SUSPENDED", "d-NONE", "d-", "missing"} {
		if _, err := r.svc.PublicDriver(context.Background(), id); !errors.Is(err, domain.ErrNoDriver) {
			t.Fatalf("%q: want ErrNoDriver, got %v", id, err)
		}
	}
	if len(r.m.public) != 7 {
		t.Fatalf("one count per lookup: %v", r.m.public)
	}
	for _, c := range r.m.public[1:] {
		if c != "not_found" {
			t.Fatalf("counters %v", r.m.public)
		}
	}
}

func TestAPublicLoadErrorCountsNothing(t *testing.T) {
	r := newRig(t)
	r.store.loadErr = errors.New("db down")
	_, err := r.svc.PublicDriver(context.Background(), "d1")
	if !errors.Is(err, r.store.loadErr) || errors.Is(err, domain.ErrNoDriver) {
		t.Fatalf("got %v", err)
	}
	if len(r.m.public) != 0 {
		t.Fatalf("counted %v", r.m.public)
	}
}

func TestTheQueueIsParsedStatusThenLimitThenCursor(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	check := func(q ListQuery, want string) {
		t.Helper()
		_, err := r.svc.ListApplications(ctx, q)
		var ir *domain.InvalidRequestError
		if !errors.As(err, &ir) || ir.Message != want {
			t.Fatalf("%+v: got %v, want %q", q, err, want)
		}
		if len(r.store.calls) != 0 {
			t.Fatalf("a refused query must not reach the store: %v", r.store.calls)
		}
	}
	const st = "status must be PENDING, APPROVED, REJECTED or SUSPENDED"
	const lim = "limit must be from 1 to 100"
	const cur = "cursor must come from a previous nextCursor"
	check(ListQuery{Status: "NONE", Limit: "0", Cursor: "bad"}, st)
	check(ListQuery{Status: "APPROVED", Limit: "0", Cursor: "bad"}, lim)
	check(ListQuery{Status: "APPROVED", Limit: "101", Cursor: "bad"}, lim)
	check(ListQuery{Status: "APPROVED", Limit: "x", Cursor: "bad"}, lim)
	check(ListQuery{Status: "APPROVED", Limit: "5", Cursor: "bad"}, cur)

	// Defaults: empty status is PENDING, empty limit is 50 (fetch 51), empty cursor is no cursor.
	if _, err := r.svc.ListApplications(ctx, ListQuery{}); err != nil {
		t.Fatal(err)
	}
	if g := r.store.queueArg; g.Status != "PENDING" || g.Limit != 51 || g.After != nil {
		t.Fatalf("got %+v", g)
	}
	c := fmt.Sprintf("%d.d9", time.Date(2026, 1, 1, 0, 0, 0, 5000, time.UTC).UnixMicro())
	if _, err := r.svc.ListApplications(ctx, ListQuery{Status: "REJECTED", Limit: "7", Cursor: c}); err != nil {
		t.Fatal(err)
	}
	g := r.store.queueArg
	if g.Status != "REJECTED" || g.Limit != 8 || g.After == nil || g.After.ID != "d9" || g.After.At.UnixMicro() != time.Date(2026, 1, 1, 0, 0, 0, 5000, time.UTC).UnixMicro() {
		t.Fatalf("got %+v", g)
	}
}

func TestTheQueueFetchesOneMoreThanTheLimitAndPagesByTheLastKeptRow(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var rows []domain.Driver
	for i := 0; i < 3; i++ {
		rows = append(rows, domain.Driver{ID: fmt.Sprintf("d%d", i), StatusAt: base.Add(time.Duration(i) * time.Second)})
	}
	r.store.queueOut = rows

	p, err := r.svc.ListApplications(ctx, ListQuery{Limit: "2"})
	if err != nil {
		t.Fatal(err)
	}
	if r.store.queueArg.Limit != 3 {
		t.Fatalf("fetched %d", r.store.queueArg.Limit)
	}
	want := fmt.Sprintf("%d.d1", base.Add(time.Second).UnixMicro())
	if len(p.Drivers) != 2 || p.Drivers[1].ID != "d1" || p.NextCursor == nil || *p.NextCursor != want {
		t.Fatalf("got %+v next %v, want next %q", p.Drivers, p.NextCursor, want)
	}

	r.store.queueOut = rows[:2]
	p, _ = r.svc.ListApplications(ctx, ListQuery{Limit: "2"})
	if len(p.Drivers) != 2 || p.NextCursor != nil {
		t.Fatalf("a full last page has no cursor: %+v", p)
	}

	r.store.loadErr = errors.New("db")
	if _, err := r.svc.ListApplications(ctx, ListQuery{}); !errors.Is(err, r.store.loadErr) {
		t.Fatalf("got %v", err)
	}
}

func TestAnEmptyQueueIsAnEmptyNonNilSlice(t *testing.T) {
	r := newRig(t)
	r.store.queueOut = nil
	p, err := r.svc.ListApplications(context.Background(), ListQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if p.Drivers == nil || len(p.Drivers) != 0 || p.NextCursor != nil {
		t.Fatalf("got %#v", p)
	}
}
