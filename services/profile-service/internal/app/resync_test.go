package app

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"grab-mini/profile-service/internal/domain"
)

// resyncRig is a rig whose Driver reads also go to the shared log, with the given drivers
// both listed by the source and stored in the unit of work.
func resyncRig(t *testing.T, drivers ...domain.Driver) *rig {
	t.Helper()
	r := newRig(t)
	r.store.rec = r.rec
	for _, d := range drivers {
		r.src.ids = append(r.src.ids, d.ID)
		r.uow.drivers[d.ID] = d
		r.store.drivers[d.ID] = d
	}
	slices.Sort(r.src.ids)
	return r
}

func wantCounts(t *testing.T, res ResyncResult, approved, rejected, suspended, total, skipped int) {
	t.Helper()
	want := ResyncResult{
		Counts: map[string]int{domain.StatusApproved: approved, domain.StatusRejected: rejected, domain.StatusSuspended: suspended},
		Total:  total, Skipped: skipped,
	}
	if !reflect.DeepEqual(res, want) {
		t.Fatalf("result %+v, want %+v", res, want)
	}
}

// sortedResyncEvents is the ResyncEvents calls in status order: the flat code ranges over a
// map, so their order within one batch is not defined.
func sortedResyncEvents(m *fakeMetrics) []resyncCount {
	out := slices.Clone(m.resync)
	slices.SortFunc(out, func(a, b resyncCount) int { return strings.Compare(a.Status, b.Status) })
	return out
}

func TestOnlyOneResyncRunsAtATime(t *testing.T) {
	r := resyncRig(t, approvedDriver("d1"))
	var inner error
	var innerRes ResyncResult
	var dosBefore, dosAfter, queuedBefore, queuedAfter int
	var runsInside []string
	r.src.hook = func(call int) {
		if call != 1 {
			return
		}
		dosBefore, queuedBefore = r.uow.dos, len(r.uow.outbox)
		innerRes, inner = r.svc.Resync(context.Background(), "")
		dosAfter, queuedAfter = r.uow.dos, len(r.uow.outbox)
		runsInside = slices.Clone(r.m.runs)
	}
	res, err := r.svc.Resync(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(inner, ErrResyncRunning) || inner.Error() != "a resync is already running" {
		t.Fatalf("second run: %v", inner)
	}
	wantCounts(t, innerRes, 0, 0, 0, 0, 0)
	if dosAfter != dosBefore || queuedAfter != queuedBefore {
		t.Fatalf("the refused run touched the database: dos %d->%d queued %d->%d", dosBefore, dosAfter, queuedBefore, queuedAfter)
	}
	if !reflect.DeepEqual(runsInside, []string{"refused_running"}) {
		t.Fatalf("runs counted by the refused run: %q", runsInside)
	}
	wantCounts(t, res, 1, 0, 0, 1, 0)
	if !reflect.DeepEqual(r.m.runs, []string{"refused_running", "completed"}) {
		t.Fatalf("runs %q", r.m.runs)
	}
	// The refused run never held the lock, so it does not release it.
	if !reflect.DeepEqual(r.lock.calls, []string{"TryLock", "TryLock", "unlock"}) || r.lock.held {
		t.Fatalf("lock calls %q held %v", r.lock.calls, r.lock.held)
	}
}

func TestALockErrorCountsFailed(t *testing.T) {
	r := resyncRig(t, approvedDriver("d1"))
	r.lock.err = errBoom
	res, err := r.svc.Resync(context.Background(), "")
	if err != errBoom {
		t.Fatalf("got %v, want the lock's error unwrapped", err)
	}
	wantCounts(t, res, 0, 0, 0, 0, 0)
	wantEvents(t, r, "TryLock", "ResyncRun(failed)")
}

func TestTheLockIsReleasedAfterEveryRunIncludingFailures(t *testing.T) {
	for _, c := range []struct {
		name     string
		driverID string
		setup    func(r *rig)
		run      string // the run counted just before the unlock; "" for none
	}{
		{"completed run", "", func(*rig) {}, "completed"},
		{"completed single driver", "d1", func(*rig) {}, "completed"},
		{"id list fails", "", func(r *rig) { r.src.err = errBoom }, "failed"},
		{"lock of a row fails", "", func(r *rig) { r.uow.failOn, r.uow.failErr = "LockDriver", errBoom }, "failed"},
		{"encode fails", "", func(r *rig) { r.enc.err = errBoom }, "failed"},
		{"queue fails", "", func(r *rig) { r.uow.failOn, r.uow.failErr = "QueueReview", errBoom }, "failed"},
		{"commit fails", "", func(r *rig) { r.uow.commitErr = errBoom }, "failed"},
		{"single driver read fails", "d1", func(r *rig) { r.store.loadErr = errBoom }, "failed"},
		{"single driver commit fails", "d1", func(r *rig) { r.uow.commitErr = errBoom }, "failed"},
		{"unknown single driver", "nobody", func(*rig) {}, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := resyncRig(t, approvedDriver("d1"), approvedDriver("d2"))
			c.setup(r)
			r.svc.Resync(context.Background(), c.driverID)
			if !reflect.DeepEqual(r.lock.calls, []string{"TryLock", "unlock"}) || r.lock.held {
				t.Fatalf("lock calls %q held %v", r.lock.calls, r.lock.held)
			}
			ev := r.rec.list()
			if ev[len(ev)-1] != "unlock" {
				t.Fatalf("the unlock is not the last step: %q", ev)
			}
			if c.run != "" && ev[len(ev)-2] != "ResyncRun("+c.run+")" {
				t.Fatalf("the run is not counted just before the unlock: %q", ev)
			}
		})
	}
}

func TestAnUnknownSingleDriverIsErrNoDriverAndCountsNoRun(t *testing.T) {
	r := resyncRig(t, approvedDriver("d1"))
	res, err := r.svc.Resync(context.Background(), "nobody")
	if !errors.Is(err, domain.ErrNoDriver) {
		t.Fatalf("got %v", err)
	}
	wantCounts(t, res, 0, 0, 0, 0, 0)
	wantEvents(t, r, "TryLock", "Driver", "unlock")
	if r.m.counted() || r.uow.dos != 0 || r.clk.reads != 0 || r.wake.n != 0 {
		t.Fatalf("not a run: counted %v dos %d reads %d wakes %d", r.m.counted(), r.uow.dos, r.clk.reads, r.wake.n)
	}
}

func TestAReadErrorOnASingleDriverCountsFailed(t *testing.T) {
	r := resyncRig(t, approvedDriver("d1"))
	r.store.loadErr = errBoom
	res, err := r.svc.Resync(context.Background(), "d1")
	if err != errBoom {
		t.Fatalf("got %v, want the read's error unwrapped", err)
	}
	wantCounts(t, res, 0, 0, 0, 0, 0)
	wantEvents(t, r, "TryLock", "Driver", "ResyncRun(failed)", "unlock")
}

func TestResyncOfOneDriverReadsThenBatchesThatDriver(t *testing.T) {
	d := approvedDriver("d1")
	d.Status = domain.StatusSuspended
	r := resyncRig(t, d, approvedDriver("d2"))
	res, err := r.svc.Resync(context.Background(), "d1")
	if err != nil {
		t.Fatal(err)
	}
	wantCounts(t, res, 0, 0, 1, 1, 0)
	wantEvents(t, r, "TryLock", "Driver", "Begin", "now", "LockDriver", "Encode", "QueueReview", "Commit",
		"ResyncEvents(SUSPENDED,1)", "Wake", "ResyncRun(completed)", "unlock")
	if len(r.src.calls) != 0 {
		t.Fatalf("the id list was read: %+v", r.src.calls)
	}
	if len(r.uow.outbox) != 1 || r.uow.outbox[0].DriverID != "d1" {
		t.Fatalf("outbox %+v", r.uow.outbox)
	}

	// A single driver who is no longer resyncable is looked at and skipped, still a run.
	r = resyncRig(t, driverWithStatus("d1", domain.StatusPending))
	res, err = r.svc.Resync(context.Background(), "d1")
	if err != nil {
		t.Fatal(err)
	}
	wantCounts(t, res, 0, 0, 0, 0, 1)
	wantEvents(t, r, "TryLock", "Driver", "Begin", "now", "LockDriver", "Commit", "Wake", "ResyncRun(completed)", "unlock")
}

func TestResyncGoesThroughTheIdsInBatchesOfTwoHundredAfterTheLastId(t *testing.T) {
	if ResyncBatch != 200 || ResyncTimeout != 15*time.Minute {
		t.Fatalf("ResyncBatch %d ResyncTimeout %v", ResyncBatch, ResyncTimeout)
	}
	var ds []domain.Driver
	for i := range 450 {
		ds = append(ds, approvedDriver(fmt.Sprintf("d%03d", i)))
	}
	r := resyncRig(t, ds...)
	res, err := r.svc.Resync(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	wantCounts(t, res, 450, 0, 0, 450, 0)
	want := []sourceCall{{"", 200}, {"d199", 200}, {"d399", 200}, {"d449", 200}}
	if !reflect.DeepEqual(r.src.calls, want) {
		t.Fatalf("id list calls %+v, want %+v", r.src.calls, want)
	}
	if r.uow.dos != 3 || r.clk.reads != 3 || r.wake.n != 3 {
		t.Fatalf("one transaction, one clock read and one wake per batch: dos %d reads %d wakes %d", r.uow.dos, r.clk.reads, r.wake.n)
	}
	if !reflect.DeepEqual(r.m.resync, []resyncCount{{"APPROVED", 200}, {"APPROVED", 200}, {"APPROVED", 50}}) {
		t.Fatalf("resync events %+v", r.m.resync)
	}
	if !reflect.DeepEqual(r.m.runs, []string{"completed"}) {
		t.Fatalf("runs %q", r.m.runs)
	}
	for i, q := range r.uow.outbox {
		if q.DriverID != ds[i].ID {
			t.Fatalf("outbox row %d is %s, want %s", i, q.DriverID, ds[i].ID)
		}
	}

	// The order of one run: the clock once per batch before the loop, the counts and the
	// wake after the commit, the next id list after that.
	r = resyncRig(t, approvedDriver("d1"), approvedDriver("d2"))
	if _, err := r.svc.Resync(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	wantEvents(t, r, "TryLock", "ResyncableIDs()", "Begin", "now",
		"LockDriver", "Encode", "QueueReview", "LockDriver", "Encode", "QueueReview", "Commit",
		"ResyncEvents(APPROVED,2)", "Wake", "ResyncableIDs(d2)", "ResyncRun(completed)", "unlock")

	// Nobody to resync: a completed run with no transaction and no wake.
	r = resyncRig(t)
	res, err = r.svc.Resync(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	wantCounts(t, res, 0, 0, 0, 0, 0)
	wantEvents(t, r, "TryLock", "ResyncableIDs()", "ResyncRun(completed)", "unlock")
}

func TestAStatusThatChangedUnderTheLockIsSkippedAndCounted(t *testing.T) {
	r := resyncRig(t, approvedDriver("d1"), approvedDriver("d2"), approvedDriver("d3"),
		driverWithStatus("d4", domain.StatusRejected), driverWithStatus("d5", domain.StatusSuspended))
	// Listed while resyncable, changed before their row was locked.
	r.uow.drivers["d2"] = driverWithStatus("d2", domain.StatusPending)
	r.uow.drivers["d3"] = driverWithStatus("d3", domain.StatusNone)
	res, err := r.svc.Resync(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	wantCounts(t, res, 1, 1, 1, 3, 2)
	if got := sortedResyncEvents(r.m); !reflect.DeepEqual(got, []resyncCount{{"APPROVED", 1}, {"REJECTED", 1}, {"SUSPENDED", 1}}) {
		t.Fatalf("resync events %+v", got)
	}
	var ids []string
	for _, q := range r.uow.outbox {
		ids = append(ids, q.DriverID)
	}
	if !reflect.DeepEqual(ids, []string{"d1", "d4", "d5"}) {
		t.Fatalf("queued %q", ids)
	}
}

func TestADriverDeletedUnderTheLockIsSkipped(t *testing.T) {
	r := resyncRig(t, approvedDriver("d1"), approvedDriver("d2"))
	delete(r.uow.drivers, "d1")
	res, err := r.svc.Resync(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	wantCounts(t, res, 1, 0, 0, 1, 1)
	if len(r.uow.outbox) != 1 || r.uow.outbox[0].DriverID != "d2" {
		t.Fatalf("outbox %+v", r.uow.outbox)
	}
	if !reflect.DeepEqual(r.m.runs, []string{"completed"}) {
		t.Fatalf("runs %q", r.m.runs)
	}
}

func TestCountsAndMetricsAreAddedOnlyAfterTheBatchCommits(t *testing.T) {
	var ds []domain.Driver
	for i := range 201 {
		ds = append(ds, approvedDriver(fmt.Sprintf("d%03d", i)))
	}
	ds[0].Status = domain.StatusRejected
	ds[200].Status = domain.StatusSuspended
	r := resyncRig(t, ds...)
	r.uow.drivers["d001"] = driverWithStatus("d001", domain.StatusPending)
	r.src.hook = func(call int) {
		if call == 2 { // the second batch's commit fails
			r.uow.commitErr = errBoom
		}
	}
	res, err := r.svc.Resync(context.Background(), "")
	if err != errBoom {
		t.Fatalf("got %v, want the commit's error unwrapped", err)
	}
	// The partial result is the first batch only, returned with the error.
	wantCounts(t, res, 198, 1, 0, 199, 1)
	if got := sortedResyncEvents(r.m); !reflect.DeepEqual(got, []resyncCount{{"APPROVED", 198}, {"REJECTED", 1}}) {
		t.Fatalf("resync events %+v", got)
	}
	if !reflect.DeepEqual(r.m.runs, []string{"failed"}) || r.wake.n != 1 {
		t.Fatalf("runs %q wakes %d", r.m.runs, r.wake.n)
	}

	// A single batch whose commit fails: nothing added at all.
	r = resyncRig(t, approvedDriver("d1"), driverWithStatus("d2", domain.StatusPending))
	r.uow.commitErr = errBoom
	res, err = r.svc.Resync(context.Background(), "")
	if err != errBoom {
		t.Fatalf("got %v", err)
	}
	wantCounts(t, res, 0, 0, 0, 0, 0)
	wantEvents(t, r, "TryLock", "ResyncableIDs()", "Begin", "now", "LockDriver", "Encode", "QueueReview", "LockDriver",
		"Rollback", "ResyncRun(failed)", "unlock")
}

// nthEncoder fails its n-th call (1-based) and passes the others to the fake encoder.
type nthEncoder struct {
	*fakeEncoder
	n, calls int
}

func (e *nthEncoder) Reviewed(ctx context.Context, ev ReviewedEvent) ([]byte, error) {
	e.calls++
	if e.calls == e.n {
		e.rec.add("Encode")
		return nil, errBoom
	}
	return e.fakeEncoder.Reviewed(ctx, ev)
}

func TestAFailedBatchQueuesNothingFromItsBatch(t *testing.T) {
	r := resyncRig(t, approvedDriver("d1"), approvedDriver("d2"), approvedDriver("d3"))
	r.svc.enc = &nthEncoder{fakeEncoder: r.enc, n: 2}
	res, err := r.svc.Resync(context.Background(), "")
	if err == nil || err.Error() != "encoding the review result: boom" || !errors.Is(err, errBoom) {
		t.Fatalf("got %v", err)
	}
	wantCounts(t, res, 0, 0, 0, 0, 0)
	if len(r.uow.outbox) != 0 {
		t.Fatalf("the first driver's event survived the failed batch: %+v", r.uow.outbox)
	}
	wantEvents(t, r, "TryLock", "ResyncableIDs()", "Begin", "now", "LockDriver", "Encode", "QueueReview",
		"LockDriver", "Encode", "Rollback", "ResyncRun(failed)", "unlock")
	if r.wake.n != 0 || len(r.m.resync) != 0 {
		t.Fatalf("wakes %d resync events %+v", r.wake.n, r.m.resync)
	}
}

func TestAnIdListErrorAfterACommittedBatchReturnsThePartialResultAndCountsFailed(t *testing.T) {
	var ds []domain.Driver
	for i := range 250 {
		ds = append(ds, approvedDriver(fmt.Sprintf("d%03d", i)))
	}
	r := resyncRig(t, ds...)
	r.src.hook = func(call int) {
		if call == 2 {
			r.src.err = errBoom
		}
	}
	res, err := r.svc.Resync(context.Background(), "")
	if !errors.Is(err, errBoom) {
		t.Fatalf("got %v", err)
	}
	wantCounts(t, res, 200, 0, 0, 200, 0)
	if !reflect.DeepEqual(r.m.runs, []string{"failed"}) {
		t.Fatalf("runs %q", r.m.runs)
	}
	if r.uow.dos != 1 || r.wake.n != 1 || len(r.uow.outbox) != 200 {
		t.Fatalf("the first batch stays committed: dos %d wakes %d queued %d", r.uow.dos, r.wake.n, len(r.uow.outbox))
	}
}

func TestEveryCommittedBatchWakesEvenWhenAllWereSkipped(t *testing.T) {
	var ds []domain.Driver
	for i := range 250 {
		ds = append(ds, approvedDriver(fmt.Sprintf("d%03d", i)))
	}
	r := resyncRig(t, ds...)
	for i := range 200 { // the whole first batch became PENDING under the lock
		id := ds[i].ID
		r.uow.drivers[id] = driverWithStatus(id, domain.StatusPending)
	}
	res, err := r.svc.Resync(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	wantCounts(t, res, 50, 0, 0, 50, 200)
	if r.wake.n != 2 {
		t.Fatalf("wakes %d, want one per committed batch", r.wake.n)
	}
	if !reflect.DeepEqual(r.m.resync, []resyncCount{{"APPROVED", 50}}) {
		t.Fatalf("resync events %+v", r.m.resync)
	}
	ev := r.rec.list()
	first := slices.Index(ev, "Commit")
	if first < 0 || ev[first+1] != "Wake" || ev[first+2] != "ResyncableIDs(d199)" {
		t.Fatalf("the skipped batch did not wake right after its commit: %q", ev[first:min(first+3, len(ev))])
	}
}

func TestResyncEventsAreDatedLikeANewDecisionAndNameResyncAsReviewer(t *testing.T) {
	past := approvedDriver("d1") // reviewed an hour ago, no reason
	future := driverWithStatus("d2", domain.StatusRejected)
	ahead := rigNow.Add(time.Second)
	future.ReviewedAt, future.Reason = &ahead, "bad documents"
	never := driverWithStatus("d3", domain.StatusSuspended)
	never.ReviewedAt, never.Reason, never.Vehicle = nil, "late", domain.VehicleBike
	same := approvedDriver("d4")
	at := rigNow
	same.ReviewedAt = &at
	r := resyncRig(t, past, future, never, same)
	before := maps.Clone(r.uow.drivers)

	if _, err := r.svc.Resync(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	want := []ReviewedEvent{
		{OccurredAt: rigNow, DriverID: "d1", Status: "APPROVED", Vehicle: domain.VehicleCar, Reason: "resync", ReviewedBy: "resync"},
		{OccurredAt: ahead.Add(time.Microsecond), DriverID: "d2", Status: "REJECTED", Vehicle: domain.VehicleCar, Reason: "bad documents", ReviewedBy: "resync"},
		{OccurredAt: rigNow, DriverID: "d3", Status: "SUSPENDED", Vehicle: domain.VehicleBike, Reason: "late", ReviewedBy: "resync"},
		{OccurredAt: rigNow.Add(time.Microsecond), DriverID: "d4", Status: "APPROVED", Vehicle: domain.VehicleCar, Reason: "resync", ReviewedBy: "resync"},
	}
	if len(r.enc.events) != len(want) {
		t.Fatalf("events %+v", r.enc.events)
	}
	seen := map[string]bool{}
	for i, ev := range r.enc.events {
		if _, err := uuid.Parse(ev.EventID); err != nil || seen[ev.EventID] {
			t.Fatalf("event %d id %q: %v (seen %v)", i, ev.EventID, err, seen[ev.EventID])
		}
		seen[ev.EventID] = true
		if string(r.uow.outbox[i].Payload) != "event:"+ev.EventID || r.uow.outbox[i].DriverID != ev.DriverID {
			t.Fatalf("outbox row %d %+v for event %+v", i, r.uow.outbox[i], ev)
		}
		ev.EventID = ""
		if !reflect.DeepEqual(ev, want[i]) {
			t.Fatalf("event %d\n got  %+v\n want %+v", i, ev, want[i])
		}
	}
	if !reflect.DeepEqual(r.uow.drivers, before) {
		t.Fatal("a resync changed a profile row")
	}
}

func TestResyncRunCountsCompletedOnlyOnSuccess(t *testing.T) {
	for _, c := range []struct {
		name     string
		driverID string
		setup    func(r *rig)
		want     []string
	}{
		{"every driver", "", func(*rig) {}, []string{"completed"}},
		{"one driver", "d1", func(*rig) {}, []string{"completed"}},
		{"lock error", "", func(r *rig) { r.lock.err = errBoom }, []string{"failed"}},
		{"another run holds the lock", "", func(r *rig) { r.lock.held = true }, []string{"refused_running"}},
		{"id list fails", "", func(r *rig) { r.src.err = errBoom }, []string{"failed"}},
		{"commit fails", "", func(r *rig) { r.uow.commitErr = errBoom }, []string{"failed"}},
		{"single read fails", "d1", func(r *rig) { r.store.loadErr = errBoom }, []string{"failed"}},
		{"unknown single driver", "nobody", func(*rig) {}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := resyncRig(t, approvedDriver("d1"))
			c.setup(r)
			r.svc.Resync(context.Background(), c.driverID)
			if !reflect.DeepEqual(r.m.runs, c.want) {
				t.Fatalf("runs %q, want %q", r.m.runs, c.want)
			}
		})
	}
}

func TestResyncAcceptsANilWaker(t *testing.T) {
	r := resyncRig(t, approvedDriver("d1"))
	svc := New(Deps{
		Drivers: r.store, Resync: r.src, Locker: r.lock, UoW: r.uow, Encoder: r.enc, Metrics: r.m, Now: r.clk.Now,
	})
	res, err := svc.Resync(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	wantCounts(t, res, 1, 0, 0, 1, 0)
	if res, err = svc.Resync(context.Background(), "d1"); err != nil {
		t.Fatal(err)
	}
	wantCounts(t, res, 1, 0, 0, 1, 0)
}

func TestResyncLogsNothing(t *testing.T) {
	r := resyncRig(t, approvedDriver("d1"), driverWithStatus("d2", domain.StatusPending))
	buf := captureLog(t)
	r.svc.Resync(context.Background(), "")
	r.svc.Resync(context.Background(), "d1")
	r.svc.Resync(context.Background(), "nobody")
	r.uow.commitErr = errBoom
	r.svc.Resync(context.Background(), "")
	if buf.Len() != 0 {
		t.Fatalf("the use case logged %q (the resync log line belongs to the HTTP handler)", buf.String())
	}
}
