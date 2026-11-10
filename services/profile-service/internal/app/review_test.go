package app

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"grab-mini/profile-service/internal/domain"
)

func driverWithStatus(id, status string) domain.Driver {
	d := approvedDriver(id)
	d.Status = status
	return d
}

func TestReviewReasonIsRefusedBeforeTheIdAndTheIdBeforeTheTransaction(t *testing.T) {
	longID := strings.Repeat("d", domain.MaxIDLen+1)
	for _, c := range []struct {
		name  string
		cmd   ReviewCommand
		check func(error) bool
	}{
		{"missing reason on a bad id", ReviewCommand{DriverID: longID, Action: "reject", AdminID: "ops-1"}, isField("reason")},
		{"reason with a newline on a bad id", ReviewCommand{DriverID: longID, Action: "approve", AdminID: "ops-1", Reason: "a\nb"}, isField("reason")},
		{"reason too long", ReviewCommand{DriverID: "d1", Action: "suspend", AdminID: "ops-1", Reason: strings.Repeat("r", 501)}, isField("reason")},
		{"id over 128 bytes", ReviewCommand{DriverID: longID, Action: "approve", AdminID: "ops-1"}, isNoDriver},
		{"id not UTF-8", ReviewCommand{DriverID: "d\xff", Action: "suspend", AdminID: "ops-1", Reason: "late"}, isNoDriver},
		{"unknown action", ReviewCommand{DriverID: "d1", Action: "promote", AdminID: "ops-1"}, func(err error) bool {
			var fe *domain.FieldError
			return err != nil && !errors.As(err, &fe) && !errors.Is(err, domain.ErrNoDriver)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t)
			buf := captureLog(t)
			r.uow.drivers["d1"] = driverWithStatus("d1", domain.StatusApproved)
			_, err := r.svc.Review(context.Background(), c.cmd)
			if !c.check(err) {
				t.Fatalf("got %v", err)
			}
			if r.uow.dos != 0 || r.clk.reads != 0 || r.m.counted() || r.wake.n != 0 || buf.Len() != 0 {
				t.Fatalf("no transaction: dos %d reads %d log %q", r.uow.dos, r.clk.reads, buf.String())
			}
		})
	}
}

func isField(field string) func(error) bool {
	return func(err error) bool {
		var fe *domain.FieldError
		return errors.As(err, &fe) && fe.Field == field
	}
}

func isNoDriver(err error) bool { return errors.Is(err, domain.ErrNoDriver) }

func TestReviewOrderIsLockTransitionClockRecordEncodeQueue(t *testing.T) {
	r := newRig(t)
	captureLogInto(t, r.rec)
	r.uow.drivers["d1"] = driverWithStatus("d1", domain.StatusPending)
	got, err := r.svc.Review(context.Background(), ReviewCommand{DriverID: "d1", Action: "approve", AdminID: "ops-1", Reason: "  ok  "})
	if err != nil {
		t.Fatal(err)
	}
	wantEvents(t, r, "Begin", "LockDriver", "now", "RecordReview", "Encode", "QueueReview", "Commit",
		"Review(approved)", "Wake", "log: driver d1: approve by ops-1 -> APPROVED")
	if got.Status != domain.StatusApproved || got.Reason != "ok" || got.ReviewedBy != "ops-1" ||
		got.StatusAt != rigNow || got.UpdatedAt != rigNow || got.ReviewedAt == nil || !got.ReviewedAt.Equal(rigNow) {
		t.Fatalf("got %+v", got)
	}
	if !reflect.DeepEqual(r.uow.drivers["d1"], got) {
		t.Fatalf("stored %+v, returned %+v", r.uow.drivers["d1"], got)
	}

	// A refused transition reads no clock and writes nothing.
	r.rec.reset()
	before := r.uow.drivers["d1"]
	_, err = r.svc.Review(context.Background(), ReviewCommand{DriverID: "d1", Action: "reject", AdminID: "ops-1", Reason: "no"})
	var te *domain.TransitionError
	if !errors.As(err, &te) || te.From != domain.StatusApproved || te.Action != domain.StatusRejected ||
		err.Error() != "a driver who is APPROVED cannot be REJECTED" {
		t.Fatalf("got %v", err)
	}
	wantEvents(t, r, "Begin", "LockDriver", "Rollback")
	if !reflect.DeepEqual(r.uow.drivers["d1"], before) {
		t.Fatal("a refused review changes nothing")
	}
}

func TestReviewAlwaysWakesAfterTheCommit(t *testing.T) {
	for _, c := range []struct{ action, from, to, decision, reason string }{
		{"approve", domain.StatusPending, domain.StatusApproved, "approved", ""},
		{"approve", domain.StatusSuspended, domain.StatusApproved, "approved", ""},
		{"reject", domain.StatusPending, domain.StatusRejected, "rejected", "blurry"},
		{"suspend", domain.StatusApproved, domain.StatusSuspended, "suspended", "complaints"},
	} {
		t.Run(c.action+"/"+c.from, func(t *testing.T) {
			r := newRig(t)
			captureLogInto(t, r.rec)
			r.uow.drivers["d1"] = driverWithStatus("d1", c.from)
			got, err := r.svc.Review(context.Background(), ReviewCommand{DriverID: "d1", Action: c.action, AdminID: "ops-2", Reason: c.reason})
			if err != nil {
				t.Fatal(err)
			}
			calls := r.rec.list()
			commit, wake := slices.Index(calls, "Commit"), slices.Index(calls, "Wake")
			if r.wake.n != 1 || commit < 0 || wake < commit {
				t.Fatalf("one wake after the commit: %q", calls)
			}
			if got.Status != c.to || !reflect.DeepEqual(r.m.reviews, []string{c.decision}) {
				t.Fatalf("status %s reviews %v", got.Status, r.m.reviews)
			}
			ev := r.enc.events[0]
			if ev.Status != c.to || ev.DriverID != "d1" || ev.ReviewedBy != "ops-2" || ev.Reason != c.reason || ev.Vehicle != domain.VehicleCar {
				t.Fatalf("event %+v", ev)
			}
			if calls[len(calls)-1] != "log: driver d1: "+c.action+" by ops-2 -> "+c.to {
				t.Fatalf("calls %q", calls)
			}
		})
	}
}

func TestReviewCountsItsDecisionAndLogsAfterTheCommitOnly(t *testing.T) {
	for _, step := range []string{"LockDriver", "RecordReview", "QueueReview", "commit"} {
		t.Run(step, func(t *testing.T) {
			r := newRig(t)
			buf := captureLog(t)
			before := driverWithStatus("d1", domain.StatusPending)
			r.uow.drivers["d1"] = before
			if step == "commit" {
				r.uow.commitErr = errBoom
			} else {
				r.uow.failOn, r.uow.failErr = step, errBoom
			}
			_, err := r.svc.Review(context.Background(), ReviewCommand{DriverID: "d1", Action: "approve", AdminID: "ops-1"})
			if err != errBoom {
				t.Fatalf("a port error is returned unwrapped, got %v", err)
			}
			if !reflect.DeepEqual(r.uow.drivers["d1"], before) || len(r.uow.outbox) != 0 {
				t.Fatalf("rolled back: %+v %v", r.uow.drivers["d1"], r.uow.outbox)
			}
			if r.m.counted() || r.wake.n != 0 || buf.Len() != 0 {
				t.Fatalf("no counter, wake or log: %+v %d %q", r.m, r.wake.n, buf.String())
			}
			if calls := r.rec.list(); calls[len(calls)-1] != "Rollback" {
				t.Fatalf("calls %q", calls)
			}
		})
	}
}

func TestAnEncodeErrorRollsBackTheDecision(t *testing.T) {
	r := newRig(t)
	buf := captureLog(t)
	before := driverWithStatus("d1", domain.StatusPending)
	r.uow.drivers["d1"] = before
	r.enc.err = errBoom
	_, err := r.svc.Review(context.Background(), ReviewCommand{DriverID: "d1", Action: "approve", AdminID: "ops-1"})
	if !errors.Is(err, errBoom) || err.Error() != "encoding the review result: boom" {
		t.Fatalf("got %v", err)
	}
	wantEvents(t, r, "Begin", "LockDriver", "now", "RecordReview", "Encode", "Rollback")
	if !reflect.DeepEqual(r.uow.drivers["d1"], before) || len(r.uow.outbox) != 0 || r.m.counted() || r.wake.n != 0 || buf.Len() != 0 {
		t.Fatalf("the decision is rolled back: %+v %v %q", r.uow.drivers["d1"], r.uow.outbox, buf.String())
	}

	// The same for a vehicle change: no UpdateProfile after the failed encode.
	r = newRig(t)
	buf.Reset()
	before = approvedDriver("d1")
	r.uow.drivers["d1"] = before
	r.enc.err = errBoom
	cmd := saveCmd(before)
	cmd.Plate = "51F-12345"
	_, err = r.svc.SaveDriver(context.Background(), cmd)
	if !errors.Is(err, errBoom) || err.Error() != "encoding the review result: boom" {
		t.Fatalf("got %v", err)
	}
	wantEvents(t, r, "Begin", "now", "LockOrCreateDriver", "Encode", "Rollback")
	if r.uow.drivers["d1"] != before || len(r.uow.outbox) != 0 || r.m.counted() || r.wake.n != 0 || buf.Len() != 0 {
		t.Fatalf("nothing kept: %+v %v %q", r.uow.drivers["d1"], r.uow.outbox, buf.String())
	}
}

func TestReviewOnAnUnknownDriverIsErrNoDriver(t *testing.T) {
	r := newRig(t)
	buf := captureLog(t)
	_, err := r.svc.Review(context.Background(), ReviewCommand{DriverID: "ghost", Action: "approve", AdminID: "ops-1"})
	if !errors.Is(err, domain.ErrNoDriver) {
		t.Fatalf("got %v", err)
	}
	wantEvents(t, r, "Begin", "LockDriver", "Rollback")
	if len(r.uow.drivers) != 0 || r.m.counted() || r.wake.n != 0 || buf.Len() != 0 {
		t.Fatal("nothing is created, counted or logged")
	}
}

func TestEveryEventGetsItsOwnEventIDAndTheOccurredAtIsTheReviewTime(t *testing.T) {
	r := newRig(t)
	captureLog(t)
	ctx := context.Background()
	r.uow.drivers["d1"] = driverWithStatus("d1", domain.StatusPending)

	if _, err := r.svc.Review(ctx, ReviewCommand{DriverID: "d1", Action: "approve", AdminID: "ops-1"}); err != nil {
		t.Fatal(err)
	}
	first := *r.uow.drivers["d1"].ReviewedAt
	// The clock steps back: the next result is still later than the previous one.
	r.clk.t = r.clk.t.Add(-time.Hour)
	if _, err := r.svc.Review(ctx, ReviewCommand{DriverID: "d1", Action: "suspend", AdminID: "ops-1", Reason: "late"}); err != nil {
		t.Fatal(err)
	}
	second := *r.uow.drivers["d1"].ReviewedAt
	if _, err := r.svc.Review(ctx, ReviewCommand{DriverID: "d1", Action: "approve", AdminID: "ops-1"}); err != nil {
		t.Fatal(err)
	}
	cmd := saveCmd(r.uow.drivers["d1"])
	cmd.Plate = "51F-12345"
	if _, err := r.svc.SaveDriver(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	last := *r.uow.drivers["d1"].ReviewedAt

	if len(r.enc.events) != 4 || len(r.uow.outbox) != 4 {
		t.Fatalf("events %d outbox %d", len(r.enc.events), len(r.uow.outbox))
	}
	seen := map[string]bool{}
	for i, ev := range r.enc.events {
		if _, err := uuid.Parse(ev.EventID); err != nil || seen[ev.EventID] {
			t.Fatalf("event %d: id %q is not a fresh UUID", i, ev.EventID)
		}
		seen[ev.EventID] = true
		if string(r.uow.outbox[i].Payload) != "event:"+ev.EventID || r.uow.outbox[i].DriverID != "d1" {
			t.Fatalf("outbox %d: %+v", i, r.uow.outbox[i])
		}
		if i > 0 && !ev.OccurredAt.After(r.enc.events[i-1].OccurredAt) {
			t.Fatalf("event %d is not later than the previous one: %v", i, r.enc.events)
		}
	}
	if !r.enc.events[0].OccurredAt.Equal(first) || !r.enc.events[1].OccurredAt.Equal(second) || !r.enc.events[3].OccurredAt.Equal(last) {
		t.Fatalf("occurredAt is the stored review time: %v %v %v", first, second, last)
	}
	if !second.Equal(first.Add(time.Microsecond)) {
		t.Fatalf("a clock step back dates one microsecond later: %v then %v", first, second)
	}
	if r.enc.events[0].OccurredAt.Location() != time.UTC {
		t.Fatal("occurredAt is UTC (the use case's now)")
	}
}
