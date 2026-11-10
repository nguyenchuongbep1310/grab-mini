package app

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"grab-mini/profile-service/internal/domain"
)

// rigNow is what the rig's clock reads, as the use cases' now() turns it: UTC, microseconds.
var rigNow = time.Date(2026, 3, 1, 11, 0, 0, 123456000, time.UTC)

func approvedDriver(id string) domain.Driver {
	reviewed := rigNow.Add(-time.Hour)
	return domain.Driver{
		ID: id, Name: "Bo", Phone: "0901234567", Vehicle: domain.VehicleCar, Plate: "29A-123.45",
		Model: "Vios", Color: "white", Status: domain.StatusApproved, ReviewedBy: "ops-1",
		ReviewedAt: &reviewed, StatusAt: reviewed, UpdatedAt: reviewed,
	}
}

func saveCmd(d domain.Driver) SaveDriverCommand {
	return SaveDriverCommand{
		ID: d.ID, Name: d.Name, Phone: d.Phone, VehicleType: string(d.Vehicle),
		Plate: d.Plate, Model: d.Model, Color: d.Color,
	}
}

func wantEvents(t *testing.T, r *rig, want ...string) {
	t.Helper()
	if got := r.rec.list(); !reflect.DeepEqual(got, want) {
		t.Fatalf("calls:\n got  %q\n want %q", got, want)
	}
}

func TestSaveDriverCleansInTheOldOrder(t *testing.T) {
	long := func(n int) string { return strings.Repeat("a", n) }
	cases := []struct {
		name  string
		cmd   SaveDriverCommand
		field string
	}{
		{"name first", SaveDriverCommand{ID: "d1", Name: "", Phone: "x", VehicleType: "TRUCK", Plate: "#", Model: long(51), Color: long(31)}, "name"},
		{"then phone", SaveDriverCommand{ID: "d1", Name: "Bo", Phone: "x", VehicleType: "TRUCK", Plate: "#", Model: long(51), Color: long(31)}, "phone"},
		{"then vehicle", SaveDriverCommand{ID: "d1", Name: "Bo", Phone: "", VehicleType: "TRUCK", Plate: "#", Model: long(51), Color: long(31)}, "vehicleType"},
		{"then plate", SaveDriverCommand{ID: "d1", Name: "Bo", VehicleType: "car", Plate: "#", Model: long(51), Color: long(31)}, "plate"},
		{"then model", SaveDriverCommand{ID: "d1", Name: "Bo", VehicleType: "car", Plate: "29A-123", Model: long(51), Color: long(31)}, "model"},
		{"then colour", SaveDriverCommand{ID: "d1", Name: "Bo", VehicleType: "car", Plate: "29A-123", Model: "Vios", Color: long(31)}, "color"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t)
			buf := captureLog(t)
			_, err := r.svc.SaveDriver(context.Background(), c.cmd)
			var fe *domain.FieldError
			if !errors.As(err, &fe) || fe.Field != c.field {
				t.Fatalf("want %s refused, got %v", c.field, err)
			}
			if r.uow.dos != 0 || r.clk.reads != 0 || len(r.rec.list()) != 0 || r.m.counted() || r.wake.n != 0 || buf.Len() != 0 {
				t.Fatalf("a refusal touches nothing: dos %d reads %d calls %q log %q", r.uow.dos, r.clk.reads, r.rec.list(), buf.String())
			}
		})
	}

	r := newRig(t)
	got, err := r.svc.SaveDriver(context.Background(), SaveDriverCommand{
		ID: "d1", Name: "  Bo   Tran ", Phone: "+84 90 123 4567", VehicleType: " bike ", Plate: "29a-123.45", Model: " Wave ", Color: " red ",
	})
	if err != nil {
		t.Fatal(err)
	}
	d := got.Driver
	if d.Name != "Bo Tran" || d.Vehicle != domain.VehicleBike || d.Plate != "29A-123.45" || d.Model != "Wave" || d.Color != "red" {
		t.Fatalf("the use case stores cleaned values, got %+v", d)
	}
	if r.uow.drivers["d1"] != d {
		t.Fatalf("stored %+v, returned %+v", r.uow.drivers["d1"], d)
	}
}

func TestSaveDriverReadsTheClockBeforeTheLockAndEncodesBeforeUpdating(t *testing.T) {
	r := newRig(t)
	captureLogInto(t, r.rec)
	r.uow.drivers["d1"] = approvedDriver("d1")
	cmd := saveCmd(approvedDriver("d1"))
	cmd.Plate = "51F-12345"
	if _, err := r.svc.SaveDriver(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	wantEvents(t, r, "Begin", "now", "LockOrCreateDriver", "Encode", "QueueReview", "UpdateProfile", "Commit",
		"Review(vehicle_changed)", "Wake", "Updated(driver)",
		"log: driver d1 changed vehicle: back to PENDING, out of service until approved")

	r.rec.reset()
	cmd.Model = "Camry"
	if _, err := r.svc.SaveDriver(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	wantEvents(t, r, "Begin", "now", "LockOrCreateDriver", "UpdateProfile", "Commit",
		"Updated(driver)", "log: driver d1 updated their profile")

	// A first save creates the row with the time read before the lock.
	r.rec.reset()
	got, err := r.svc.SaveDriver(context.Background(), SaveDriverCommand{ID: "new", Name: "Cy"})
	if err != nil {
		t.Fatal(err)
	}
	wantEvents(t, r, "Begin", "now", "LockOrCreateDriver", "UpdateProfile", "Commit",
		"Updated(driver)", "log: driver new updated their profile")
	if got.Driver.Status != domain.StatusNone || got.Driver.StatusAt != rigNow || got.Driver.UpdatedAt != rigNow || got.VehicleChanged {
		t.Fatalf("got %+v", got)
	}
}

func TestAnIncompletePastNoneProfileIsRefusedAfterTheLock(t *testing.T) {
	for _, c := range []struct {
		name    string
		vehicle string
		plate   string
		missing []string
	}{
		{"both", "", "", []string{"vehicleType", "plate"}},
		{"plate", "CAR", "", []string{"plate"}},
		{"vehicle", "", "29A-123", []string{"vehicleType"}},
	} {
		for _, status := range []string{domain.StatusPending, domain.StatusApproved, domain.StatusRejected, domain.StatusSuspended} {
			t.Run(c.name+"/"+status, func(t *testing.T) {
				r := newRig(t)
				buf := captureLog(t)
				before := approvedDriver("d1")
				before.Status = status
				r.uow.drivers["d1"] = before
				_, err := r.svc.SaveDriver(context.Background(), SaveDriverCommand{ID: "d1", Name: "Bo", VehicleType: c.vehicle, Plate: c.plate})
				var ie *domain.IncompleteError
				if !errors.As(err, &ie) || !reflect.DeepEqual(ie.Missing, c.missing) {
					t.Fatalf("want missing %v, got %v", c.missing, err)
				}
				wantEvents(t, r, "Begin", "now", "LockOrCreateDriver", "Rollback")
				if r.uow.drivers["d1"] != before || len(r.uow.outbox) != 0 || r.m.counted() || r.wake.n != 0 || buf.Len() != 0 {
					t.Fatalf("nothing changes: %+v outbox %v log %q", r.uow.drivers["d1"], r.uow.outbox, buf.String())
				}
			})
		}
	}

	// NONE may be saved incomplete.
	r := newRig(t)
	if _, err := r.svc.SaveDriver(context.Background(), SaveDriverCommand{ID: "d1", Name: "Bo"}); err != nil {
		t.Fatalf("a NONE driver saves an incomplete profile, got %v", err)
	}
}

func TestAVehicleChangeQueuesASuspendedResultBySystemAndWakes(t *testing.T) {
	r := newRig(t)
	before := approvedDriver("d1")
	// The previous result is later than the clock: the new one is one microsecond after it.
	last := rigNow.Add(time.Second)
	before.ReviewedAt = &last
	r.uow.drivers["d1"] = before
	cmd := saveCmd(before)
	cmd.VehicleType = "BIKE"

	got, err := r.svc.SaveDriver(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	at := last.Add(time.Microsecond)
	d := got.Driver
	if !got.VehicleChanged || d.Status != domain.StatusPending || d.Reason != domain.VehicleChangedReason ||
		d.ReviewedBy != domain.SystemReviewer || d.ReviewedAt == nil || !d.ReviewedAt.Equal(at) ||
		d.StatusAt != rigNow || d.UpdatedAt != rigNow || d.Vehicle != domain.VehicleBike {
		t.Fatalf("got %+v", got)
	}
	if !reflect.DeepEqual(r.uow.drivers["d1"], d) {
		t.Fatalf("stored %+v, returned %+v", r.uow.drivers["d1"], d)
	}
	if len(r.enc.events) != 1 {
		t.Fatalf("one event, got %+v", r.enc.events)
	}
	ev := r.enc.events[0]
	if ev.DriverID != "d1" || ev.Status != domain.StatusSuspended || ev.Vehicle != domain.VehicleBike ||
		ev.Reason != domain.VehicleChangedReason || ev.ReviewedBy != domain.SystemReviewer || !ev.OccurredAt.Equal(at) {
		t.Fatalf("event %+v", ev)
	}
	if len(r.uow.outbox) != 1 || r.uow.outbox[0].DriverID != "d1" || string(r.uow.outbox[0].Payload) != "event:"+ev.EventID {
		t.Fatalf("outbox %+v", r.uow.outbox)
	}
	if r.wake.n != 1 || !reflect.DeepEqual(r.m.reviews, []string{"vehicle_changed"}) || !reflect.DeepEqual(r.m.updated, []string{"driver"}) {
		t.Fatalf("wake %d reviews %v updated %v", r.wake.n, r.m.reviews, r.m.updated)
	}
}

func TestOnlyAVehicleChangeCountsAReviewAndWakes(t *testing.T) {
	for _, c := range []struct {
		name    string
		status  string
		edit    func(*SaveDriverCommand)
		changed bool
	}{
		{"model only", domain.StatusApproved, func(c *SaveDriverCommand) { c.Model = "Camry" }, false},
		{"colour only", domain.StatusApproved, func(c *SaveDriverCommand) { c.Color = "black" }, false},
		{"name and phone", domain.StatusApproved, func(c *SaveDriverCommand) { c.Name, c.Phone = "Bo Tran", "0909999999" }, false},
		{"plate", domain.StatusApproved, func(c *SaveDriverCommand) { c.Plate = "51F-12345" }, true},
		{"vehicle type", domain.StatusApproved, func(c *SaveDriverCommand) { c.VehicleType = "bike" }, true},
		{"plate while pending", domain.StatusPending, func(c *SaveDriverCommand) { c.Plate = "51F-12345" }, false},
		{"plate while rejected", domain.StatusRejected, func(c *SaveDriverCommand) { c.Plate = "51F-12345" }, false},
		{"plate while suspended", domain.StatusSuspended, func(c *SaveDriverCommand) { c.Plate = "51F-12345" }, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t)
			before := approvedDriver("d1")
			before.Status = c.status
			r.uow.drivers["d1"] = before
			cmd := saveCmd(before)
			c.edit(&cmd)
			got, err := r.svc.SaveDriver(context.Background(), cmd)
			if err != nil {
				t.Fatal(err)
			}
			if got.VehicleChanged != c.changed {
				t.Fatalf("VehicleChanged %v", got.VehicleChanged)
			}
			if !reflect.DeepEqual(r.m.updated, []string{"driver"}) {
				t.Fatalf("updated %v", r.m.updated)
			}
			if c.changed {
				if r.wake.n != 1 || !reflect.DeepEqual(r.m.reviews, []string{"vehicle_changed"}) || len(r.uow.outbox) != 1 {
					t.Fatalf("wake %d reviews %v outbox %d", r.wake.n, r.m.reviews, len(r.uow.outbox))
				}
				return
			}
			if r.wake.n != 0 || len(r.m.reviews) != 0 || len(r.uow.outbox) != 0 || len(r.enc.events) != 0 {
				t.Fatalf("no review: wake %d reviews %v outbox %d", r.wake.n, r.m.reviews, len(r.uow.outbox))
			}
			if got.Driver.Status != c.status || got.Driver.Reason != before.Reason || got.Driver.StatusAt != before.StatusAt || got.Driver.UpdatedAt != rigNow {
				t.Fatalf("only the fields and UpdatedAt change: %+v", got.Driver)
			}
		})
	}
}

func TestSaveDriverLogsWhichKindOfUpdateItWas(t *testing.T) {
	r := newRig(t)
	buf := captureLog(t)
	r.uow.drivers["d1"] = approvedDriver("d1")
	cmd := saveCmd(approvedDriver("d1"))
	cmd.Model = "Camry"
	if _, err := r.svc.SaveDriver(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	cmd.Plate = "51F-12345"
	if _, err := r.svc.SaveDriver(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	want := "driver d1 updated their profile\n" +
		"driver d1 changed vehicle: back to PENDING, out of service until approved\n"
	if buf.String() != want {
		t.Fatalf("log %q, want %q", buf.String(), want)
	}
}

func TestSaveDriverFailuresRollBackAndCountNothing(t *testing.T) {
	for _, step := range []string{"LockOrCreateDriver", "QueueReview", "UpdateProfile", "commit"} {
		t.Run(step, func(t *testing.T) {
			r := newRig(t)
			buf := captureLog(t)
			before := approvedDriver("d1")
			r.uow.drivers["d1"] = before
			if step == "commit" {
				r.uow.commitErr = errBoom
			} else {
				r.uow.failOn, r.uow.failErr = step, errBoom
			}
			cmd := saveCmd(before)
			cmd.Plate = "51F-12345"
			_, err := r.svc.SaveDriver(context.Background(), cmd)
			if !errors.Is(err, errBoom) || err.Error() != "boom" {
				t.Fatalf("a port error is returned unwrapped, got %v", err)
			}
			if r.uow.drivers["d1"] != before || len(r.uow.outbox) != 0 || r.m.counted() || r.wake.n != 0 || buf.Len() != 0 {
				t.Fatalf("nothing kept, counted, woken or logged: %+v %v %q", r.uow.drivers["d1"], r.uow.outbox, buf.String())
			}
			if calls := r.rec.list(); calls[len(calls)-1] != "Rollback" || (step == "QueueReview" && slices.Contains(calls, "UpdateProfile")) {
				t.Fatalf("calls %q", calls)
			}
		})
	}
}

func TestApplyOrderIsLockCheckClockMarkPending(t *testing.T) {
	for _, status := range []string{domain.StatusNone, domain.StatusRejected} {
		t.Run(status, func(t *testing.T) {
			r := newRig(t)
			captureLogInto(t, r.rec)
			before := approvedDriver("d1")
			before.Status, before.Reason = status, "blurry photo"
			r.uow.drivers["d1"] = before
			got, err := r.svc.Apply(context.Background(), "d1")
			if err != nil {
				t.Fatal(err)
			}
			wantEvents(t, r, "Begin", "LockDriver", "now", "MarkPending", "Commit",
				"Application(submitted)", "log: driver d1 applied: PENDING")
			d := got.Driver
			if !got.Submitted || d.Status != domain.StatusPending || d.Reason != "" || d.StatusAt != rigNow || d.UpdatedAt != rigNow {
				t.Fatalf("got %+v", got)
			}
			if !reflect.DeepEqual(r.uow.drivers["d1"], d) {
				t.Fatalf("stored %+v, returned %+v", r.uow.drivers["d1"], d)
			}
			if r.wake.n != 0 {
				t.Fatal("apply never wakes")
			}
		})
	}
}

func TestApplyReplayWritesNothingCountsReplayedAndDoesNotWake(t *testing.T) {
	for _, status := range []string{domain.StatusPending, domain.StatusApproved} {
		t.Run(status, func(t *testing.T) {
			r := newRig(t)
			buf := captureLogInto(t, r.rec)
			before := approvedDriver("d1")
			before.Status = status
			r.uow.drivers["d1"] = before
			got, err := r.svc.Apply(context.Background(), "d1")
			if err != nil {
				t.Fatal(err)
			}
			wantEvents(t, r, "Begin", "LockDriver", "Commit", "Application(replayed)")
			if got.Submitted || !reflect.DeepEqual(got.Driver, before) || r.uow.drivers["d1"] != before {
				t.Fatalf("got %+v", got)
			}
			if r.clk.reads != 0 || r.wake.n != 0 || buf.Len() != 0 {
				t.Fatalf("reads %d wake %d log %q", r.clk.reads, r.wake.n, buf.String())
			}
		})
	}
}

func TestApplyBeforeAnyProfileIsErrNoProfile(t *testing.T) {
	r := newRig(t)
	buf := captureLog(t)
	_, err := r.svc.Apply(context.Background(), "d1")
	if !errors.Is(err, domain.ErrNoProfile) || errors.Is(err, domain.ErrNoDriver) {
		t.Fatalf("got %v", err)
	}
	wantEvents(t, r, "Begin", "LockDriver", "Rollback")
	if r.m.counted() || buf.Len() != 0 || len(r.uow.drivers) != 0 {
		t.Fatalf("counters %+v log %q", r.m, buf.String())
	}

	// Missing fields in the order name, vehicleType, plate; checked before the clock.
	r = newRig(t)
	r.uow.drivers["d1"] = domain.Driver{ID: "d1", Status: domain.StatusNone}
	_, err = r.svc.Apply(context.Background(), "d1")
	var ie *domain.IncompleteError
	if !errors.As(err, &ie) || !reflect.DeepEqual(ie.Missing, []string{"name", "vehicleType", "plate"}) {
		t.Fatalf("got %v", err)
	}
	wantEvents(t, r, "Begin", "LockDriver", "Rollback")

	// Any other lock error is returned as it is.
	r = newRig(t)
	r.uow.failOn, r.uow.failErr = "LockDriver", errBoom
	if _, err := r.svc.Apply(context.Background(), "d1"); err != errBoom {
		t.Fatalf("got %v", err)
	}
	// A failing write or commit counts nothing.
	for _, fail := range []func(*fakeUoW){
		func(u *fakeUoW) { u.failOn, u.failErr = "MarkPending", errBoom },
		func(u *fakeUoW) { u.commitErr = errBoom },
	} {
		r = newRig(t)
		buf.Reset()
		before := approvedDriver("d1")
		before.Status = domain.StatusNone
		r.uow.drivers["d1"] = before
		fail(r.uow)
		if _, err := r.svc.Apply(context.Background(), "d1"); err != errBoom {
			t.Fatalf("got %v", err)
		}
		if r.uow.drivers["d1"] != before || r.m.counted() || buf.Len() != 0 {
			t.Fatalf("kept %+v counters %+v log %q", r.uow.drivers["d1"], r.m, buf.String())
		}
	}
}

func TestASuspendedDriverCannotApply(t *testing.T) {
	r := newRig(t)
	buf := captureLog(t)
	before := approvedDriver("d1")
	before.Status = domain.StatusSuspended
	r.uow.drivers["d1"] = before
	_, err := r.svc.Apply(context.Background(), "d1")
	if !errors.Is(err, domain.ErrSuspended) {
		t.Fatalf("got %v", err)
	}
	wantEvents(t, r, "Begin", "LockDriver", "Rollback")
	if r.uow.drivers["d1"] != before || r.m.counted() || r.wake.n != 0 || buf.Len() != 0 {
		t.Fatalf("nothing changes: %+v log %q", r.uow.drivers["d1"], buf.String())
	}
}

func TestANilWakerIsAccepted(t *testing.T) {
	r := newRig(t)
	captureLog(t)
	// An untyped nil Relay, as the CLI and the tests pass it.
	svc := New(Deps{UoW: r.uow, Encoder: r.enc, Metrics: r.m, Now: r.clk.Now})
	r.uow.drivers["d1"] = approvedDriver("d1")
	cmd := saveCmd(approvedDriver("d1"))
	cmd.Plate = "51F-12345"
	if got, err := svc.SaveDriver(context.Background(), cmd); err != nil || !got.VehicleChanged {
		t.Fatalf("got %+v %v", got, err)
	}
	if _, err := svc.Review(context.Background(), ReviewCommand{DriverID: "d1", Action: "approve", AdminID: "ops-1"}); err != nil {
		t.Fatal(err)
	}
	if len(r.m.reviews) != 2 {
		t.Fatalf("reviews %v", r.m.reviews)
	}
}

func TestDriverLogLinesCarryOnlyIds(t *testing.T) {
	const name, phone, plate, newPlate, model, color = "Zebediah Secretname", "+84 999 888 777", "77Q-SECRET1", "88Q-SECRET2", "Secretmodel", "Secretcolour"
	secrets := []string{"Zebediah", "Secretname", "999", "SECRET1", "SECRET2", "Secretmodel", "Secretcolour"}
	r := newRig(t)
	buf := captureLog(t)
	ctx := context.Background()
	var errs []error
	keep := func(err error) { errs = append(errs, err) }

	cmd := SaveDriverCommand{ID: "d1", Name: name, Phone: phone, VehicleType: "CAR", Plate: plate, Model: model, Color: color}
	_, err := r.svc.SaveDriver(ctx, cmd)
	keep(err)
	_, err = r.svc.Apply(ctx, "d1")
	keep(err)
	_, err = r.svc.Apply(ctx, "d1") // replay
	keep(err)
	_, err = r.svc.Review(ctx, ReviewCommand{DriverID: "d1", Action: "approve", AdminID: "ops-1"})
	keep(err)
	cmd.Plate = newPlate // vehicle change
	_, err = r.svc.SaveDriver(ctx, cmd)
	keep(err)
	_, err = r.svc.Review(ctx, ReviewCommand{DriverID: "d1", Action: "suspend", AdminID: "ops-1", Reason: "late"}) // PENDING: refused
	r.enc.err = errBoom
	_, err2 := r.svc.Review(ctx, ReviewCommand{DriverID: "d1", Action: "approve", AdminID: "ops-1"})
	_, err3 := r.svc.SaveDriver(ctx, SaveDriverCommand{ID: "d1", Name: name, Phone: phone, VehicleType: "CAR", Plate: "x"})
	if err == nil || err2 == nil || err3 == nil {
		t.Fatalf("want three refusals, got %v %v %v", err, err2, err3)
	}
	errs = append(errs, err, err2, err3)

	out := buf.String()
	for _, e := range errs {
		if e != nil {
			out += "\nerr: " + e.Error()
		}
	}
	for _, s := range secrets {
		if strings.Contains(out, s) {
			t.Fatalf("personal data %q in the log or an error: %q", s, out)
		}
	}
	for _, line := range []string{"driver d1 updated their profile", "driver d1 applied: PENDING", "driver d1: approve by ops-1 -> APPROVED", "driver d1 changed vehicle"} {
		if !strings.Contains(out, line) {
			t.Fatalf("missing %q in %q", line, out)
		}
	}
}
