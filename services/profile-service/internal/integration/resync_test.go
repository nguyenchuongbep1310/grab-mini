package integration

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/profile-service/internal/adapters/postgres"
	"grab-mini/profile-service/internal/app"
)

func (f *fixture) resync(body any) result {
	return f.call("POST", "/driver-applications/resync", auth.Admin, "ops-1", body)
}

func eventTime(t *testing.T, e events.DriverApplicationReviewed) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339Nano, e.OccurredAt)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

// fleet: two approved, one rejected, one suspended, one pending, one with only a profile.
func (f *fixture) fleet() {
	f.approved("driver-a")
	f.approved("driver-b")
	f.applied("driver-r")
	f.decide("reject", "driver-r", "documents unreadable")
	f.approved("driver-s")
	f.decide("suspend", "driver-s", "complaints")
	f.applied("driver-p")
	f.putDriver("driver-n", vehicle("30G-246.80"))
}

// profileRows is every column of every driver row, in driver_id order: a resync must change
// none of them.
func (f *fixture) profileRows() string {
	f.t.Helper()
	rows, err := f.db.Query(context.Background(), `SELECT * FROM driver_profiles ORDER BY driver_id`)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			f.t.Fatal(err)
		}
		fmt.Fprintf(&b, "%#v\n", vals)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	return b.String()
}

// counter is one series of the service's counters, read through the default registry as
// /metrics does (the counters are process-wide: tests compare before and after).
func counter(t *testing.T, series string) float64 {
	t.Helper()
	return gCounters(t)[series]
}

func TestResyncRepublishesTheCurrentStatusOfEveryEligibleDriverAndChangesNoRow(t *testing.T) {
	f := newFixture(t)
	f.fleet()
	before := f.queued()
	rowsBefore := f.profileRows()
	last := map[string]time.Time{}
	for _, e := range before {
		last[e.DriverID] = eventTime(t, e)
	}
	runs := counter(t, `profile_resync_runs_total{result="completed"}`)
	approvedBefore := counter(t, `profile_resync_events_total{status="APPROVED"}`)

	f.now = f.now.Add(time.Minute)
	r := f.resync(nil)
	wantStatus(t, "resync", r, 200)
	counts, _ := r.body["counts"].(map[string]any)
	if r.body["queued"] != float64(4) || counts["APPROVED"] != float64(2) || counts["REJECTED"] != float64(1) || counts["SUSPENDED"] != float64(1) {
		t.Errorf("body = %s", r.raw)
	}
	if got := f.profileRows(); got != rowsBefore {
		t.Errorf("a resync changed profile rows:\nbefore %s\nafter  %s", rowsBefore, got)
	}
	got := f.queued()[len(before):]
	if len(got) != 4 {
		t.Fatalf("%d new events, want 4: %+v", len(got), got)
	}
	want := map[string]struct{ status, reason string }{
		"driver-a": {"APPROVED", "resync"}, "driver-b": {"APPROVED", "resync"},
		"driver-r": {"REJECTED", "documents unreadable"}, "driver-s": {"SUSPENDED", "complaints"},
	}
	for _, e := range got {
		w, ok := want[e.DriverID]
		if !ok {
			t.Errorf("event for %s, who is not APPROVED, REJECTED or SUSPENDED", e.DriverID)
			continue
		}
		delete(want, e.DriverID)
		if e.Status != w.status || e.Reason != w.reason || e.ReviewedBy != "resync" || e.VehicleType != events.VehicleCar {
			t.Errorf("%s: %+v, want status %s reason %q reviewedBy resync CAR", e.DriverID, e, w.status, w.reason)
		}
		if !eventTime(t, e).After(last[e.DriverID]) {
			t.Errorf("%s: occurredAt %s is not after their previous result %s", e.DriverID, e.OccurredAt, last[e.DriverID])
		}
	}
	if len(want) != 0 {
		t.Errorf("no event for %v", want)
	}
	if d := counter(t, `profile_resync_runs_total{result="completed"}`) - runs; d != 1 {
		t.Errorf("completed runs +%v, want 1", d)
	}
	if d := counter(t, `profile_resync_events_total{status="APPROVED"}`) - approvedBefore; d != 2 {
		t.Errorf("APPROVED events counted +%v, want 2", d)
	}
}

func TestResyncEventsStayLaterThanThePreviousResultEvenIfTheClockStepsBackAndRepeats(t *testing.T) {
	f := newFixture(t)
	f.approved("driver-1")
	f.now = f.now.Add(-time.Hour)
	wantStatus(t, "first", f.resync(nil), 200)
	wantStatus(t, "second", f.resync(nil), 200)
	got := f.queued()
	if len(got) != 3 {
		t.Fatalf("%d events", len(got))
	}
	// Dated from the row's reviewed_at, which a resync does not move: both resyncs come after
	// the approval, and neither is older than it.
	for _, e := range got[1:] {
		if !eventTime(t, e).After(eventTime(t, got[0])) {
			t.Errorf("resync event %s is not after the approval %s", e.OccurredAt, got[0].OccurredAt)
		}
	}
}

func TestResyncOfOneDriver(t *testing.T) {
	f := newFixture(t)
	f.fleet()
	n := len(f.queued())

	r := f.resync(map[string]string{"driverId": "driver-s"})
	wantStatus(t, "one driver", r, 200)
	if r.body["queued"] != float64(1) {
		t.Errorf("body = %s", r.raw)
	}
	got := f.queued()[n:]
	if len(got) != 1 || got[0].DriverID != "driver-s" || got[0].Status != "SUSPENDED" {
		t.Errorf("events = %+v", got)
	}

	for _, id := range []string{"driver-p", "driver-n"} { // PENDING and NONE emit nothing
		r := f.resync(map[string]string{"driverId": id})
		wantStatus(t, id, r, 200)
		if r.body["queued"] != float64(0) || r.body["skipped"] != float64(1) {
			t.Errorf("%s: %s", id, r.raw)
		}
	}
	if len(f.queued()) != n+1 {
		t.Errorf("a PENDING or NONE driver got an event")
	}
	wantCode(t, "unknown driver", f.resync(map[string]string{"driverId": "nobody"}), 404, "not_found")
	wantCode(t, "bad json", f.resync("{nope"), 400, "invalid_request")
	if len(f.queued()) != n+1 {
		t.Errorf("a refused resync queued events")
	}
}

func TestResyncIsForAdminsOnly(t *testing.T) {
	f := newFixture(t)
	f.approved("driver-1")
	n := len(f.queued())
	wantStatus(t, "no token", f.call("POST", "/driver-applications/resync", auth.Admin, "", nil), 401)
	wantStatus(t, "driver", f.call("POST", "/driver-applications/resync", auth.Driver, "driver-1", nil), 403)
	wantStatus(t, "rider", f.call("POST", "/driver-applications/resync", auth.Rider, "rider-1", nil), 403)
	if len(f.queued()) != n {
		t.Error("a refused caller queued events")
	}
}

func TestResyncGoesThroughAllDriversInBatches(t *testing.T) {
	f := newFixture(t)
	const total = 2*app.ResyncBatch + 37
	_, err := f.db.Exec(context.Background(), `
		INSERT INTO driver_profiles (driver_id, name, vehicle_type, plate, status, reviewed_by, reviewed_at, status_at, updated_at)
		SELECT 'bulk-' || lpad(i::text, 5, '0'), 'n', CASE WHEN i % 2 = 0 THEN 'CAR' ELSE 'BIKE' END, 'P' || i,
		       CASE i % 3 WHEN 0 THEN 'APPROVED' WHEN 1 THEN 'REJECTED' ELSE 'SUSPENDED' END,
		       'ops-1', now(), now(), now()
		FROM generate_series(1, $1) AS i`, total)
	if err != nil {
		t.Fatal(err)
	}
	r := f.resync(nil)
	wantStatus(t, "resync", r, 200)
	if r.body["queued"] != float64(total) {
		t.Fatalf("body = %s, want %d queued", r.raw, total)
	}
	seen := map[string]bool{}
	for _, e := range f.queued() {
		if seen[e.DriverID] {
			t.Errorf("%s twice", e.DriverID)
		}
		seen[e.DriverID] = true
	}
	if len(seen) != total {
		t.Errorf("%d drivers got an event, want %d", len(seen), total)
	}
}

func TestOnlyOneResyncRunsAtATime(t *testing.T) {
	f := newFixture(t)
	f.approved("driver-1")
	n := len(f.queued())
	ctx := context.Background()

	// Another instance (or the CLI) holds the lock.
	other, err := f.db.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Exec(ctx, `SELECT pg_advisory_lock($1)`, postgres.ResyncLockKey); err != nil {
		t.Fatal(err)
	}
	refused := counter(t, `profile_requests_rejected_total{reason="resync_running"}`)
	wantCode(t, "while running", f.resync(nil), 409, "resync_running")
	if d := counter(t, `profile_requests_rejected_total{reason="resync_running"}`) - refused; d != 1 {
		t.Errorf("resync_running counted +%v", d)
	}
	if len(f.queued()) != n {
		t.Error("the refused resync queued events")
	}
	if _, err := other.Exec(ctx, `SELECT pg_advisory_unlock($1)`, postgres.ResyncLockKey); err != nil {
		t.Fatal(err)
	}
	other.Release()

	wantStatus(t, "after it ended", f.resync(nil), 200)
	wantStatus(t, "again", f.resync(nil), 200) // the lock is released after every run
}

func TestAFailedResyncQueuesNothingFromItsBatchAndReleasesTheLock(t *testing.T) {
	f := newFixture(t)
	f.approved("driver-1")
	n := len(f.queued())
	f.useEncoder(failingEncoder{})
	failed := counter(t, `profile_resync_runs_total{result="failed"}`)
	wantCode(t, "failing", f.resync(nil), 500, "internal")
	f.useEncoder(jsonEncoder{})
	if len(f.queued()) != n {
		t.Error("a failed batch left events behind")
	}
	if d := counter(t, `profile_resync_runs_total{result="failed"}`) - failed; d != 1 {
		t.Errorf("failed runs +%v", d)
	}
	wantStatus(t, "retry", f.resync(nil), 200)
}

func TestResyncNeverLogsPersonalData(t *testing.T) {
	f := newFixture(t)
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	body := map[string]any{"name": "Zephyrine Quillfeather", "phone": "0912 345 678", "vehicleType": "CAR", "plate": "77Z-987.65", "model": "Batmobile", "color": "Chartreuse"}
	f.putDriver("driver-1", body)
	f.apply("driver-1")
	f.decide("approve", "driver-1", "")
	buf.Reset()
	wantStatus(t, "resync", f.resync(nil), 200)
	wantStatus(t, "one", f.resync(map[string]string{"driverId": "driver-1"}), 200)
	logged := buf.String()
	if !strings.Contains(logged, "resync by ops-1") {
		t.Errorf("the resync was not logged:\n%s", logged)
	}
	for _, s := range []string{"Zephyrine", "0912", "77Z-987.65", "Batmobile", "Chartreuse"} {
		if strings.Contains(logged, s) {
			t.Errorf("%q reached the log:\n%s", s, logged)
		}
	}
}
