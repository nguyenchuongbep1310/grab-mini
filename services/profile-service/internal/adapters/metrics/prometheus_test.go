package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"grab-mini/profile-service/internal/app"
)

var _ app.Metrics = (*Metrics)(nil)

func family(t *testing.T, name string) *dto.MetricFamily {
	t.Helper()
	fams, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

func value(t *testing.T, name, label, v string) float64 {
	t.Helper()
	f := family(t, name)
	if f == nil {
		return 0
	}
	for _, m := range f.Metric {
		for _, l := range m.Label {
			if l.GetName() == label && l.GetValue() == v {
				return m.Counter.GetValue()
			}
		}
	}
	return 0
}

// The strings recorded in baseline.md (Task 1): dashboards and alerts key on them.
func TestTheCountersKeepTheirNamesLabelsAndHelp(t *testing.T) {
	m := New()
	m.Updated("rider")
	m.Application("submitted")
	m.Review("approved")
	m.Refused("forbidden")
	m.PublicLookup("found")
	m.ResyncEvents("APPROVED", 1)
	m.ResyncRun("completed")
	for _, tc := range []struct{ name, label, help string }{
		{"profile_updates_total", "kind", "PUT /riders/{id} and PUT /drivers/{id} that were accepted, by kind (rider, driver)."},
		{"profile_applications_total", "result", "POST /drivers/{id}/application, by result: submitted (now PENDING), replayed (already PENDING or APPROVED: nothing changed)."},
		{"profile_reviews_total", "decision", "driver.application.reviewed events queued, by decision: approved, rejected, suspended, vehicle_changed (an approved driver changed plate or vehicle type and is out of service until approved again)."},
		{"profile_requests_rejected_total", "reason", "Requests refused, by reason: invalid_request, invalid_profile, forbidden, not_found, incomplete_profile, suspended, invalid_transition, rate_limited, resync_running."},
		{"profile_public_lookups_total", "result", "GET /drivers/{id}/public, by result: found, not_found (no such driver, or not approved: the two look the same)."},
		{"profile_resync_events_total", "status", "driver.application.reviewed events queued by a resync, by the status they repeat (APPROVED, REJECTED, SUSPENDED)."},
		{"profile_resync_runs_total", "result", "Resyncs (POST /driver-applications/resync or the resync command), by result: completed, failed, refused_running (another one was running)."},
	} {
		f := family(t, tc.name)
		if f == nil {
			t.Errorf("%s is not registered", tc.name)
			continue
		}
		if f.GetHelp() != tc.help {
			t.Errorf("%s help = %q", tc.name, f.GetHelp())
		}
		if f.GetType() != dto.MetricType_COUNTER {
			t.Errorf("%s is not a counter", tc.name)
		}
		if len(f.Metric[0].Label) != 1 || f.Metric[0].Label[0].GetName() != tc.label {
			t.Errorf("%s labels = %v, want [%s]", tc.name, f.Metric[0].Label, tc.label)
		}
	}
}

func TestEachMethodMovesItsOwnCounter(t *testing.T) {
	m := New()
	for _, tc := range []struct {
		name, label, v string
		by             float64
		do             func()
	}{
		{"profile_updates_total", "kind", "driver", 1, func() { m.Updated("driver") }},
		{"profile_applications_total", "result", "replayed", 1, func() { m.Application("replayed") }},
		{"profile_reviews_total", "decision", "vehicle_changed", 1, func() { m.Review("vehicle_changed") }},
		{"profile_requests_rejected_total", "reason", "rate_limited", 1, func() { m.Refused("rate_limited") }},
		{"profile_public_lookups_total", "result", "not_found", 1, func() { m.PublicLookup("not_found") }},
		{"profile_resync_events_total", "status", "SUSPENDED", 7, func() { m.ResyncEvents("SUSPENDED", 7) }},
		{"profile_resync_runs_total", "result", "refused_running", 1, func() { m.ResyncRun("refused_running") }},
	} {
		before := value(t, tc.name, tc.label, tc.v)
		tc.do()
		if got := value(t, tc.name, tc.label, tc.v) - before; got != tc.by {
			t.Errorf("%s{%s=%q} moved by %v, want %v", tc.name, tc.label, tc.v, got, tc.by)
		}
	}
}

func TestCreatingTheWrapperTwiceDoesNotRegisterTwice(t *testing.T) {
	a, b := New(), New()
	before := value(t, "profile_updates_total", "kind", "rider")
	a.Updated("rider")
	b.Updated("rider")
	if got := value(t, "profile_updates_total", "kind", "rider") - before; got != 2 {
		t.Errorf("moved by %v, want 2 (one shared counter)", got)
	}
}
