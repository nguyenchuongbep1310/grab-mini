package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

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

func value(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	f := family(t, name)
	if f == nil {
		return 0
	}
next:
	for _, m := range f.Metric {
		for _, l := range m.Label {
			if labels[l.GetName()] != l.GetValue() {
				continue next
			}
		}
		if f.GetType() == dto.MetricType_COUNTER {
			return m.Counter.GetValue()
		}
	}
	return 0
}

// The strings recorded in baseline.md (Task 1): dashboards and alerts key on them.
func TestTheCountersKeepTheirNamesLabelsAndHelp(t *testing.T) {
	m := New()
	m.Validation("ok")
	m.Redemption("REDEEMED", "")
	m.Released(1)
	m.Duplicate()
	for _, tc := range []struct {
		name, help string
		labels     []string
	}{
		{"promotion_validations_total", "GET /promotions/validate answers, by result: ok or the rejection code (not_found, expired, not_started, min_fare, wrong_vehicle, limit_reached, already_used).", []string{"result"}},
		{"promotion_redemptions_total", "Redemptions recorded from order.created, by status (REDEEMED, OVER_LIMIT) and, for OVER_LIMIT, the limit reached (usage_limit, per_rider_limit).", []string{"reason", "status"}},
		{"promotion_redemptions_released_total", "Redemptions given back because the order ended without a ride (order.cancelled, a cancelled trip, order.no_driver_found).", nil},
		{"promotion_redemptions_duplicate_total", "Events for a redemption or release that was already applied (redelivery or replay): nothing changed.", nil},
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
		var got []string
		for _, l := range f.Metric[0].Label {
			got = append(got, l.GetName())
		}
		if len(got) != len(tc.labels) {
			t.Errorf("%s labels = %v, want %v", tc.name, got, tc.labels)
			continue
		}
		for i := range got {
			if got[i] != tc.labels[i] {
				t.Errorf("%s labels = %v, want %v", tc.name, got, tc.labels)
			}
		}
	}
}

func TestRedemptionCountsWithAnEmptyReasonLabelForRedeemed(t *testing.T) {
	m := New()
	redeemed := map[string]string{"status": "REDEEMED", "reason": ""}
	over := map[string]string{"status": "OVER_LIMIT", "reason": "usage_limit"}
	r0, o0 := value(t, "promotion_redemptions_total", redeemed), value(t, "promotion_redemptions_total", over)
	m.Redemption("REDEEMED", "")
	m.Redemption("OVER_LIMIT", "usage_limit")
	m.Redemption("OVER_LIMIT", "usage_limit")
	if got := value(t, "promotion_redemptions_total", redeemed) - r0; got != 1 {
		t.Errorf("REDEEMED/\"\" moved by %v, want 1", got)
	}
	if got := value(t, "promotion_redemptions_total", over) - o0; got != 2 {
		t.Errorf("OVER_LIMIT/usage_limit moved by %v, want 2", got)
	}
	rel := value(t, "promotion_redemptions_released_total", nil)
	m.Released(3)
	if got := value(t, "promotion_redemptions_released_total", nil) - rel; got != 3 {
		t.Errorf("released moved by %v, want 3", got)
	}
}
