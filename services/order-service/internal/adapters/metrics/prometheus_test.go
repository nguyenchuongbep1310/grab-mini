package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"grab-mini/order-service/internal/app"
)

var _ app.Metrics = (*Metrics)(nil)

// The names, labels and help strings are the ones recorded in baseline.md;
// dashboards and alerts depend on them.
func TestTheCountersKeepTheirNamesLabelsAndHelp(t *testing.T) {
	m := New()
	m.OrderCreated("CAR")
	m.OrderCancelled()
	m.Pricing("quote")
	m.SurgeFailOpen()
	m.Promo("applied")
	m.Idempotent("new")

	want := map[string]struct {
		help   string
		labels []string
	}{
		"orders_created_total":         {"Orders accepted (saved with their order.created event), by vehicle type.", []string{"vehicle_type"}},
		"orders_cancelled_total":       {"Orders cancelled by the rider while still PENDING (saved with their order.cancelled event).", nil},
		"orders_idempotency_total":     {"POST /orders carrying an Idempotency-Key, by result: new (first use), replayed (original response returned), mismatch (422, same key with another body).", []string{"result"}},
		"orders_promo_total":           {"Promo tokens presented at booking, by result: applied, unchecked (Redis down: the single-use check was skipped, the discount was applied), used (409, already used for another booking), or the refusal (invalid, expired, wrong_rider, wrong_price, wrong_vehicle: 400).", []string{"result"}},
		"orders_pricing_total":         {"How bookings were priced: quote (the rider's quote honoured), repriced (no or stale quote, current multiplier used), requote (409, multiplier moved too far), quote_reused (409, the quote already priced another order), quote_unchecked (Redis down: the single-use check was skipped, current multiplier used), invalid_quote (400).", []string{"result"}},
		"orders_surge_fail_open_total": {"Bookings priced at 1.0 because the surge multiplier could not be read from Redis.", nil},
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, f := range families {
		w, ok := want[f.GetName()]
		if !ok {
			continue
		}
		seen++
		if f.GetHelp() != w.help {
			t.Errorf("%s: help = %q, want %q", f.GetName(), f.GetHelp(), w.help)
		}
		if f.GetType().String() != "COUNTER" {
			t.Errorf("%s: type %s, want COUNTER", f.GetName(), f.GetType())
		}
		var got []string
		for _, l := range f.GetMetric()[0].GetLabel() {
			got = append(got, l.GetName())
		}
		if len(got) != len(w.labels) || (len(got) == 1 && got[0] != w.labels[0]) {
			t.Errorf("%s: labels %v, want %v", f.GetName(), got, w.labels)
		}
		if v := f.GetMetric()[0].GetCounter().GetValue(); v != 1 {
			t.Errorf("%s: value %v, want 1", f.GetName(), v)
		}
	}
	if seen != len(want) {
		t.Errorf("found %d of %d counters in the default registry", seen, len(want))
	}
}
