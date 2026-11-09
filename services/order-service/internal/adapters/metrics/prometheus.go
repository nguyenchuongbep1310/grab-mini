// Package metrics counts what order-service does, as Prometheus counters.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The names, labels and help text are the ones dashboards and alerts use.

var ordersCreated = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "orders_created_total",
	Help: "Orders accepted (saved with their order.created event), by vehicle type.",
}, []string{"vehicle_type"})

var ordersCancelled = promauto.NewCounter(prometheus.CounterOpts{
	Name: "orders_cancelled_total",
	Help: "Orders cancelled by the rider while still PENDING (saved with their order.cancelled event).",
})

var idempotentRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "orders_idempotency_total",
	Help: "POST /orders carrying an Idempotency-Key, by result: new (first use), replayed (original response returned), mismatch (422, same key with another body).",
}, []string{"result"})

var bookingPromos = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "orders_promo_total",
	Help: "Promo tokens presented at booking, by result: applied, unchecked (Redis down: the single-use check was skipped, the discount was applied), used (409, already used for another booking), or the refusal (invalid, expired, wrong_rider, wrong_price, wrong_vehicle: 400).",
}, []string{"result"})

var bookingPricing = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "orders_pricing_total",
	Help: "How bookings were priced: quote (the rider's quote honoured), repriced (no or stale quote, current multiplier used), requote (409, multiplier moved too far), quote_reused (409, the quote already priced another order), quote_unchecked (Redis down: the single-use check was skipped, current multiplier used), invalid_quote (400).",
}, []string{"result"})

var surgeFailOpen = promauto.NewCounter(prometheus.CounterOpts{
	Name: "orders_surge_fail_open_total",
	Help: "Bookings priced at 1.0 because the surge multiplier could not be read from Redis.",
})

// Metrics implements app.Metrics over the package's counters.
type Metrics struct{}

// New returns the metrics; the counters are registered in the default registry.
func New() *Metrics { return &Metrics{} }

func (*Metrics) OrderCreated(vehicle string) { ordersCreated.WithLabelValues(vehicle).Inc() }
func (*Metrics) OrderCancelled()             { ordersCancelled.Inc() }
func (*Metrics) Pricing(result string)       { bookingPricing.WithLabelValues(result).Inc() }
func (*Metrics) SurgeFailOpen()              { surgeFailOpen.Inc() }
func (*Metrics) Promo(result string)         { bookingPromos.WithLabelValues(result).Inc() }
func (*Metrics) Idempotent(result string)    { idempotentRequests.WithLabelValues(result).Inc() }
