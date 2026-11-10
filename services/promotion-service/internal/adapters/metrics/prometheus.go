// Package metrics holds the Prometheus counters the promotion use cases move.
// It is the only package that registers them.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	validations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "promotion_validations_total",
		Help: "GET /promotions/validate answers, by result: ok or the rejection code (not_found, expired, not_started, min_fare, wrong_vehicle, limit_reached, already_used).",
	}, []string{"result"})

	redemptions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "promotion_redemptions_total",
		Help: "Redemptions recorded from order.created, by status (REDEEMED, OVER_LIMIT) and, for OVER_LIMIT, the limit reached (usage_limit, per_rider_limit).",
	}, []string{"status", "reason"})

	redemptionsReleased = promauto.NewCounter(prometheus.CounterOpts{
		Name: "promotion_redemptions_released_total",
		Help: "Redemptions given back because the order ended without a ride (order.cancelled, a cancelled trip, order.no_driver_found).",
	})

	redemptionsDuplicate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "promotion_redemptions_duplicate_total",
		Help: "Events for a redemption or release that was already applied (redelivery or replay): nothing changed.",
	})
)

// Metrics implements app.Metrics over the process-wide counters.
type Metrics struct{}

// New returns the Metrics.
func New() *Metrics { return &Metrics{} }

func (*Metrics) Validation(result string)         { validations.WithLabelValues(result).Inc() }
func (*Metrics) Redemption(status, reason string) { redemptions.WithLabelValues(status, reason).Inc() }
func (*Metrics) Released(n int64)                 { redemptionsReleased.Add(float64(n)) }
func (*Metrics) Duplicate()                       { redemptionsDuplicate.Inc() }
