// Package metrics is the Prometheus implementation of app.Metrics. The metric
// names, labels and help text are part of the service's contract (dashboards,
// alerts and the runbook use them): do not rename them.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/trip-service/internal/app"
)

var (
	Transitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "trip_transitions_total",
		Help: "Trip status changes committed, by the status moved to.",
	}, []string{"status"})

	Ratings = promauto.NewCounter(prometheus.CounterOpts{
		Name: "trip_ratings_total",
		Help: "Ratings saved.",
	})

	ProximityRefusals = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "trip_proximity_refused_total",
		Help: "Pickup/complete requests refused by the driver proximity check, by step and reason (too_far, location_unknown, location_stale, location_unavailable).",
	}, []string{"step", "reason"})
)

// Prometheus implements app.Metrics on the counters above.
type Prometheus struct{}

var _ app.Metrics = Prometheus{}

func (Prometheus) Transitioned(status string) { Transitions.WithLabelValues(status).Inc() }
func (Prometheus) Rated()                     { Ratings.Inc() }
func (Prometheus) ProximityRefused(step, reason string) {
	ProximityRefusals.WithLabelValues(step, reason).Inc()
}
