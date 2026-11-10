// Package metrics holds the Prometheus counters the profile use cases and the HTTP adapter
// move. It is the only package of the new layout that registers them.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	profileUpdates = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_updates_total",
		Help: "PUT /riders/{id} and PUT /drivers/{id} that were accepted, by kind (rider, driver).",
	}, []string{"kind"})

	applications = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_applications_total",
		Help: "POST /drivers/{id}/application, by result: submitted (now PENDING), replayed (already PENDING or APPROVED: nothing changed).",
	}, []string{"result"})

	reviews = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_reviews_total",
		Help: "driver.application.reviewed events queued, by decision: approved, rejected, suspended, vehicle_changed (an approved driver changed plate or vehicle type and is out of service until approved again).",
	}, []string{"decision"})

	requestsRefused = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_requests_rejected_total",
		Help: "Requests refused, by reason: invalid_request, invalid_profile, forbidden, not_found, incomplete_profile, suspended, invalid_transition, rate_limited, resync_running.",
	}, []string{"reason"})

	publicLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_public_lookups_total",
		Help: "GET /drivers/{id}/public, by result: found, not_found (no such driver, or not approved: the two look the same).",
	}, []string{"result"})

	resyncEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_resync_events_total",
		Help: "driver.application.reviewed events queued by a resync, by the status they repeat (APPROVED, REJECTED, SUSPENDED).",
	}, []string{"status"})

	resyncRuns = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_resync_runs_total",
		Help: "Resyncs (POST /driver-applications/resync or the resync command), by result: completed, failed, refused_running (another one was running).",
	}, []string{"result"})
)

// Metrics implements app.Metrics and the HTTP adapter's Refusals over the process-wide
// counters. It registers nothing itself, so New may be called any number of times.
type Metrics struct{}

// New returns the Metrics.
func New() *Metrics { return &Metrics{} }

func (*Metrics) Updated(kind string)        { profileUpdates.WithLabelValues(kind).Inc() }
func (*Metrics) Application(result string)  { applications.WithLabelValues(result).Inc() }
func (*Metrics) Review(decision string)     { reviews.WithLabelValues(decision).Inc() }
func (*Metrics) Refused(code string)        { requestsRefused.WithLabelValues(code).Inc() }
func (*Metrics) PublicLookup(result string) { publicLookups.WithLabelValues(result).Inc() }
func (*Metrics) ResyncRun(result string)    { resyncRuns.WithLabelValues(result).Inc() }
func (*Metrics) ResyncEvents(status string, n int) {
	resyncEvents.WithLabelValues(status).Add(float64(n))
}
