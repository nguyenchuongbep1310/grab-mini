// Package metrics is the Prometheus implementation of app.Metrics. The metric
// names, labels and help text are part of the service's contract (dashboards,
// alerts and the runbook use them): do not rename them.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/payment-service/internal/app"
)

var (
	PaymentsRecorded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payments_recorded_total",
		Help: "Payments written to the ledger side (and announced), by kind (FARE, TIP, CANCELLATION_FEE) and status (CAPTURED, DECLINED).",
	}, []string{"kind", "status"})

	PaymentsCapturedAmount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payments_captured_amount_total",
		Help: "Sum of captured payments in VND (minor units), by kind.",
	}, []string{"kind"})

	PaymentsDuplicate = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payments_duplicate_total",
		Help: "Events for a charge that was already recorded (redelivery or replay): nothing was charged again.",
	}, []string{"kind"})

	PaymentsSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payments_skipped_total",
		Help: "Trip events that produced no charge, by reason (no_price, zero_amount, fee_disabled, driver_cancel, unknown_actor).",
	}, []string{"reason"})

	ProviderErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payment_provider_errors_total",
		Help: "Charges the payment provider could not answer (the event is retried; the idempotency key makes that safe).",
	})

	UnbalancedRejected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ledger_unbalanced_rejected_total",
		Help: "Ledger transactions refused because their entries did not sum to zero. Should always be 0: a non-zero value is a bug in the code that built the transaction.",
	})
)

// Prometheus implements app.Metrics on the counters above.
type Prometheus struct{}

var _ app.Metrics = Prometheus{}

func (Prometheus) Recorded(kind, status string) { PaymentsRecorded.WithLabelValues(kind, status).Inc() }
func (Prometheus) CapturedAmount(kind string, amount int64) {
	PaymentsCapturedAmount.WithLabelValues(kind).Add(float64(amount))
}
func (Prometheus) Duplicate(kind string) { PaymentsDuplicate.WithLabelValues(kind).Inc() }
func (Prometheus) Skipped(reason string) { PaymentsSkipped.WithLabelValues(reason).Inc() }
func (Prometheus) ProviderError()        { ProviderErrors.Inc() }
func (Prometheus) LedgerUnbalanced()     { UnbalancedRejected.Inc() }
