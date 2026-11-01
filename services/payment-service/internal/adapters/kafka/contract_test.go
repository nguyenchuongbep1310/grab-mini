package kafka

import (
	"testing"

	"grab-mini/payment-service/internal/app"
	"grab-mini/payment-service/internal/domain"
	"grab-mini/pkg/events"
)

// The domain and the app own their copies of a few wire values (they may not
// import services/pkg/events). This test is what keeps the copies equal to the
// published contract.
func TestDomainAndAppValuesMatchTheWireContract(t *testing.T) {
	for _, c := range []struct{ name, got, want string }{
		{"fare kind", domain.KindFare, events.PaymentKindFare},
		{"tip kind", domain.KindTip, events.PaymentKindTip},
		{"cancellation fee kind", domain.KindCancellationFee, events.PaymentKindCancellationFee},
		{"captured status", domain.StatusCaptured, events.PaymentStatusCaptured},
		{"declined status", domain.StatusDeclined, events.PaymentStatusDeclined},
		{"cancelled by rider", domain.CancelledByRider, events.CancelledByRider},
		{"cancelled by driver", domain.CancelledByDriver, events.CancelledByDriver},
		{"completed trip", app.TripCompleted, string(events.TripStatusCompleted)},
		{"cancelled trip", app.TripCancelled, string(events.TripStatusCancelled)},
	} {
		if c.got != c.want {
			t.Errorf("%s: %q here, %q on the wire", c.name, c.got, c.want)
		}
	}
}
