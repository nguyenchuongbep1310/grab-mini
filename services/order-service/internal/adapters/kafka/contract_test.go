package kafka

import (
	"testing"

	"grab-mini/order-service/internal/app"
	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/events"
)

// The domain owns its copies of a few wire values (it may not import
// services/pkg/events). This test is what keeps the copies equal to the
// published contract.
func TestDomainValuesMatchTheWireContract(t *testing.T) {
	for _, c := range []struct{ name, got, want string }{
		{"pending", string(domain.StatusPending), string(events.OrderStatusPending)},
		{"matched", string(domain.StatusMatched), string(events.OrderStatusMatched)},
		{"no driver", string(domain.StatusNoDriver), string(events.OrderStatusNoDriver)},
		{"cancelled", string(domain.StatusCancelled), string(events.OrderStatusCancelled)},
		{"cancelled by rider", domain.CancelledByRider, events.CancelledByRider},
		{"car", string(domain.VehicleCar), string(events.VehicleCar)},
		{"bike", string(domain.VehicleBike), string(events.VehicleBike)},
	} {
		if c.got != c.want {
			t.Errorf("%s: %q here, %q on the wire", c.name, c.got, c.want)
		}
	}
}

var _ app.Encoder = (*Encoder)(nil)
