package kafka

import (
	"testing"

	"grab-mini/pkg/events"
	"grab-mini/trip-service/internal/domain"
)

// The domain owns its copies of a few wire values (it may not import
// services/pkg/events). This test is what keeps the copies equal to the
// published contract.
func TestDomainValuesMatchTheWireContract(t *testing.T) {
	for _, c := range []struct{ name, got, want string }{
		{"assigned", string(domain.Assigned), string(events.TripStatusAssigned)},
		{"in progress", string(domain.InProgress), string(events.TripStatusInProgress)},
		{"completed", string(domain.Completed), string(events.TripStatusCompleted)},
		{"cancelled", string(domain.Cancelled), string(events.TripStatusCancelled)},
		{"cancelled by rider", domain.CancelledByRider, events.CancelledByRider},
		{"cancelled by driver", domain.CancelledByDriver, events.CancelledByDriver},
	} {
		if c.got != c.want {
			t.Errorf("%s: %q here, %q on the wire", c.name, c.got, c.want)
		}
	}
}
