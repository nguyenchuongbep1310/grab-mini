package kafka

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"grab-mini/pkg/events"
	"grab-mini/profile-service/internal/domain"
)

// The domain owns its copies of a few wire values (it may not import
// services/pkg/events). This test keeps the copies equal to the contract.
func TestTheWireConstantsMatchPkgEvents(t *testing.T) {
	for _, c := range []struct{ name, got, want string }{
		{"car", string(domain.VehicleCar), string(events.VehicleCar)},
		{"bike", string(domain.VehicleBike), string(events.VehicleBike)},
		{"approved", domain.StatusApproved, events.ReviewApproved},
		{"rejected", domain.StatusRejected, events.ReviewRejected},
		{"suspended", domain.StatusSuspended, events.ReviewSuspended},
		{"topic", "driver.application.reviewed", events.TopicDriverApplicationReviewed},
	} {
		if c.got != c.want {
			t.Errorf("%s: %q here, %q on the wire", c.name, c.got, c.want)
		}
	}
	for _, v := range []domain.VehicleType{domain.VehicleCar, domain.VehicleBike} {
		if !v.Valid() || !events.VehicleType(v).Valid() {
			t.Errorf("vehicle %q is not valid on both sides", v)
		}
	}
}

// The CHECK constraints of the migration list the same values the domain names.
func TestTheMigrationChecksMatchTheDomain(t *testing.T) {
	b, err := os.ReadFile("../../../migrations/00001_create_profiles.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(b)
	vehicles := regexp.MustCompile(`vehicle_type IN \(([^)]*)\)`).FindStringSubmatch(sql)
	statuses := regexp.MustCompile(`status IN \(([^)]*)\)`).FindStringSubmatch(sql)
	if vehicles == nil || statuses == nil {
		t.Fatal("CHECK lists not found")
	}
	if want := `'', '` + string(domain.VehicleCar) + `', '` + string(domain.VehicleBike) + `'`; vehicles[1] != want {
		t.Errorf("vehicle_type CHECK %s, want %s", vehicles[1], want)
	}
	var want []string
	for _, s := range []string{domain.StatusNone, domain.StatusPending, domain.StatusApproved, domain.StatusRejected, domain.StatusSuspended} {
		want = append(want, "'"+s+"'")
	}
	if statuses[1] != strings.Join(want, ", ") {
		t.Errorf("status CHECK %s, want %s", statuses[1], strings.Join(want, ", "))
	}
}
