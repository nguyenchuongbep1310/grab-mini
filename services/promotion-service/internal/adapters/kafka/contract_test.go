package kafka

import (
	"os"
	"regexp"
	"testing"

	"grab-mini/pkg/events"
	"grab-mini/promotion-service/internal/domain"
)

// The domain owns its copies of a few wire values (it may not import
// services/pkg/events). This test keeps the copies equal to the contract.
func TestTheWireConstantsMatchPkgEvents(t *testing.T) {
	for _, c := range []struct{ name, got, want string }{
		{"redeemed", domain.StatusRedeemed, events.PromotionStatusRedeemed},
		{"over limit", domain.StatusOverLimit, events.PromotionStatusOverLimit},
		{"trip cancelled", domain.TripStatusCancelled, string(events.TripStatusCancelled)},
		{"car", domain.VehicleCar, string(events.VehicleCar)},
		{"bike", "BIKE", string(events.VehicleBike)},
	} {
		if c.got != c.want {
			t.Errorf("%s: %q here, %q on the wire", c.name, c.got, c.want)
		}
	}
	// VND is sent as the currency of promotion.redeemed and the validate answer.
	if domain.CurrencyVND != "VND" {
		t.Errorf("currency %q", domain.CurrencyVND)
	}
	for _, v := range []string{domain.VehicleCar, "BIKE"} {
		if !events.VehicleType(v).Valid() || !domain.ValidVehicle(v) {
			t.Errorf("vehicle %q is not valid on both sides", v)
		}
	}
}

func TestTheConsumedTopicsAndGroupAreUnchanged(t *testing.T) {
	want := []string{events.TopicOrderCreated, events.TopicOrderCancelled, events.TopicTripStatusChanged, events.TopicOrderNoDriverFound}
	if len(ConsumedTopics) != len(want) {
		t.Fatalf("topics %v", ConsumedTopics)
	}
	for i := range want {
		if ConsumedTopics[i] != want[i] {
			t.Errorf("topic %d: %q, want %q", i, ConsumedTopics[i], want[i])
		}
	}
	// deploy/k8s/check.py reads the group with this regexp over the .go files.
	src, err := os.ReadFile("handler.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`GroupID:\s*"([^"]+)"`).FindSubmatch(src)
	if m == nil || string(m[1]) != "promotion-service-group" {
		t.Errorf("handler.go must carry GroupID: %q as a literal, found %q", "promotion-service-group", m)
	}
	for _, label := range []string{`LogLabel:   "[promotion-service]"`, `RetryTiers: kafkaconsumer.DefaultRetryTiers`} {
		if !regexp.MustCompile(regexp.QuoteMeta(label)).Match(src) {
			t.Errorf("handler.go lost %s", label)
		}
	}
}
