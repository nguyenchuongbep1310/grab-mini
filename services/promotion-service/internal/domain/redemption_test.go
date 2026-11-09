package domain

import "testing"

func TestDecideTable(t *testing.T) {
	p := Promotion{PerRider: 2, UsageLimit: ptr[int64](10)}
	cases := []struct {
		name      string
		u         Usage
		cancelled bool
		want      Redemption
	}{
		{"under limits", Usage{Total: 3, ByRider: 1}, false, Redemption{true, StatusRedeemed, StatusRedeemed, ""}},
		{"global limit named before per-rider", Usage{Total: 10, ByRider: 2}, false, Redemption{true, StatusOverLimit, StatusOverLimit, ReasonUsageLimit}},
		{"per-rider limit", Usage{Total: 3, ByRider: 2}, false, Redemption{true, StatusOverLimit, StatusOverLimit, ReasonPerRiderLimit}},
		{"cancelled ignores usage", Usage{Total: 10, ByRider: 2}, true, Redemption{false, StatusRedeemed, StatusReleased, ""}},
		{"cancelled under limits", Usage{}, true, Redemption{false, StatusRedeemed, StatusReleased, ""}},
	}
	for _, tc := range cases {
		if got := Decide(p, tc.u, tc.cancelled); got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

func TestValidateRedemptionMessages(t *testing.T) {
	cases := []struct {
		order, rider string
		discount     int64
		want         string
	}{
		{"o", "r", 0, ""},
		{"o", "r", MaxAmount, ""},
		{"", "r", 0, "invalid promotion event: order.created without orderId or riderId"},
		{"o", "", 0, "invalid promotion event: order.created without orderId or riderId"},
		{"", "", -1, "invalid promotion event: order.created without orderId or riderId"},
		{"o", "r", -1, "invalid promotion event: discount -1 is not an amount of VND"},
		{"o", "r", MaxAmount + 1, "invalid promotion event: discount 1000000000001 is not an amount of VND"},
	}
	for _, tc := range cases {
		err := ValidateRedemption(tc.order, tc.rider, tc.discount)
		if tc.want == "" {
			if err != nil {
				t.Errorf("%+v: %v", tc, err)
			}
			continue
		}
		if err == nil || err.Error() != tc.want {
			t.Errorf("%+v: err = %v, want %q", tc, err, tc.want)
		}
		if _, ok := err.(*InvalidEvent); !ok {
			t.Errorf("%+v: %T, want *InvalidEvent", tc, err)
		}
	}
}
