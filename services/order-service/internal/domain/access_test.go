package domain

import "testing"

func TestOnlyTheRiderAndTheMatchedDriverMayReadAnOrder(t *testing.T) {
	matched := Order{RiderID: "rider-1", DriverID: "driver-1"}
	pending := Order{RiderID: "rider-1"}
	cases := []struct {
		name string
		who  Actor
		o    Order
		want bool
	}{
		{"the rider", Actor{"rider-1", RoleRider}, matched, true},
		{"the rider before a match", Actor{"rider-1", RoleRider}, pending, true},
		{"another rider", Actor{"rider-2", RoleRider}, matched, false},
		{"the matched driver", Actor{"driver-1", RoleDriver}, matched, true},
		{"another driver", Actor{"driver-2", RoleDriver}, matched, false},
		{"any driver before a match", Actor{"driver-1", RoleDriver}, pending, false},
		{"a driver whose id equals the rider's", Actor{"rider-1", RoleDriver}, matched, false},
		{"a rider whose id equals the driver's", Actor{"driver-1", RoleRider}, matched, false},
		{"an admin", Actor{"rider-1", "ADMIN"}, matched, false},
		{"an unknown role", Actor{"rider-1", "superuser"}, matched, false},
		{"an empty driver id and subject", Actor{"", RoleDriver}, pending, false},
	}
	for _, c := range cases {
		if got := c.o.MayBeReadBy(c.who); got != c.want {
			t.Errorf("%s: MayBeReadBy = %v, want %v", c.name, got, c.want)
		}
	}
}
