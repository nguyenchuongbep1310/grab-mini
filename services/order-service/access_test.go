package main

import (
	"testing"

	"grab-mini/pkg/auth"
)

func TestOnlyTheRiderAndTheMatchedDriverMayReadAnOrder(t *testing.T) {
	matched := &orderView{RiderID: "rider-1", driverID: "driver-1"}
	pending := &orderView{RiderID: "rider-1"}
	cases := []struct {
		name string
		who  auth.Identity
		o    *orderView
		want bool
	}{
		{"the rider", auth.Identity{Subject: "rider-1", Role: auth.Rider}, matched, true},
		{"the rider before a match", auth.Identity{Subject: "rider-1", Role: auth.Rider}, pending, true},
		{"another rider", auth.Identity{Subject: "rider-2", Role: auth.Rider}, matched, false},
		{"the matched driver", auth.Identity{Subject: "driver-1", Role: auth.Driver}, matched, true},
		{"another driver", auth.Identity{Subject: "driver-2", Role: auth.Driver}, matched, false},
		{"any driver before a match", auth.Identity{Subject: "driver-1", Role: auth.Driver}, pending, false},
		{"a driver whose id equals the rider's", auth.Identity{Subject: "rider-1", Role: auth.Driver}, matched, false},
		{"a rider whose id equals the driver's", auth.Identity{Subject: "driver-1", Role: auth.Rider}, matched, false},
		{"an unknown role", auth.Identity{Subject: "rider-1", Role: "admin"}, matched, false},
	}
	for _, c := range cases {
		if got := mayReadOrder(c.who, c.o); got != c.want {
			t.Errorf("%s: mayReadOrder = %v, want %v", c.name, got, c.want)
		}
	}
}
