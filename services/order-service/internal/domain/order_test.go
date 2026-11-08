package domain

import "testing"

func TestFinalPriceNeverGoesBelowZeroAndIsNilWithoutAPrice(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	cases := []struct {
		name  string
		price *float64
		disc  int64
		want  *float64
	}{
		{"no price", nil, 5000, nil},
		{"no discount", f(30000), 0, f(30000)},
		{"a discount", f(30000), 10000, f(20000)},
		{"exactly the price", f(30000), 30000, f(0)},
		{"more than the price", f(30000), 50000, f(0)},
	}
	for _, c := range cases {
		got := Order{Price: c.price, Discount: c.disc}.FinalPrice()
		switch {
		case c.want == nil && got != nil:
			t.Errorf("%s: got %v, want nil", c.name, *got)
		case c.want != nil && (got == nil || *got != *c.want):
			t.Errorf("%s: got %v, want %v", c.name, got, *c.want)
		}
	}
}

func TestValidLatLngAndVehicleType(t *testing.T) {
	if ValidLatLng(nil) || ValidLatLng(&LatLng{91, 0}) || ValidLatLng(&LatLng{0, -181}) {
		t.Error("accepted an invalid point")
	}
	if !ValidLatLng(&LatLng{90, 180}) || !ValidLatLng(&LatLng{-90, -180}) {
		t.Error("refused a boundary point")
	}
	if !VehicleCar.Valid() || !VehicleBike.Valid() || VehicleType("car").Valid() || VehicleType("").Valid() {
		t.Error("vehicle validity wrong")
	}
}
