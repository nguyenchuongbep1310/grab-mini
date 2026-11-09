package domain

import "testing"

func TestDiscountMath(t *testing.T) {
	percent := func(value int64, cap *int64) Promotion {
		return Promotion{Kind: KindPercent, Value: value, MaxDiscount: cap}
	}
	fixed := func(value int64) Promotion { return Promotion{Kind: KindFixed, Value: value} }

	cases := []struct {
		name  string
		p     Promotion
		price int64
		want  int64
	}{
		{"10 percent", percent(10, nil), 87000, 8700},
		{"percent rounds down to whole dong", percent(10, nil), 12345, 1234},
		{"percent of a tiny price rounds to zero", percent(10, nil), 9, 0},
		{"percent capped by max_discount", percent(10, ptr[int64](20000)), 500000, 20000},
		{"percent under the cap is not raised to it", percent(10, ptr[int64](20000)), 100000, 10000},
		{"cap equal to the raw discount", percent(10, ptr[int64](10000)), 100000, 10000},
		{"100 percent is the whole price", percent(100, nil), 87000, 87000},
		{"100 percent with a cap", percent(100, ptr[int64](30000)), 87000, 30000},
		{"fixed", fixed(15000), 87000, 15000},
		{"fixed never more than the price", fixed(15000), 12000, 12000},
		{"fixed equal to the price is a free ride", fixed(15000), 15000, 15000},
		{"a cap above the price still cannot exceed it", percent(100, ptr[int64](1_000_000)), 50000, 50000},
		{"zero price", fixed(15000), 0, 0},
		{"negative price is never a negative discount", fixed(15000), -5, 0},
		{"unknown kind gives nothing", Promotion{Kind: "BOGO", Value: 5}, 87000, 0},
		{"a corrupt percentage above 100 is clamped", percent(250, nil), 10000, 10000},
		{"a negative value gives nothing", fixed(-5), 10000, 0},
		{"largest price does not overflow", percent(100, nil), MaxAmount, MaxAmount},
	}
	for _, tc := range cases {
		got := tc.p.Discount(tc.price)
		if got != tc.want {
			t.Errorf("%s: discount(%d) = %d, want %d", tc.name, tc.price, got, tc.want)
		}
		if got < 0 || got > max(tc.price, 0) {
			t.Errorf("%s: discount %d outside [0, price %d]", tc.name, got, tc.price)
		}
	}
}

// Whatever the promotion and the price, the discount is whole, non-negative,
// and leaves a final price of 0 or more.
func TestDiscountInvariantsOverARangeOfInputs(t *testing.T) {
	promos := []Promotion{
		{Kind: KindPercent, Value: 1}, {Kind: KindPercent, Value: 33}, {Kind: KindPercent, Value: 100},
		{Kind: KindPercent, Value: 50, MaxDiscount: ptr[int64](7777)},
		{Kind: KindFixed, Value: 1}, {Kind: KindFixed, Value: 15000}, {Kind: KindFixed, Value: MaxAmount},
	}
	for _, p := range promos {
		for price := int64(0); price <= 400000; price += 977 {
			d := p.Discount(price)
			if d < 0 || price-d < 0 {
				t.Fatalf("%+v price %d: discount %d leaves %d", p, price, d, price-d)
			}
		}
	}
}

func ptr[T any](v T) *T { return &v }
