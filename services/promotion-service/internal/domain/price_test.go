package domain

import "testing"

func TestParsePriceRoundsToWholeDongAndRefusesTheRest(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr string
	}{
		{"87000", 87000, ""},
		{"87000.0", 87000, ""},
		{"87000.4", 87000, ""},
		{"87000.5", 87001, ""},
		{"1000000000000", MaxAmount, ""},
		{"", 0, "price is required"},
		{"abc", 0, "price must be a number of VND"},
		{"NaN", 0, "price must be a number of VND"},
		{"Inf", 0, "price must be a number of VND"},
		{"-Inf", 0, "price must be a number of VND"},
		{"0", 0, "price must be above 0 VND"},
		{"-5", 0, "price must be above 0 VND"},
		{"1000000000001", 0, "price must be above 0 VND"},
	}
	for _, tc := range cases {
		got, err := ParsePrice(tc.in)
		if tc.wantErr == "" {
			if err != nil || got != tc.want {
				t.Errorf("%q: got %d, %v; want %d", tc.in, got, err, tc.want)
			}
			continue
		}
		if err == nil || err.Error() != tc.wantErr {
			t.Errorf("%q: err = %v, want %q", tc.in, err, tc.wantErr)
		}
	}
}
