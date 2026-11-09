package domain

import "testing"

func TestNormalizeCodeAndShape(t *testing.T) {
	if got := NormalizeCode("  welcome10\t"); got != "WELCOME10" {
		t.Errorf("NormalizeCode = %q", got)
	}
	cases := map[string]bool{
		"ABC": true, "A-B_9": true, "AB": false, "": false, "lower": false, "HAS SPACE": false,
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ012345":  true,
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456": false,
		"BAD!":                              false,
	}
	for in, want := range cases {
		if got := ValidCode(in); got != want {
			t.Errorf("ValidCode(%q) = %v, want %v", in, got, want)
		}
	}
	for in, want := range map[string]bool{"CAR": true, "BIKE": true, "car": false, "": false, "TRUCK": false} {
		if got := ValidVehicle(in); got != want {
			t.Errorf("ValidVehicle(%q) = %v, want %v", in, got, want)
		}
	}
}
