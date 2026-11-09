package domain

import (
	"testing"
	"time"
)

func TestStatusAsOfNow(t *testing.T) {
	p := Promotion{ValidFrom: now0.Add(-time.Hour), ValidTo: now0.Add(time.Hour)}
	disabled := p
	disabled.DisabledAt = &now0
	expiredAndDisabled := disabled
	expiredAndDisabled.ValidTo = now0.Add(-time.Minute)
	cases := []struct {
		name string
		p    Promotion
		now  time.Time
		want string
	}{
		{"active", p, now0, "active"},
		{"at valid_from is active", p, p.ValidFrom, "active"},
		{"before valid_from", p, now0.Add(-2 * time.Hour), "scheduled"},
		{"at valid_to is expired", p, p.ValidTo, "expired"},
		{"after valid_to", p, now0.Add(2 * time.Hour), "expired"},
		{"disabled", disabled, now0, "disabled"},
		{"disabled outranks expired", expiredAndDisabled, now0, "disabled"},
	}
	for _, tc := range cases {
		if got := Status(tc.p, tc.now); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}
