package domain

import (
	"errors"
	"testing"
	"time"
)

var now0 = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

func validSpec() PromotionSpec {
	return PromotionSpec{Code: "WELCOME10", Kind: KindPercent, Value: 10, ValidTo: now0.Add(time.Hour)}
}

func TestNewPromotionRefusesEachCaseWithItsMessageInOrder(t *testing.T) {
	cases := []struct {
		name string
		edit func(*PromotionSpec)
		want string
	}{
		{"bad code", func(s *PromotionSpec) { s.Code = "x" }, "code must be 3 to 32 characters: letters, digits, '-' or '_'"},
		{"bad kind", func(s *PromotionSpec) { s.Kind = "BOGO" }, "kind must be PERCENT or FIXED"},
		{"zero value", func(s *PromotionSpec) { s.Value = 0 }, "value must be above 0"},
		{"huge value", func(s *PromotionSpec) { s.Kind = KindFixed; s.Value = MaxAmount + 1 }, "value must be above 0"},
		{"percent over 100", func(s *PromotionSpec) { s.Value = 101 }, "a PERCENT value is a percentage from 1 to 100"},
		{"max discount on fixed", func(s *PromotionSpec) { s.Kind = KindFixed; s.MaxDiscount = ptr[int64](5) }, "maxDiscount applies to PERCENT promotions only"},
		{"max discount zero", func(s *PromotionSpec) { s.MaxDiscount = ptr[int64](0) }, "maxDiscount must be above 0"},
		{"max discount huge", func(s *PromotionSpec) { s.MaxDiscount = ptr(MaxAmount + 1) }, "maxDiscount must be above 0"},
		{"negative min fare", func(s *PromotionSpec) { s.MinFare = -1 }, "minFare must be 0 or more"},
		{"huge min fare", func(s *PromotionSpec) { s.MinFare = MaxAmount + 1 }, "minFare must be 0 or more"},
		{"bad vehicle", func(s *PromotionSpec) { s.VehicleType = ptr("TRUCK") }, "vehicleType must be CAR or BIKE (or omitted for any)"},
		{"no valid to", func(s *PromotionSpec) { s.ValidTo = time.Time{} }, "validTo is required"},
		{"valid to not after valid from", func(s *PromotionSpec) { s.ValidFrom = ptr(now0.Add(2 * time.Hour)) }, "validTo must be after validFrom"},
		{"valid to in the past", func(s *PromotionSpec) { s.ValidFrom = ptr(now0.Add(-3 * time.Hour)); s.ValidTo = now0.Add(-time.Hour) }, "validTo is in the past"},
		{"usage limit zero", func(s *PromotionSpec) { s.UsageLimit = ptr[int64](0) }, "usageLimit must be above 0 (or omitted for no limit)"},
		{"usage limit huge", func(s *PromotionSpec) { s.UsageLimit = ptr(MaxAmount + 1) }, "usageLimit must be above 0 (or omitted for no limit)"},
		{"per rider zero", func(s *PromotionSpec) { s.PerRider = ptr[int64](0) }, "perRiderLimit must be 1 or more"},
		{"per rider huge", func(s *PromotionSpec) { s.PerRider = ptr(MaxAmount + 1) }, "perRiderLimit must be 1 or more"},
		// Two faults at once: the earlier case in the switch wins.
		{"code before kind", func(s *PromotionSpec) { s.Code = "x"; s.Kind = "BOGO" }, "code must be 3 to 32 characters: letters, digits, '-' or '_'"},
		{"kind before value", func(s *PromotionSpec) { s.Kind = "BOGO"; s.Value = 0 }, "kind must be PERCENT or FIXED"},
		{"value before max discount", func(s *PromotionSpec) { s.Value = 0; s.MaxDiscount = ptr[int64](0) }, "value must be above 0"},
		{"percent range before max discount kind", func(s *PromotionSpec) { s.Value = 150; s.MaxDiscount = ptr[int64](0) }, "a PERCENT value is a percentage from 1 to 100"},
		{"min fare before vehicle", func(s *PromotionSpec) { s.MinFare = -1; s.VehicleType = ptr("TRUCK") }, "minFare must be 0 or more"},
		{"vehicle before valid to", func(s *PromotionSpec) { s.VehicleType = ptr("TRUCK"); s.ValidTo = time.Time{} }, "vehicleType must be CAR or BIKE (or omitted for any)"},
		{"after valid from before past", func(s *PromotionSpec) { s.ValidFrom = ptr(now0.Add(-time.Hour)); s.ValidTo = now0.Add(-2 * time.Hour) }, "validTo must be after validFrom"},
		{"past before usage limit", func(s *PromotionSpec) {
			s.ValidFrom = ptr(now0.Add(-3 * time.Hour))
			s.ValidTo = now0.Add(-time.Hour)
			s.UsageLimit = ptr[int64](0)
		}, "validTo is in the past"},
		{"usage limit before per rider", func(s *PromotionSpec) { s.UsageLimit = ptr[int64](0); s.PerRider = ptr[int64](0) }, "usageLimit must be above 0 (or omitted for no limit)"},
	}
	for _, tc := range cases {
		s := validSpec()
		tc.edit(&s)
		_, err := NewPromotion(s, now0)
		var ip *InvalidPromotionError
		if !errors.As(err, &ip) {
			t.Errorf("%s: err = %v, want *InvalidPromotionError", tc.name, err)
			continue
		}
		if ip.Message != tc.want || err.Error() != tc.want {
			t.Errorf("%s: message = %q, want %q", tc.name, ip.Message, tc.want)
		}
	}
}

func TestNewPromotionDefaultsValidFromPerRiderAndNormalises(t *testing.T) {
	s := validSpec()
	s.Code = "  welcome10 "
	s.Kind = " percent\t"
	p, err := NewPromotion(s, now0)
	if err != nil {
		t.Fatal(err)
	}
	if p.Code != "WELCOME10" || p.Kind != KindPercent {
		t.Errorf("code/kind = %q/%q", p.Code, p.Kind)
	}
	if !p.ValidFrom.Equal(now0) {
		t.Errorf("ValidFrom = %v, want now", p.ValidFrom)
	}
	if p.PerRider != 1 {
		t.Errorf("PerRider = %d, want 1", p.PerRider)
	}
	if p.Value != 10 || !p.ValidTo.Equal(s.ValidTo) || p.DisabledAt != nil {
		t.Errorf("unexpected promotion %+v", p)
	}

	from := now0.Add(10 * time.Minute)
	s.ValidFrom = &from
	s.PerRider = ptr[int64](3)
	p, err = NewPromotion(s, now0)
	if err != nil || !p.ValidFrom.Equal(from) || p.PerRider != 3 {
		t.Errorf("explicit values not kept: %+v, %v", p, err)
	}
}
