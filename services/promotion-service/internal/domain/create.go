package domain

import (
	"strings"
	"time"
)

// PromotionSpec is what an admin asks for when creating a promotion; nil
// ValidFrom and PerRider take their defaults.
type PromotionSpec struct {
	Code        string
	Kind        string
	Value       int64
	MaxDiscount *int64
	MinFare     int64
	VehicleType *string
	ValidFrom   *time.Time
	ValidTo     time.Time
	UsageLimit  *int64
	PerRider    *int64
}

// NewPromotion returns the promotion the spec describes, or an
// *InvalidPromotionError saying why it cannot be one. ValidFrom defaults to now
// and PerRider to 1; the code is normalised and the kind upper-cased.
func NewPromotion(spec PromotionSpec, now time.Time) (Promotion, error) {
	p := Promotion{
		Code: NormalizeCode(spec.Code), Kind: strings.ToUpper(strings.TrimSpace(spec.Kind)), Value: spec.Value,
		MaxDiscount: spec.MaxDiscount, MinFare: spec.MinFare, VehicleType: spec.VehicleType,
		ValidFrom: now, ValidTo: spec.ValidTo, UsageLimit: spec.UsageLimit, PerRider: 1,
	}
	if spec.ValidFrom != nil {
		p.ValidFrom = *spec.ValidFrom
	}
	if spec.PerRider != nil {
		p.PerRider = *spec.PerRider
	}
	why := ""
	switch {
	case !ValidCode(p.Code):
		why = "code must be 3 to 32 characters: letters, digits, '-' or '_'"
	case p.Kind != KindPercent && p.Kind != KindFixed:
		why = "kind must be PERCENT or FIXED"
	case p.Value <= 0 || p.Value > MaxAmount:
		why = "value must be above 0"
	case p.Kind == KindPercent && p.Value > 100:
		why = "a PERCENT value is a percentage from 1 to 100"
	case p.MaxDiscount != nil && p.Kind != KindPercent:
		why = "maxDiscount applies to PERCENT promotions only"
	case p.MaxDiscount != nil && (*p.MaxDiscount <= 0 || *p.MaxDiscount > MaxAmount):
		why = "maxDiscount must be above 0"
	case p.MinFare < 0 || p.MinFare > MaxAmount:
		why = "minFare must be 0 or more"
	case p.VehicleType != nil && !ValidVehicle(*p.VehicleType):
		why = "vehicleType must be CAR or BIKE (or omitted for any)"
	case p.ValidTo.IsZero():
		why = "validTo is required"
	case !p.ValidTo.After(p.ValidFrom):
		why = "validTo must be after validFrom"
	case !p.ValidTo.After(now):
		why = "validTo is in the past"
	case p.UsageLimit != nil && (*p.UsageLimit <= 0 || *p.UsageLimit > MaxAmount):
		why = "usageLimit must be above 0 (or omitted for no limit)"
	case p.PerRider <= 0 || p.PerRider > MaxAmount:
		why = "perRiderLimit must be 1 or more"
	}
	if why != "" {
		return Promotion{}, &InvalidPromotionError{Message: why}
	}
	return p, nil
}
