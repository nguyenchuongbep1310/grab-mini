package main

import (
	"fmt"
	"time"
)

// Promotion kinds.
const (
	kindPercent = "PERCENT"
	kindFixed   = "FIXED"
)

// currencyVND: every amount here is a whole number of dong (VND has no minor unit).
const currencyVND = "VND"

// maxAmount bounds any single amount (1 trillion dong), far above any fare and far
// enough below the int64 limit that price * percent can never overflow.
const maxAmount int64 = 1_000_000_000_000

func ptr[T any](v T) *T { return &v }

// Promotion is a row of the promotions table.
type Promotion struct {
	Code        string
	Kind        string
	Value       int64
	MaxDiscount *int64 // PERCENT only; nil: no cap
	MinFare     int64
	VehicleType *string // nil: any vehicle
	ValidFrom   time.Time
	ValidTo     time.Time
	UsageLimit  *int64 // nil: unlimited
	PerRider    int64
	DisabledAt  *time.Time
}

// Discount is what the promotion takes off a trip priced at price, in whole VND.
//
//   - PERCENT: price * value / 100 rounded down (a rider is never given a
//     fraction of a dong more than the percentage), then capped at MaxDiscount.
//   - FIXED: Value.
//
// The result is never negative and never more than price, so the final price
// (price - discount) is never below 0. It does not look at eligibility; see Check.
func (p Promotion) Discount(price int64) int64 {
	if price <= 0 {
		return 0
	}
	var d int64
	switch p.Kind {
	case kindPercent:
		pct := min(max(p.Value, 0), 100)
		d = price * pct / 100
		if p.MaxDiscount != nil {
			d = min(d, *p.MaxDiscount)
		}
	case kindFixed:
		d = p.Value
	}
	return min(max(d, 0), price)
}

// Rejection codes: the machine-readable reason a code cannot be used, in the
// "code" field of a 4xx answer from GET /promotions/validate.
const (
	codeNotFound     = "not_found"
	codeNotStarted   = "not_started"
	codeExpired      = "expired"
	codeMinFare      = "min_fare"
	codeWrongVehicle = "wrong_vehicle"
	codeLimitReached = "limit_reached"
	codeAlreadyUsed  = "already_used"
)

// Rejection says why a promotion cannot be applied.
type Rejection struct {
	Code    string
	Message string
}

func (r *Rejection) Error() string { return r.Code + ": " + r.Message }

// Usage is how many redemptions count (status REDEEMED) against a promotion:
// by anyone, and by the rider asking.
type Usage struct {
	Total   int64
	ByRider int64
}

// Check says whether the promotion applies to a trip priced at price, for a
// vehicle, now, given its usage so far; nil means it does. A disabled promotion
// is reported as not found: to a rider it no longer exists.
func (p Promotion) Check(price int64, vehicle string, now time.Time, u Usage) *Rejection {
	switch {
	case p.DisabledAt != nil:
		return &Rejection{codeNotFound, "no such promotion code"}
	case now.Before(p.ValidFrom):
		return &Rejection{codeNotStarted, "this code is not valid yet"}
	case !now.Before(p.ValidTo):
		return &Rejection{codeExpired, "this code has expired"}
	case p.VehicleType != nil && *p.VehicleType != vehicle:
		return &Rejection{codeWrongVehicle, fmt.Sprintf("this code is for %s trips only", *p.VehicleType)}
	case price < p.MinFare:
		return &Rejection{codeMinFare, fmt.Sprintf("this code needs a fare of at least %d VND", p.MinFare)}
	case p.UsageLimit != nil && u.Total >= *p.UsageLimit:
		return &Rejection{codeLimitReached, "this code has been fully redeemed"}
	case u.ByRider >= p.PerRider:
		return &Rejection{codeAlreadyUsed, "you have already used this code"}
	}
	return nil
}

// overLimitReason is the limit a redemption would exceed given usage so far,
// or "" when it fits. The global limit is named first. Used when the order is
// already booked: it cannot be refused, only recorded for what it is.
func (p Promotion) overLimitReason(u Usage) string {
	switch {
	case p.UsageLimit != nil && u.Total >= *p.UsageLimit:
		return "usage_limit"
	case u.ByRider >= p.PerRider:
		return "per_rider_limit"
	}
	return ""
}
