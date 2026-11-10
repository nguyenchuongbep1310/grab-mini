package domain

import (
	"fmt"
	"time"
)

// Rejection codes: the machine-readable reason a code cannot be used. The HTTP
// status of each is the HTTP adapter's business, not the domain's.
const (
	CodeNotFound     = "not_found"
	CodeNotStarted   = "not_started"
	CodeExpired      = "expired"
	CodeMinFare      = "min_fare"
	CodeWrongVehicle = "wrong_vehicle"
	CodeLimitReached = "limit_reached"
	CodeAlreadyUsed  = "already_used"
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
		return &Rejection{CodeNotFound, "no such promotion code"}
	case now.Before(p.ValidFrom):
		return &Rejection{CodeNotStarted, "this code is not valid yet"}
	case !now.Before(p.ValidTo):
		return &Rejection{CodeExpired, "this code has expired"}
	case p.VehicleType != nil && *p.VehicleType != vehicle:
		return &Rejection{CodeWrongVehicle, fmt.Sprintf("this code is for %s trips only", *p.VehicleType)}
	case price < p.MinFare:
		return &Rejection{CodeMinFare, fmt.Sprintf("this code needs a fare of at least %d VND", p.MinFare)}
	case p.UsageLimit != nil && u.Total >= *p.UsageLimit:
		return &Rejection{CodeLimitReached, "this code has been fully redeemed"}
	case u.ByRider >= p.PerRider:
		return &Rejection{CodeAlreadyUsed, "you have already used this code"}
	}
	return nil
}

// Reasons an order was recorded past a limit.
const (
	ReasonUsageLimit    = "usage_limit"
	ReasonPerRiderLimit = "per_rider_limit"
)

// OverLimitReason is the limit a redemption would exceed given usage so far,
// or "" when it fits. The global limit is named first. Used when the order is
// already booked: it cannot be refused, only recorded for what it is.
func (p Promotion) OverLimitReason(u Usage) string {
	switch {
	case p.UsageLimit != nil && u.Total >= *p.UsageLimit:
		return ReasonUsageLimit
	case u.ByRider >= p.PerRider:
		return ReasonPerRiderLimit
	}
	return ""
}
