// Package domain holds the promotion rules: what a code is worth, whether it
// applies, how a new one is validated and how a redemption is recorded. It
// imports the standard library only and does no I/O.
package domain

import "time"

// Promotion kinds.
const (
	KindPercent = "PERCENT"
	KindFixed   = "FIXED"
)

// CurrencyVND: every amount here is a whole number of dong (VND has no minor unit).
const CurrencyVND = "VND"

// MaxAmount bounds any single amount (1 trillion dong), far above any fare and far
// enough below the int64 limit that price * percent can never overflow.
const MaxAmount int64 = 1_000_000_000_000

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

// Status says where a promotion is in its life as of now: disabled, scheduled,
// expired or active.
func Status(p Promotion, now time.Time) string {
	switch {
	case p.DisabledAt != nil:
		return "disabled"
	case now.Before(p.ValidFrom):
		return "scheduled"
	case !now.Before(p.ValidTo):
		return "expired"
	}
	return "active"
}

// Summary is a promotion as an admin sees it: with its usage and who made it.
// Redeemed counts redemptions that count (REDEEMED); OverLimit the orders
// recorded past a limit (OVER_LIMIT).
type Summary struct {
	Promotion
	Redeemed, OverLimit int64
	CreatedBy           string
	CreatedAt           time.Time
}
