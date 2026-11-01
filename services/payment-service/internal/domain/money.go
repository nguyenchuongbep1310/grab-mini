// Package domain holds payment-service's business rules: money, the ledger, the fee and
// commission policy. It imports the standard library only (internal/arch_test.go
// enforces that).
package domain

import (
	"fmt"
	"math"
)

// Money here is always an int64 count of minor units of one currency. The only
// currency is VND, which has no minor unit (its exponent is 0), so an amount of
// 50000 is fifty thousand dong. There are no floats anywhere past the edge
// where an event's float64 price is turned into an integer (PriceToMinor).
const (
	CurrencyVND = "VND"

	// MaxAmount bounds any single amount (1 trillion dong). It is far above any
	// fare and far enough below the int64 limit that sums of amounts and the
	// commission multiplication below can never overflow.
	MaxAmount int64 = 1_000_000_000_000

	// BpsDenominator: commission is expressed in basis points (1/100 of a percent).
	BpsDenominator int64 = 10_000
)

// AmountError is a money value that can never be charged (negative, NaN, too
// large). It is permanent: retrying the same event cannot fix it.
type AmountError struct{ reason string }

func (e *AmountError) Error() string   { return "invalid amount: " + e.reason }
func (e *AmountError) Permanent() bool { return true }

// PriceToMinor turns the price carried by order.created (a float64 number of
// dong, already rounded to 1000 by Order Service) into minor units. It rounds
// to the nearest dong, so a value that arrives as 49999.9999999 (a float's
// noise) still becomes 50000.
func PriceToMinor(price float64) (int64, error) {
	switch {
	case math.IsNaN(price) || math.IsInf(price, 0):
		return 0, &AmountError{fmt.Sprintf("price %v is not a number", price)}
	case price < 0:
		return 0, &AmountError{fmt.Sprintf("price %v is negative", price)}
	case price > float64(MaxAmount):
		return 0, &AmountError{fmt.Sprintf("price %v is above the limit", price)}
	}
	return int64(math.Round(price)), nil
}

// CheckAmount validates an amount that is already an integer (a tip).
func CheckAmount(amount int64) error {
	if amount < 0 {
		return &AmountError{fmt.Sprintf("%d is negative", amount)}
	}
	if amount > MaxAmount {
		return &AmountError{fmt.Sprintf("%d is above the limit", amount)}
	}
	return nil
}

// CheckDiscount validates the discount carried by an order against its price:
// whole VND, not negative, and not more than the price (the rider is never paid to ride).
func CheckDiscount(discount, price int64) error {
	switch {
	case discount < 0:
		return &AmountError{fmt.Sprintf("discount %d is negative", discount)}
	case discount > price:
		return &AmountError{fmt.Sprintf("discount %d is above the price %d", discount, price)}
	}
	return nil
}

// Split divides amount between the platform and the driver. The platform's
// commission is amount * bps / 10000 rounded down, and the driver gets the
// remainder, so platform + driver == amount exactly: no minor unit is ever
// created or lost by rounding. bps is clamped by the caller's config check to
// 0..10000.
func Split(amount, bps int64) (driver, platform int64) {
	platform = amount * bps / BpsDenominator
	return amount - platform, platform
}
