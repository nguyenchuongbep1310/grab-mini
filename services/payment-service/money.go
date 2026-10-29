package main

import (
	"fmt"
	"math"
)

// Money here is always an int64 count of minor units of one currency. The only
// currency is VND, which has no minor unit (its exponent is 0), so an amount of
// 50000 is fifty thousand dong. There are no floats anywhere past the edge
// where an event's float64 price is turned into an integer (priceToMinor).
const (
	currencyVND = "VND"

	// maxAmount bounds any single amount (1 trillion dong). It is far above any
	// fare and far enough below the int64 limit that sums of amounts and the
	// commission multiplication below can never overflow.
	maxAmount int64 = 1_000_000_000_000

	// bpsDenominator: commission is expressed in basis points (1/100 of a percent).
	bpsDenominator int64 = 10_000
)

// amountError is a money value that can never be charged (negative, NaN, too
// large). It is permanent: retrying the same event cannot fix it.
type amountError struct{ reason string }

func (e *amountError) Error() string   { return "invalid amount: " + e.reason }
func (e *amountError) Permanent() bool { return true }

// priceToMinor turns the price carried by order.created (a float64 number of
// dong, already rounded to 1000 by Order Service) into minor units. It rounds
// to the nearest dong, so a value that arrives as 49999.9999999 (a float's
// noise) still becomes 50000.
func priceToMinor(price float64) (int64, error) {
	switch {
	case math.IsNaN(price) || math.IsInf(price, 0):
		return 0, &amountError{fmt.Sprintf("price %v is not a number", price)}
	case price < 0:
		return 0, &amountError{fmt.Sprintf("price %v is negative", price)}
	case price > float64(maxAmount):
		return 0, &amountError{fmt.Sprintf("price %v is above the limit", price)}
	}
	return int64(math.Round(price)), nil
}

// checkAmount validates an amount that is already an integer (a tip).
func checkAmount(amount int64) error {
	if amount < 0 {
		return &amountError{fmt.Sprintf("%d is negative", amount)}
	}
	if amount > maxAmount {
		return &amountError{fmt.Sprintf("%d is above the limit", amount)}
	}
	return nil
}

// split divides amount between the platform and the driver. The platform's
// commission is amount * bps / 10000 rounded down, and the driver gets the
// remainder, so platform + driver == amount exactly: no minor unit is ever
// created or lost by rounding. bps is clamped by the caller's config check to
// 0..10000.
func split(amount, bps int64) (driver, platform int64) {
	platform = amount * bps / bpsDenominator
	return amount - platform, platform
}
