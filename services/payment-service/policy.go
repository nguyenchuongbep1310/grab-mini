package main

import (
	"fmt"

	"grab-mini/pkg/events"
)

// Policy holds the business rules that decide what a trip event costs. It is
// plain data and pure functions so every rule is unit-tested without a
// database.
type Policy struct {
	// CommissionBps is the platform's cut of a fare or cancellation fee in basis
	// points (2000 = 20%). Tips are never commissioned: they go to the driver in full.
	CommissionBps int64
	// CancellationFee is the fixed fee, in VND, charged to a rider who cancels
	// after a driver was assigned. 0 (the default) charges nothing, which is how
	// cancellation behaved before payments existed. It is capped at the order's
	// price, so a cancellation can never cost more than the ride would have.
	CancellationFee int64
}

func (p Policy) Validate() error {
	if p.CommissionBps < 0 || p.CommissionBps > bpsDenominator {
		return fmt.Errorf("commission must be 0 to %d basis points, got %d", bpsDenominator, p.CommissionBps)
	}
	if err := checkAmount(p.CancellationFee); err != nil {
		return fmt.Errorf("cancellation fee: %w", err)
	}
	return nil
}

// Why an event produced no charge (the label of payments_skipped_total).
const (
	skipNoPrice      = "no_price"      // the order predates priced orders (price is null)
	skipZeroAmount   = "zero_amount"   // a free ride, or no tip
	skipFeeDisabled  = "fee_disabled"  // the rider cancelled but the cancellation fee is 0
	skipDriverCancel = "driver_cancel" // the driver cancelled: the rider never pays
	skipUnknownActor = "unknown_actor" // cancelledBy is empty (an event from before it was recorded)
)

// charge is what to take from the rider and how it is shared.
type charge struct {
	Kind     string
	Total    int64
	Driver   int64
	Platform int64
}

func (p Policy) commissioned(kind string, total int64) charge {
	d, pl := split(total, p.CommissionBps)
	return charge{Kind: kind, Total: total, Driver: d, Platform: pl}
}

// Fare is the charge for a COMPLETED trip: exactly the price locked at
// booking. skip is the reason there is nothing to charge, or empty.
func (p Policy) Fare(price *int64) (c charge, skip string) {
	if price == nil {
		return charge{}, skipNoPrice
	}
	if *price == 0 {
		return charge{}, skipZeroAmount
	}
	return p.commissioned(events.PaymentKindFare, *price), ""
}

// Tip is the charge for a tip: all of it goes to the driver.
func (p Policy) Tip(amount int64) (c charge, skip string) {
	if amount == 0 {
		return charge{}, skipZeroAmount
	}
	return charge{Kind: events.PaymentKindTip, Total: amount, Driver: amount}, ""
}

// Cancellation is the charge for a trip that was CANCELLED. A trip exists only
// once a driver was assigned, so every cancelled trip is past the free window:
// a rider cancelling a PENDING order (no driver yet) produces no trip event
// and so never reaches here. The driver cancelling is never the rider's cost.
func (p Policy) Cancellation(cancelledBy string, price *int64) (c charge, skip string) {
	switch cancelledBy {
	case events.CancelledByDriver:
		return charge{}, skipDriverCancel
	case events.CancelledByRider:
	default:
		return charge{}, skipUnknownActor
	}
	if p.CancellationFee == 0 {
		return charge{}, skipFeeDisabled
	}
	fee := p.CancellationFee
	if price != nil && *price < fee {
		fee = *price
	}
	if fee == 0 {
		return charge{}, skipZeroAmount
	}
	return p.commissioned(events.PaymentKindCancellationFee, fee), ""
}
