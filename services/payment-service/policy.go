package main

import (
	"fmt"
	"time"

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
	// after a driver was assigned and the grace period has passed. 0 charges
	// nothing, which is how cancellation behaved before payments existed. It is
	// capped at the order's price, so a cancellation can never cost more than
	// the ride would have.
	CancellationFee int64
	// CancellationGrace is how long after the driver is assigned the rider can
	// still cancel for free. A cancel at exactly the grace limit is still free.
	CancellationGrace time.Duration
}

func (p Policy) Validate() error {
	if p.CommissionBps < 0 || p.CommissionBps > bpsDenominator {
		return fmt.Errorf("commission must be 0 to %d basis points, got %d", bpsDenominator, p.CommissionBps)
	}
	if err := checkAmount(p.CancellationFee); err != nil {
		return fmt.Errorf("cancellation fee: %w", err)
	}
	if p.CancellationGrace < 0 {
		return fmt.Errorf("cancellation grace must be 0 or more, got %s", p.CancellationGrace)
	}
	return nil
}

// Why an event produced no charge (the label of payments_skipped_total).
const (
	skipNoPrice      = "no_price"      // the order predates priced orders (price is null)
	skipZeroAmount   = "zero_amount"   // a free ride, or no tip
	skipFeeDisabled  = "fee_disabled"  // the rider cancelled but the cancellation fee is 0
	skipWithinGrace  = "within_grace"  // the rider cancelled soon enough after the assignment to pay nothing
	skipNoAssignment = "no_assignment" // the trip's ASSIGNED event was never seen, so the time held is unknown
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

// CancellationApplies says, from who cancelled and the fee setting alone,
// whether a cancellation can cost the rider anything. skip is the reason it
// cannot, or empty. It needs no order and no timing, so the common case (nothing
// to charge) never depends on either being known.
func (p Policy) CancellationApplies(cancelledBy string) (skip string) {
	switch cancelledBy {
	case events.CancelledByDriver:
		return skipDriverCancel
	case events.CancelledByRider:
	default:
		return skipUnknownActor
	}
	if p.CancellationFee == 0 {
		return skipFeeDisabled
	}
	return ""
}

// Cancellation is the charge for a trip that was CANCELLED. A trip exists only
// once a driver was assigned, so a rider cancelling a PENDING order (no driver
// yet) produces no trip event and never reaches here. assignedFor is how long the
// driver had been assigned when the trip was cancelled; nil means the assignment
// was never seen, which is not held against the rider. The driver cancelling is
// never the rider's cost.
func (p Policy) Cancellation(cancelledBy string, price *int64, assignedFor *time.Duration) (c charge, skip string) {
	if skip := p.CancellationApplies(cancelledBy); skip != "" {
		return charge{}, skip
	}
	if assignedFor == nil {
		return charge{}, skipNoAssignment
	}
	if *assignedFor <= p.CancellationGrace {
		return charge{}, skipWithinGrace
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
