package app

import (
	"context"
	"fmt"
	"time"

	"grab-mini/payment-service/internal/domain"
)

// RecordOrder keeps the price locked at booking, and the discount a promo code took
// off it; what the rider is charged when the trip completes is price - discount.
// order.created is the source because it is the one event that carries them and the
// rider together; the table is a local copy so a trip event never needs a call to
// Order Service. A redelivery hits the primary key and changes nothing.
func (s *Service) RecordOrder(ctx context.Context, evt OrderCreated) error {
	if evt.OrderID == "" || evt.RiderID == "" {
		return &InvalidEventError{"order.created without orderId or riderId"}
	}
	var price *int64
	var discount int64
	if evt.Price != nil {
		p, err := domain.PriceToMinor(*evt.Price)
		if err != nil {
			return err
		}
		price = &p
		// Order Service never locks more than the price. If one arrives it is a bug
		// upstream; refuse it here (and page, through the dead-letter topic) rather than
		// guess between a free ride and the full fare.
		if err := domain.CheckDiscount(evt.Discount, p); err != nil {
			return err
		}
		discount = evt.Discount
	}
	return s.orders.SaveOrder(ctx, evt.OrderID, evt.RiderID, price, discount)
}

// RecordAssignment remembers when an order got its driver: the cancellation fee needs to
// know how long the driver had been committed. Trip Service publishes no event when it
// creates the trip, so this is the assignment moment (Matching Service stamps it, and the
// trip exists a moment later). The first order.matched wins; a redelivery changes nothing.
func (s *Service) RecordAssignment(ctx context.Context, evt OrderMatched) error {
	if evt.OrderID == "" {
		return &InvalidEventError{"order.matched without orderId"}
	}
	at, err := parseEventTime(evt.OccurredAt)
	if err != nil {
		return err
	}
	return s.assignments.SaveAssignment(ctx, evt.OrderID, at)
}

func (s *Service) lookupOrder(ctx context.Context, orderID string) (domain.OrderInfo, error) {
	o, found, err := s.orders.FindOrder(ctx, orderID)
	if err != nil {
		return domain.OrderInfo{}, err
	}
	if !found {
		return domain.OrderInfo{}, &OrderUnknownError{orderID}
	}
	return o, nil
}

// assignedFor is how long the order's driver had been assigned at cancelledAt, or nil when
// the order.matched event was never recorded (this service was not yet consuming when it was
// published, or is badly behind).
func (s *Service) assignedFor(ctx context.Context, orderID string, cancelledAt time.Time) (*time.Duration, error) {
	assignedAt, found, err := s.assignments.AssignedAt(ctx, orderID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	d := cancelledAt.Sub(assignedAt)
	return &d, nil
}

// parseEventTime reads an event's occurredAt. A missing or malformed time is a
// permanent error: no amount of retrying will make it parse.
func parseEventTime(v string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, &InvalidEventError{fmt.Sprintf("occurredAt %q is not an RFC 3339 time", v)}
	}
	return t, nil
}
