package app

import (
	"context"
	"log"

	"grab-mini/promotion-service/internal/domain"
)

// OnOrderCancelled gives back the redemption of an order a rider cancelled
// while it was still PENDING (the only time order.cancelled is published).
func (s *Service) OnOrderCancelled(ctx context.Context, c ReleaseCommand) error {
	return s.releaseOrder(ctx, c.EventID, c.OrderID, "order.cancelled", "cancelled")
}

// OnTripStatusChanged gives back the redemption of an order whose trip was
// cancelled after the match. Every other trip status leaves the redemption
// alone, a COMPLETED trip being the one that used it.
func (s *Service) OnTripStatusChanged(ctx context.Context, c ReleaseCommand) error {
	if c.Status != domain.TripStatusCancelled {
		return nil
	}
	return s.releaseOrder(ctx, c.EventID, c.OrderID, "trip.status.changed", "trip cancelled")
}

// OnNoDriverFound gives back the redemption of an order that ended NO_DRIVER.
func (s *Service) OnNoDriverFound(ctx context.Context, c ReleaseCommand) error {
	return s.releaseOrder(ctx, c.EventID, c.OrderID, "order.no_driver_found", "found no driver")
}

// releaseOrder marks the order's REDEEMED and OVER_LIMIT redemptions RELEASED
// (stamped with the database clock), which stops them counting against the
// limits. It announces nothing. A RELEASED row is left as it was, so the
// release is idempotent across the several events that can name one order.
//
// When nothing was released, the order id is remembered, so that an
// order.created handled later records its redemption as released.
func (s *Service) releaseOrder(ctx context.Context, eventID, orderID, topic, what string) error {
	if orderID == "" {
		return invalidEvent(topic + " without orderId")
	}
	var released int64 // read after a successful commit
	err := s.d.UoW.Do(ctx, func(tx Tx) error {
		if err := checkEventID(eventID); err != nil {
			return err
		}
		first, err := tx.MarkEventProcessed(ctx, eventID)
		if err != nil {
			return err
		}
		if !first {
			s.d.Metrics.Duplicate()
			log.Printf("duplicate event %s, skipping", eventID)
			return nil
		}
		if err := tx.LockOrder(ctx, orderID); err != nil {
			return err
		}
		n, err := tx.ReleaseRedemptions(ctx, orderID)
		if err != nil {
			return err
		}
		if n == 0 {
			if err := tx.RememberCancelled(ctx, orderID); err != nil {
				return err
			}
		}
		released = n
		return nil
	})
	if err != nil {
		return err
	}
	if released > 0 {
		s.d.Metrics.Released(released)
		log.Printf("order %s %s (%s): %d redemption(s) released", orderID, what, topic, released)
	}
	return nil
}
