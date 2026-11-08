package app

import (
	"context"
	"errors"
	"log"

	"github.com/google/uuid"

	"grab-mini/order-service/internal/domain"
)

// CancelOrder lets a rider cancel an order that has no driver yet. reason has
// been checked (domain.NormalizeCancelReason).
//
//   - PENDING moves to CANCELLED and order.cancelled is queued in the same
//     transaction, so Matching stops looking and any trip that raced in is cancelled.
//   - Already CANCELLED is a no-op success (a retried tap), with no second event.
//   - MATCHED is a conflict: a driver is on the way, so the rider cancels the
//     trip (POST /trips/{id}/cancel) instead and Trip Service tells us.
//   - NO_DRIVER is a conflict: there is nothing left to cancel.
//
// A malformed id, a missing order and another rider's order are all
// domain.ErrOrderNotFound: to anyone but its rider the order doesn't exist.
func (s *Service) CancelOrder(ctx context.Context, actor domain.Actor, orderID, reason string) (CancelResult, error) {
	// orders.id is a UUID column: a malformed id would otherwise be a database error.
	if _, err := uuid.Parse(orderID); err != nil {
		return CancelResult{}, domain.ErrOrderNotFound
	}
	o, err := s.orders.ByID(ctx, orderID)
	if err == nil && o.RiderID != actor.Subject {
		err = domain.ErrOrderNotFound
	}
	if errors.Is(err, domain.ErrOrderNotFound) {
		return CancelResult{}, err
	}
	if err != nil {
		log.Printf("failed to load order: %v", err)
		return CancelResult{}, ErrInternal
	}

	if o.Status == domain.StatusPending {
		// Encode before touching the database, like order.created.
		payload, err := s.enc.OrderCancelled(ctx, OrderCancelled{
			OrderID: o.ID, RiderID: o.RiderID, CancelledBy: domain.CancelledByRider, Reason: reason,
		})
		if err != nil {
			log.Printf("failed to encode order.cancelled: %v", err)
			return CancelResult{}, ErrInternal
		}
		// The status check lives in CancelPending, so against a concurrent
		// order.matched exactly one of the two wins; the loser queues nothing.
		cancelled := false
		err = s.uow.Do(ctx, func(tx Tx) error {
			ok, err := tx.CancelPending(ctx, o.ID, reason)
			if err != nil || !ok {
				return err
			}
			if err := tx.QueueOrderCancelled(ctx, o.ID, payload); err != nil {
				return err
			}
			cancelled = true
			return nil
		})
		if err != nil {
			log.Printf("failed to cancel order: %v", err)
			return CancelResult{}, ErrInternal
		}
		if cancelled {
			s.relay.Wake()
			s.metrics.OrderCancelled()
		}
		// Not cancelled means we lost a race (matched, or cancelled by a
		// concurrent request); the status read below says which.
		if o, err = s.orders.ByID(ctx, orderID); err != nil {
			log.Printf("failed to reload order: %v", err)
			return CancelResult{}, ErrInternal
		}
	}

	switch o.Status {
	case domain.StatusCancelled:
		return CancelResult{Order: o}, nil
	case domain.StatusMatched:
		return CancelResult{Order: o, Conflict: true, Message: "a driver is already assigned: cancel the trip instead (POST /trips/{id}/cancel)"}, nil
	default:
		return CancelResult{Order: o, Conflict: true, Message: "this order can no longer be cancelled"}, nil
	}
}
