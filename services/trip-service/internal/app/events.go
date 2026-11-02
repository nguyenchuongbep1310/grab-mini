package app

import (
	"context"
	"errors"
	"log"

	"github.com/google/uuid"

	"grab-mini/trip-service/internal/domain"
)

// OrderMatched creates the trip for a matched order and confirms the driver as
// ON_TRIP, which ends the lease Matching put on them (without it the driver
// would become claimable again after a couple of minutes).
//
// It can be retried or redelivered at any point and converges:
//   - an order that already has a trip is left alone, so a replay never
//     flips a finished trip's driver back to ON_TRIP;
//   - the driver is confirmed before the trip is saved, so a failure there
//     leaves nothing recorded and the retry tries again;
//   - the eventId dedupe row and the trip are saved in one transaction, so
//     "seen" never outlives a trip that failed to save.
func (s *Service) OrderMatched(ctx context.Context, evt OrderMatched) error {
	if _, err := s.trips.ByOrderID(ctx, evt.OrderID); err == nil {
		log.Printf("trip already exists for order=%s, skipping order.matched event %s", evt.OrderID, evt.EventID)
		return nil
	} else if !errors.Is(err, ErrTripNotFound) {
		return err
	}

	// The rider cancelled the order before we heard of the match: no trip, and
	// the driver Matching claimed goes back to the pool.
	if cancelled, err := s.orders.IsCancelled(ctx, evt.OrderID); err != nil {
		return err
	} else if cancelled {
		return s.releaseCancelledMatch(ctx, evt, domain.DriverAssigned)
	}

	if err := s.drivers.Set(ctx, evt.DriverID, domain.DriverOnTrip); err != nil {
		return err
	}

	var (
		cancelledNow bool
		created      bool
		tripID       string
	)
	err := s.uow.Do(ctx, func(tx Tx) error {
		// Same lock as OrderCancelled: check again now that a cancellation
		// can no longer slip in between this check and the trip being saved.
		if err := tx.LockOrder(ctx, evt.OrderID); err != nil {
			return err
		}
		if cancelled, err := tx.IsOrderCancelled(ctx, evt.OrderID); err != nil {
			return err
		} else if cancelled {
			cancelledNow = true
			return errRollback
		}

		firstTime, err := tx.MarkEventProcessed(ctx, evt.EventID)
		if err != nil {
			return err
		}
		if !firstTime {
			log.Printf("duplicate order.matched event %s, skipping", evt.EventID)
			return errRollback
		}

		tripID = uuid.NewString()
		created, err = tx.CreateTrip(ctx, domain.Trip{
			ID: tripID, OrderID: evt.OrderID, DriverID: evt.DriverID, RiderID: evt.RiderID, Status: domain.Assigned,
		}, domain.Route{Pickup: evt.Pickup, Dropoff: evt.Dropoff})
		return err
	})
	if errors.Is(err, errRollback) {
		if cancelledNow {
			return s.releaseCancelledMatch(ctx, evt, domain.DriverOnTrip)
		}
		return nil
	}
	if err != nil {
		return err
	}

	if !created {
		log.Printf("trip already exists for order=%s, skipping (order_id safety net)", evt.OrderID)
		return nil
	}
	log.Printf("created trip=%s for order=%s driver=%s", tripID, evt.OrderID, evt.DriverID)
	return nil
}

// releaseCancelledMatch hands back a driver whose order was cancelled before
// the trip existed. The change is a compare-and-set from the status we left
// them in, so a driver who has since moved on is never touched.
func (s *Service) releaseCancelledMatch(ctx context.Context, evt OrderMatched, from domain.DriverStatus) error {
	released, err := s.drivers.Transition(ctx, evt.DriverID, from, domain.DriverAvailable)
	if err != nil {
		return err
	}
	log.Printf("order=%s was cancelled before its trip existed: no trip created, driver=%s released=%v", evt.OrderID, evt.DriverID, released)
	return nil
}

// OrderCancelled remembers that the order is cancelled and cancels the trip
// already created for it, if any (for the case where a driver was matched just
// before the rider tapped cancel). It converges when redelivered:
//   - a trip that is already CANCELLED (or COMPLETED) is left alone, and no
//     second trip.status.changed is queued;
//   - no trip yet: the order is remembered, so that when order.matched
//     arrives late OrderMatched doesn't create one.
//
// Both sides take the same per-order advisory lock, so "the trip is created"
// and "the order is cancelled" can't pass each other unnoticed.
func (s *Service) OrderCancelled(ctx context.Context, evt OrderCancelled) error {
	t, err := s.trips.ByOrderID(ctx, evt.OrderID)
	has := true
	if errors.Is(err, ErrTripNotFound) {
		has = false
	} else if err != nil {
		return err
	}

	info := domain.OrderCancelledInfo(evt.CancelledBy, evt.Reason)

	// Prepared outside the transaction (it may call the schema registry).
	var payload []byte
	if has && domain.Cancel.CanStartFrom(t.Status) {
		if payload, err = s.enc.EncodeStatusChanged(ctx, StatusChanged{
			TripID: t.ID, OrderID: t.OrderID, DriverID: t.DriverID, RiderID: t.RiderID,
			Status: domain.Cancelled, CancelledBy: info.By, Reason: info.Reason,
		}); err != nil {
			return err
		}
	}

	var (
		appeared bool
		moved    bool
		tripID   string
	)
	err = s.uow.Do(ctx, func(tx Tx) error {
		if err := tx.LockOrder(ctx, evt.OrderID); err != nil {
			return err
		}
		if err := tx.MarkOrderCancelled(ctx, evt.OrderID); err != nil {
			return err
		}

		id, found, err := tx.TripIDForOrder(ctx, evt.OrderID)
		if err != nil {
			return err
		}
		if !found {
			return nil // nothing to cancel; the order is remembered
		}
		if !has || t.ID != id {
			// Remember the order, then try again with the trip in hand.
			appeared = true
			return nil
		}
		if payload == nil {
			return nil // already CANCELLED or COMPLETED: nothing to do
		}

		tripID = id
		moved, err = tx.Transition(ctx, id, domain.Cancel, info)
		if err != nil {
			return err
		}
		if moved {
			return tx.EnqueueStatusChanged(ctx, id, payload)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if appeared {
		return errTripAppeared
	}
	if moved {
		s.metrics.Transitioned(string(domain.Cancelled))
		log.Printf("trip=%s cancelled because order=%s was cancelled by the %s", tripID, evt.OrderID, info.By)
	}
	return nil
}

// ReleaseDriver sets the driver of a COMPLETED or CANCELLED trip back to
// AVAILABLE, unless that would be wrong. handleComplete and handleCancel
// already do it in the request, but that step sits outside the transaction
// that commits the trip: a crash between the two used to leave the driver
// ON_TRIP forever. The trip.status.changed event is queued in the same
// transaction as the status, so it always arrives, and this is the step that
// cannot be lost.
//
// Events arrive late and again, so it checks the present rather than trusting
// the event:
//   - the driver has another trip in flight in the database: leave them;
//   - the driver isn't ON_TRIP in Redis (already released, or Matching has
//     since claimed them for a new order): leave them. The change is a
//     compare-and-set, so a claim that lands in between is never overwritten.
func (s *Service) ReleaseDriver(ctx context.Context, evt TripEnded) error {
	if !evt.Status.Ended() {
		return nil
	}

	if _, err := s.trips.ActiveByDriver(ctx, evt.DriverID); err == nil {
		return nil
	} else if !errors.Is(err, ErrTripNotFound) {
		return err
	}

	released, err := s.drivers.Transition(ctx, evt.DriverID, domain.DriverOnTrip, domain.DriverAvailable)
	if err != nil {
		return err
	}
	if released {
		log.Printf("driver=%s released after trip=%s %s", evt.DriverID, evt.TripID, evt.Status)
	}
	return nil
}
