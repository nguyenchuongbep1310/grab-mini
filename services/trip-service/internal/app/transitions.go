package app

import (
	"context"
	"errors"
	"log"

	"grab-mini/trip-service/internal/domain"
)

// Pickup is US-4.1: confirm pickup moves a trip from ASSIGNED to IN_PROGRESS.
// Only the trip's driver may, and only near the pickup point.
func (s *Service) Pickup(ctx context.Context, who domain.Actor, tripID string) (domain.Trip, error) {
	return s.transition(ctx, who, tripID, domain.Pickup, domain.CancelInfo{})
}

// Complete is US-4.2: completing a trip moves IN_PROGRESS to COMPLETED and
// releases the driver back to AVAILABLE. Only the trip's driver may, and only
// near the dropoff.
func (s *Service) Complete(ctx context.Context, who domain.Actor, tripID string) (domain.Trip, error) {
	return s.transition(ctx, who, tripID, domain.Complete, domain.CancelInfo{})
}

// Cancel is US-4.3: cancelling is allowed from ASSIGNED or IN_PROGRESS, never
// once COMPLETED. The caller's side says who cancelled (a rider or the driver)
// and the optional reason says why; both go out on the event. A reason that is
// too long is refused before anything is read.
func (s *Service) Cancel(ctx context.Context, who domain.Actor, tripID, reason string) (domain.Trip, error) {
	info, err := domain.NewCancelInfo(who, reason)
	if err != nil {
		return domain.Trip{}, err
	}
	return s.transition(ctx, who, tripID, domain.Cancel, info)
}

// transition is the shared idempotent-transition shape used by pickup,
// complete and cancel: move the trip along the action only if it is currently
// in a status the action starts from. If it is already where the action leads,
// the call is a no-op success (US-6.1). Any other current status is a rejected
// transition (e.g. US-4.3: cancelling an already-completed trip).
//
// The trip returned carries the new status (and completedAt, once COMPLETED).
func (s *Service) transition(ctx context.Context, who domain.Actor, id string, a domain.Action, info domain.CancelInfo) (domain.Trip, error) {
	t, err := s.trips.ByID(ctx, id)
	if errors.Is(err, ErrTripNotFound) || (err == nil && !a.MayBeDoneBy(who, t)) {
		return domain.Trip{}, ErrTripNotFound
	}
	if err != nil {
		log.Printf("failed to load trip: %v", err)
		return domain.Trip{}, ErrInternal
	}

	if a.Reached(t) {
		return t, nil
	}

	// Pickup and complete only from where the trip says they happen. This
	// comes after the "already there" no-op above, so a retry of a finished
	// step still succeeds from wherever the driver is by then.
	if step, ok := a.ProximityStep(); ok {
		if err := s.nearRoute(ctx, t, step); err != nil {
			return domain.Trip{}, err
		}
	}

	// Encode before changing any state: if the schema registry is down we
	// refuse the request rather than move the trip without telling anyone.
	// The event is queued with the state change, in one transaction.
	payload, err := s.enc.EncodeStatusChanged(ctx, StatusChanged{
		TripID: t.ID, OrderID: t.OrderID, DriverID: t.DriverID, RiderID: t.RiderID,
		Status: a.To(), CancelledBy: info.By, Reason: info.Reason,
	})
	if err != nil {
		log.Printf("failed to encode trip.status.changed: %v", err)
		return domain.Trip{}, ErrInternal
	}

	moved, err := s.moveWithEvent(ctx, id, a, info, t.ID, payload)
	if err != nil {
		log.Printf("failed to transition trip: %v", err)
		return domain.Trip{}, ErrInternal
	}
	if !moved {
		return domain.Trip{}, &CannotTransitionError{To: a.To()}
	}

	t.Status = a.To()
	if t.Status == domain.Completed {
		// Report the time the database stored, not the time of this response.
		if fresh, err := s.trips.ByID(ctx, id); err == nil {
			t.CompletedAt = fresh.CompletedAt
		}
	}

	if a.ReleasesDriver() {
		if err := s.drivers.Set(ctx, t.DriverID, domain.DriverAvailable); err != nil {
			log.Printf("post-transition step failed for trip %s: %v", t.ID, err)
		}
	}
	return t, nil
}

// moveWithEvent moves a trip along the action and queues its event in one
// transaction, so the state and the announcement can't disagree. It reports
// false (and queues nothing) if the trip wasn't in a status the action starts
// from. Only a committed transition is counted.
func (s *Service) moveWithEvent(ctx context.Context, id string, a domain.Action, info domain.CancelInfo, key string, payload []byte) (bool, error) {
	err := s.uow.Do(ctx, func(tx Tx) error {
		moved, err := tx.Transition(ctx, id, a, info)
		if err != nil {
			return err
		}
		if !moved {
			return errRollback
		}
		return tx.EnqueueStatusChanged(ctx, key, payload)
	})
	if errors.Is(err, errRollback) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	s.metrics.Transitioned(string(a.To()))
	return true, nil
}

// nearRoute is the precheck of a transition: the driver must be within the
// radius of the trip's pickup or dropoff. The check fails closed: Redis being
// down is a refusal the driver retries (domain.Unavailable), not a pass, since
// letting everyone through whenever Redis hiccups would make the check
// worthless. A trip with no recorded route (created before this feature) is
// allowed, with a log line; those trips are short-lived.
//
// It returns nil to allow, a *domain.Refusal to send, or ErrInternal.
func (s *Service) nearRoute(ctx context.Context, t domain.Trip, step domain.Step) error {
	cfg := s.prox
	if cfg.Off {
		return nil
	}
	route, err := s.routes.Route(ctx, t.ID)
	if err != nil {
		log.Printf("trip proximity: loading route of trip %s: %v", t.ID, err)
		return ErrInternal
	}
	target, radius := cfg.Target(step, route)
	if target == nil {
		log.Printf("trip proximity: no recorded %s for driver=%s's trip, skipping the check", step, t.DriverID)
		return nil
	}
	pos, found, err := s.locations.Last(ctx, t.DriverID)
	if err != nil {
		log.Printf("trip proximity: reading location of driver=%s: %v", t.DriverID, err)
		return s.refused(domain.Unavailable(step, radius))
	}
	if r := cfg.Judge(step, s.now(), *target, radius, pos, found); r != nil {
		return s.refused(r)
	}
	return nil
}

func (s *Service) refused(r *domain.Refusal) error {
	s.metrics.ProximityRefused(string(r.Step), r.Reason)
	return r
}
