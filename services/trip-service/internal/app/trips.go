package app

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"

	"grab-mini/trip-service/internal/domain"
)

// Trip reads a trip for its rider or its driver. To anyone else it is
// ErrTripNotFound: whether a trip exists is not theirs to learn.
func (s *Service) Trip(ctx context.Context, who domain.Actor, id string) (domain.Trip, error) {
	return s.view(who, func() (domain.Trip, error) { return s.trips.ByID(ctx, id) })
}

// TripByOrder lets a client that only knows the orderId find the trip created for it.
func (s *Service) TripByOrder(ctx context.Context, who domain.Actor, orderID string) (domain.Trip, error) {
	return s.view(who, func() (domain.Trip, error) { return s.trips.ByOrderID(ctx, orderID) })
}

// ActiveTrip is the driver's trip still in flight, so a driver app that was
// closed or reloaded can pick up where it left off. Only the driver themself
// may ask (ErrForbidden otherwise).
func (s *Service) ActiveTrip(ctx context.Context, who domain.Actor, driverID string) (domain.Trip, error) {
	if who.Role != domain.RoleDriver || who.ID != driverID {
		return domain.Trip{}, ErrForbidden
	}
	return s.view(who, func() (domain.Trip, error) { return s.trips.ActiveByDriver(ctx, driverID) })
}

func (s *Service) view(who domain.Actor, load func() (domain.Trip, error)) (domain.Trip, error) {
	t, err := load()
	if err == nil && !t.IsParty(who) {
		err = ErrTripNotFound
	}
	if errors.Is(err, ErrTripNotFound) {
		return domain.Trip{}, ErrTripNotFound
	}
	if err != nil {
		log.Printf("failed to load trip: %v", err)
		return domain.Trip{}, ErrInternal
	}
	return t, nil
}

// currency is the only currency tips are recorded in.
const currency = "VND"

// RateTrip lets a rider rate a completed trip once, from 1 to 5 stars. The
// rider is whoever the token says (who); a request naming someone else is
// refused. The rating and the trip.rated event commit together, so a rating
// that is stored is always announced (Rating Service counts it).
func (s *Service) RateTrip(ctx context.Context, who domain.Actor, id string, in RateInput) (Rated, error) {
	rating, err := domain.NewRating(in.Stars, in.Comment, in.Tip)
	if err != nil {
		return Rated{}, err
	}
	if in.RiderID != "" && in.RiderID != who.ID {
		return Rated{}, ErrRiderMismatch
	}

	// trips.id is a UUID column: a malformed id would otherwise be a Postgres error.
	if _, err := uuid.Parse(id); err != nil {
		return Rated{}, ErrTripNotFound
	}

	t, err := s.trips.ByID(ctx, id)
	if errors.Is(err, ErrTripNotFound) || (err == nil && !t.IsRiderOf(who)) { // only the trip's rider may rate it
		return Rated{}, ErrTripNotFound
	}
	if err != nil {
		log.Printf("failed to load trip: %v", err)
		return Rated{}, ErrInternal
	}
	if t.Status != domain.Completed {
		return Rated{}, ErrNotCompleted
	}
	if t.Rating != nil {
		return Rated{}, ErrAlreadyRated
	}

	// Encode before saving, like status changes: no rating without an event.
	payload, err := s.enc.EncodeTripRated(ctx, TripRated{
		TripID: t.ID, OrderID: t.OrderID, RiderID: who.ID, DriverID: t.DriverID,
		Stars: rating.Stars, Comment: rating.Comment, TipAmount: rating.Tip, Currency: currency,
	})
	if err != nil {
		log.Printf("failed to encode trip.rated: %v", err)
		return Rated{}, ErrInternal
	}

	saved, err := s.rateWithEvent(ctx, t, rating, payload)
	if err != nil {
		log.Printf("failed to save rating: %v", err)
		return Rated{}, ErrInternal
	}
	if !saved {
		return Rated{}, ErrAlreadyRated
	}
	return Rated{TripID: t.ID, Stars: rating.Stars, Tip: rating.Tip}, nil
}

// rateWithEvent saves the rating and queues trip.rated in one transaction.
// It reports false (and queues nothing) if the trip was rated meanwhile.
func (s *Service) rateWithEvent(ctx context.Context, t domain.Trip, r domain.Rating, payload []byte) (bool, error) {
	err := s.uow.Do(ctx, func(tx Tx) error {
		saved, err := tx.SaveRating(ctx, t.ID, r)
		if err != nil {
			return err
		}
		if !saved {
			return errRollback
		}
		return tx.EnqueueTripRated(ctx, t.DriverID, payload)
	})
	if errors.Is(err, errRollback) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	s.metrics.Rated()
	return true, nil
}

// ReceiptClosed is the outcome of closing a receipt.
type ReceiptClosed struct {
	TripID string
	At     time.Time
}

// CloseReceipt records that the rider closed the finished ride's receipt, so
// no device shows it again. Idempotent: the first time is kept.
func (s *Service) CloseReceipt(ctx context.Context, who domain.Actor, id string) (ReceiptClosed, error) {
	if _, err := uuid.Parse(id); err != nil {
		return ReceiptClosed{}, ErrTripNotFound
	}
	t, err := s.trips.ByID(ctx, id)
	if errors.Is(err, ErrTripNotFound) || (err == nil && !t.IsRiderOf(who)) {
		return ReceiptClosed{}, ErrTripNotFound
	}
	if err != nil {
		log.Printf("failed to load trip: %v", err)
		return ReceiptClosed{}, ErrInternal
	}
	if t.Status != domain.Completed {
		return ReceiptClosed{}, ErrNotCompleted
	}
	at, found, err := s.receipts.CloseReceipt(ctx, id)
	if err != nil {
		log.Printf("failed to close receipt: %v", err)
		return ReceiptClosed{}, ErrInternal
	}
	if !found { // the trip stopped being COMPLETED between the read and the write
		return ReceiptClosed{}, ErrNotCompleted
	}
	return ReceiptClosed{TripID: id, At: at}, nil
}
