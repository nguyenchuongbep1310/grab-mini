package app

import (
	"context"
	"errors"
	"log"

	"github.com/google/uuid"

	"grab-mini/order-service/internal/domain"
)

// GetOrder lets a client that only knows the order id (e.g. the Driver App
// after an order.matched push) find where the trip starts and ends. To anyone
// who may not read it (domain.Order.MayBeReadBy), an order doesn't exist.
func (s *Service) GetOrder(ctx context.Context, actor domain.Actor, id string) (domain.Order, error) {
	// orders.id is a UUID column: a malformed id would surface as a database
	// error instead of the "not found" it really is.
	if _, err := uuid.Parse(id); err != nil {
		return domain.Order{}, domain.ErrOrderNotFound
	}
	o, err := s.orders.ByID(ctx, id)
	if err == nil && !o.MayBeReadBy(actor) {
		err = domain.ErrOrderNotFound
	}
	return loaded(o, err)
}

// LatestOrder is how a Rider App that was closed or reloaded finds the ride
// it may still be in the middle of. Only the rider may ask (ErrForbidden).
func (s *Service) LatestOrder(ctx context.Context, actor domain.Actor, riderID string) (domain.Order, error) {
	if actor.Subject != riderID {
		return domain.Order{}, ErrForbidden
	}
	return loaded(s.orders.LatestByRider(ctx, riderID))
}

// loaded passes not-found through and logs any other failure as ErrInternal.
func loaded(o domain.Order, err error) (domain.Order, error) {
	if errors.Is(err, domain.ErrOrderNotFound) {
		return domain.Order{}, err
	}
	if err != nil {
		log.Printf("failed to load order: %v", err)
		return domain.Order{}, ErrInternal
	}
	return o, nil
}
