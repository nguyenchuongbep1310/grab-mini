package app

import (
	"context"
	"log"

	"github.com/google/uuid"

	"grab-mini/order-service/internal/domain"
)

// SetMatched records that Matching found driverID for a PENDING order: that
// driver is the only one who may read it. A redelivered or out-of-order event
// is a harmless no-op.
func (s *Service) SetMatched(ctx context.Context, orderID, driverID string) error {
	return s.setOutcome(ctx, orderID, domain.StatusMatched, driverID)
}

// SetNoDriver records that Matching found nobody for a PENDING order.
func (s *Service) SetNoDriver(ctx context.Context, orderID string) error {
	return s.setOutcome(ctx, orderID, domain.StatusNoDriver, "")
}

func (s *Service) setOutcome(ctx context.Context, orderID string, status domain.Status, driverID string) error {
	if _, err := uuid.Parse(orderID); err != nil {
		return &BadOutcomeError{"order id " + orderID + " is not a UUID"}
	}
	changed, err := s.outcomes.Set(ctx, orderID, status, driverID)
	if err != nil {
		return err
	}
	if changed {
		log.Printf("order %s -> %s", orderID, status)
	}
	return nil
}

// CancelMatchedOrder records that the trip of a MATCHED order was cancelled
// (by the rider or the driver), so the order reads CANCELLED. Only MATCHED
// orders move, so a redelivered event, or one for an order the rider already
// cancelled while PENDING, is a harmless no-op.
func (s *Service) CancelMatchedOrder(ctx context.Context, orderID, by, reason string) error {
	if _, err := uuid.Parse(orderID); err != nil {
		return &BadOutcomeError{"order id " + orderID + " is not a UUID"}
	}
	changed, err := s.outcomes.CancelMatched(ctx, orderID, by, reason)
	if err != nil {
		return err
	}
	if changed {
		log.Printf("order %s -> CANCELLED (trip cancelled by %q)", orderID, by)
	}
	return nil
}

// PurgeExpiredKeys deletes one batch of Idempotency-Key records past their
// TTL and returns how many went. Expired keys are ignored by a lookup either
// way; this only keeps the table small.
func (s *Service) PurgeExpiredKeys(ctx context.Context) (int64, error) {
	return s.keys.Purge(ctx)
}
