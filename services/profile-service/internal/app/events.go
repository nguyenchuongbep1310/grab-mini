package app

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"grab-mini/profile-service/internal/domain"
)

// queueReview adds driver.application.reviewed to the outbox in tx. d carries the driver's
// vehicle, reason and reviewer as they will be after the change; at is the result's time
// (the event's occurredAt). Every event gets a fresh eventId, minted here.
func (s *Service) queueReview(ctx context.Context, tx Tx, d domain.Driver, status string, at time.Time) error {
	payload, err := s.enc.Reviewed(ctx, ReviewedEvent{
		EventID:    uuid.NewString(),
		OccurredAt: at,
		DriverID:   d.ID,
		Status:     status,
		Vehicle:    d.Vehicle,
		Reason:     d.Reason,
		ReviewedBy: d.ReviewedBy,
	})
	if err != nil {
		return fmt.Errorf("encoding the review result: %w", err)
	}
	return tx.QueueReview(ctx, d.ID, payload)
}
