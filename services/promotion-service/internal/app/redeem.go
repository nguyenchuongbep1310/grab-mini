package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"grab-mini/promotion-service/internal/domain"
)

// RedeemOrder records the redemption of an order that booked with a promo
// code. An order without one is none of this service's business.
//
// The order already exists with its discount locked, and the rider has been
// told the price, so nothing here may refuse it: a limit reached in the
// meantime is recorded as OVER_LIMIT, not failed. The limits are counted under
// the promotion's row lock, so the counts are exact even with several workers.
//
// The eventId marker, the redemption and the promotion.redeemed outbox row
// commit together. A second order.created for the same order (a different
// eventId) hits (code, order_id) and changes nothing but its marker.
//
// Errors: *domain.InvalidEvent (permanent) for an event that can never be
// applied; a port's error as it is otherwise, after rolling everything back.
func (s *Service) RedeemOrder(ctx context.Context, c RedeemOrderCommand) error {
	if c.PromoCode == "" {
		return nil
	}
	code := domain.NormalizeCode(c.PromoCode)
	if err := domain.ValidateRedemption(c.OrderID, c.RiderID, c.Discount); err != nil {
		return err
	}

	// What fn decided, read after a successful commit.
	var (
		recorded bool // a new redemption row was inserted
		decision domain.Redemption
	)
	err := s.d.UoW.Do(ctx, func(tx Tx) error {
		if err := checkEventID(c.EventID); err != nil {
			return err
		}
		first, err := tx.MarkEventProcessed(ctx, c.EventID)
		if err != nil {
			return err
		}
		if !first {
			s.d.Metrics.Duplicate()
			log.Printf("duplicate event %s, skipping", c.EventID)
			return nil
		}
		if err := tx.LockOrder(ctx, c.OrderID); err != nil {
			return err
		}
		p, err := tx.FindPromotionForUpdate(ctx, code)
		if errors.Is(err, domain.ErrNotFound) {
			return invalidEvent(fmt.Sprintf("order %s booked with unknown promotion %q", c.OrderID, code))
		}
		if err != nil {
			return err
		}

		// The order was cancelled before we saw it was placed: record it as given back.
		cancelled, err := tx.OrderWasCancelled(ctx, c.OrderID)
		if err != nil {
			return err
		}
		var u domain.Usage
		if !cancelled {
			if u, err = tx.Usage(ctx, code, c.RiderID); err != nil {
				return err
			}
		}
		d := domain.Decide(p, u, cancelled)

		var releasedAt *time.Time
		if cancelled {
			now := s.now() // the application clock, unlike a release's
			releasedAt = &now
		}
		inserted, err := tx.InsertRedemption(ctx, NewRedemption{
			OrderID: c.OrderID, Code: code, RiderID: c.RiderID, Discount: c.Discount,
			StoredStatus: d.StoredStatus, OverLimitReason: d.Reason, SourceEventID: c.EventID, ReleasedAt: releasedAt,
		})
		if err != nil {
			return err
		}
		if !inserted {
			s.d.Metrics.Duplicate()
			return nil // already recorded under another event; keep the marker
		}

		if d.Announced {
			payload, err := s.d.Encoder.Redeemed(ctx, RedeemedEvent{
				EventID: uuid.NewString(), OccurredAt: s.now(),
				OrderID: c.OrderID, RiderID: c.RiderID, Code: code, Discount: c.Discount,
				Status: d.Status, Currency: domain.CurrencyVND,
			})
			if err != nil {
				return err
			}
			if err := tx.QueueRedeemed(ctx, c.OrderID, payload); err != nil {
				return err
			}
		}
		recorded, decision = true, d
		return nil
	})
	if err != nil || !recorded {
		return err
	}

	// Woken after every commit that recorded a redemption, even one that queued
	// nothing (an order already cancelled), as the flat code did.
	if s.d.Relay != nil {
		s.d.Relay.Wake()
	}
	switch {
	case !decision.Announced:
		s.d.Metrics.Released(1)
		log.Printf("order %s used %s but was already cancelled: recorded as released", c.OrderID, code)
	case decision.Status == domain.StatusOverLimit:
		s.d.Metrics.Redemption(decision.Status, decision.Reason)
		log.Printf("order %s used %s past its %s: recorded as OVER_LIMIT (discount %d VND stays on the order)", c.OrderID, code, decision.Reason, c.Discount)
	default:
		s.d.Metrics.Redemption(decision.Status, "")
		log.Printf("order %s redeemed %s for %d VND", c.OrderID, code, c.Discount)
	}
	return nil
}
