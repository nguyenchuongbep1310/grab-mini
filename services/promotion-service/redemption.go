package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
)

// invalidEvent is an event that can never be applied (no order id, a code that
// does not exist here). Permanent: straight to the dead-letter topic instead of
// being retried, where it pages: a discount that was given and not recorded.
type invalidEvent struct{ reason string }

func (e *invalidEvent) Error() string   { return "invalid promotion event: " + e.reason }
func (e *invalidEvent) Permanent() bool { return true }

// markEventProcessed records that eventID was applied, and reports whether
// this is the first time. It must run in the transaction that applies it.
func markEventProcessed(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	if _, err := uuid.Parse(eventID); err != nil {
		return false, &invalidEvent{"eventId " + eventID + " is not a UUID"}
	}
	tag, err := tx.Exec(ctx, `INSERT INTO promotion_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// lockOrder serialises a redemption and a release of one order. order.created and
// the events that release it are on different topics, so they can be handled at
// the same moment by different workers; whichever takes the lock second sees what
// the first did.
func lockOrder(ctx context.Context, tx pgx.Tx, orderID string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "promotion-order:"+orderID)
	return err
}

// onOrderCreated records the redemption of an order that booked with a promo
// code. An order without one is none of this service's business.
//
// The order already exists with its discount locked, and the rider has been
// told the price, so nothing here may refuse it: a limit that was reached in the
// meantime is recorded as OVER_LIMIT, not failed. The limits are counted under
// the promotion's row lock, so the counts are exact even with several workers;
// what stays soft is that two orders can both have been validated before either
// was recorded (see production-design.md, "Promotions").
//
// The eventId dedupe row, the redemption and the promotion.redeemed outbox row
// commit together. A second order.created for the same order (a different
// eventId) hits (code, order_id) and changes nothing.
func (s *service) onOrderCreated(ctx context.Context, evt events.OrderCreated) error {
	if evt.PromoCode == "" {
		return nil
	}
	code := normalizeCode(evt.PromoCode)
	if evt.OrderID == "" || evt.RiderID == "" {
		return &invalidEvent{"order.created without orderId or riderId"}
	}
	if evt.Discount < 0 || evt.Discount > maxAmount {
		return &invalidEvent{fmt.Sprintf("discount %d is not an amount of VND", evt.Discount)}
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	first, err := markEventProcessed(ctx, tx, evt.EventID)
	if err != nil {
		return err
	}
	if !first {
		redemptionsDuplicate.Inc()
		log.Printf("duplicate event %s, skipping", evt.EventID)
		return nil
	}
	if err := lockOrder(ctx, tx, evt.OrderID); err != nil {
		return err
	}
	p, err := findPromotion(ctx, tx, code, true)
	if errors.Is(err, errNoPromotion) {
		return &invalidEvent{fmt.Sprintf("order %s booked with unknown promotion %q", evt.OrderID, code)}
	}
	if err != nil {
		return err
	}

	// The order was cancelled before we saw it was placed: record it as given back.
	var cancelled bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM promotion_cancelled_orders WHERE order_id = $1)`, evt.OrderID).Scan(&cancelled); err != nil {
		return err
	}

	status, reason := events.PromotionStatusRedeemed, ""
	if !cancelled {
		u, err := usage(ctx, tx, code, evt.RiderID)
		if err != nil {
			return err
		}
		if reason = p.overLimitReason(u); reason != "" {
			status = events.PromotionStatusOverLimit
		}
	}

	stored := status
	var releasedAt *time.Time
	if cancelled {
		stored = "RELEASED"
		now := s.now()
		releasedAt = &now
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO redemptions (order_id, code, rider_id, discount, status, over_limit_reason, source_event_id, released_at)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8)
		ON CONFLICT (code, order_id) DO NOTHING`,
		evt.OrderID, code, evt.RiderID, evt.Discount, stored, reason, evt.EventID, releasedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		redemptionsDuplicate.Inc()
		return tx.Commit(ctx) // already recorded under another event; keep the dedupe row
	}

	if !cancelled {
		payload, err := s.enc.Encode(ctx, events.TopicPromotionRedeemed, events.PromotionRedeemed{
			Envelope: events.Envelope{EventID: uuid.NewString(), OccurredAt: s.now().UTC().Format(time.RFC3339Nano)},
			OrderID:  evt.OrderID, RiderID: evt.RiderID, Code: code, Discount: evt.Discount, Status: status, Currency: currencyVND,
		})
		if err != nil {
			return err
		}
		if err := outbox.Add(ctx, tx, outboxSource, events.TopicPromotionRedeemed, []byte(evt.OrderID), payload); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if s.relay != nil {
		s.relay.Wake()
	}
	switch {
	case cancelled:
		redemptionsReleased.Inc()
		log.Printf("order %s used %s but was already cancelled: recorded as released", evt.OrderID, code)
	case status == events.PromotionStatusOverLimit:
		redemptions.WithLabelValues(status, reason).Inc()
		log.Printf("order %s used %s past its %s: recorded as OVER_LIMIT (discount %d VND stays on the order)", evt.OrderID, code, reason, evt.Discount)
	default:
		redemptions.WithLabelValues(status, "").Inc()
		log.Printf("order %s redeemed %s for %d VND", evt.OrderID, code, evt.Discount)
	}
	return nil
}

// onOrderCancelled gives back the redemption of an order a rider cancelled while
// it was still PENDING (the only time order.cancelled is published). The other
// ways an order ends without a ride are onTripStatusChanged and onNoDriverFound.
func (s *service) onOrderCancelled(ctx context.Context, evt events.OrderCancelled) error {
	return s.releaseOrder(ctx, evt.EventID, evt.OrderID, "order.cancelled", "cancelled")
}

// onTripStatusChanged gives back the redemption of an order whose trip was
// cancelled after the match (by the rider or the driver): Order Service reads it
// as a CANCELLED order, but publishes no order.cancelled for it. Every other trip
// status leaves the redemption alone, a COMPLETED trip being the one that used it.
func (s *service) onTripStatusChanged(ctx context.Context, evt events.TripStatusChanged) error {
	if evt.Status != events.TripStatusCancelled {
		return nil
	}
	return s.releaseOrder(ctx, evt.EventID, evt.OrderID, "trip.status.changed", "trip cancelled")
}

// onNoDriverFound gives back the redemption of an order that ended NO_DRIVER: no
// ride took place, and the rider is not charged.
func (s *service) onNoDriverFound(ctx context.Context, evt events.OrderNoDriverFound) error {
	return s.releaseOrder(ctx, evt.EventID, evt.OrderID, "order.no_driver_found", "found no driver")
}

// releaseOrder is what the three events that end an order without a ride have in
// common: mark the order's redemption RELEASED, which stops it counting against
// the limits. It publishes nothing (as with order.cancelled before: promotion.redeemed
// announces a redemption, not its release).
//
// The same order can be named by several of them (a rider cancels just as a driver
// is matched: order.cancelled, then the trip's CANCELLED), and each has its own
// eventId, so the release is also idempotent by itself: only REDEEMED and OVER_LIMIT
// rows move, and a RELEASED one is left as it was (with its original released_at).
//
// When no redemption exists yet, the order id is remembered, so that an
// order.created handled later records the redemption as released. Most such orders
// have no code, and for them that row is all that happens.
func (s *service) releaseOrder(ctx context.Context, eventID, orderID, topic, what string) error {
	if orderID == "" {
		return &invalidEvent{topic + " without orderId"}
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	first, err := markEventProcessed(ctx, tx, eventID)
	if err != nil {
		return err
	}
	if !first {
		redemptionsDuplicate.Inc()
		log.Printf("duplicate event %s, skipping", eventID)
		return nil
	}
	if err := lockOrder(ctx, tx, orderID); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE redemptions SET status = 'RELEASED', released_at = now()
		WHERE order_id = $1 AND status IN ('REDEEMED', 'OVER_LIMIT')`, orderID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if _, err := tx.Exec(ctx, `INSERT INTO promotion_cancelled_orders (order_id) VALUES ($1) ON CONFLICT DO NOTHING`, orderID); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if n := tag.RowsAffected(); n > 0 {
		redemptionsReleased.Add(float64(n))
		log.Printf("order %s %s (%s): %d redemption(s) released", orderID, what, topic, n)
	}
	return nil
}
