package app

import (
	"context"
	"errors"
	"log"
	"math"
	"time"

	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/surge"
)

// Pricer settles the price of a booking.
type Pricer struct {
	Signer *surge.Signer
	Mult   MultiplierSource
	// Makes each quote bookable once; nil means a quote can be reused.
	Claims ClaimStore
	// A stale quote is re-quoted (409) when the multiplier has moved by more
	// than Tolerance since the rider saw it; within it the booking is simply
	// priced at the current multiplier.
	Tolerance, CellDeg float64
	Now                func() time.Time
	Metrics            Metrics
}

// Pricing is the settled price of an order.
type Pricing struct {
	Multiplier float64
	Price      float64
	// The quote this booking used up; give it back with Release if the
	// order is not saved.
	ClaimedQuote string
}

// Release gives back the quote a failed booking claimed, so the rider's retry
// can still use it.
func (p *Pricer) Release(ctx context.Context, settled Pricing) {
	if p.Claims != nil && settled.ClaimedQuote != "" {
		p.Claims.Release(ctx, settled.ClaimedQuote)
	}
}

// Price settles the price of a booking by rider for the trip in req.
//
//   - A valid quote is honoured: the rider pays what they were shown.
//   - No quote, or an expired one: the current multiplier is read and the
//     price is locked at booking. If the rider had an expired quote and the
//     multiplier moved by more than the tolerance, nothing is booked and a
//     *QuoteConflict carries a fresh quote to confirm.
//   - Distance: Pricing Service works the base fare out from the routed
//     distance, which Order Service does not have. A re-pricing that follows a
//     quote (expired, reused, or unchecked) therefore keeps the base fare the
//     quote's signed body carries, and so does the replacement quote of a 409,
//     so only the multiplier moves. With no quote at all there is no routed
//     distance to keep and the base fare is worked out from the straight line.
//   - A forged, damaged or mismatched quote is ErrInvalidQuote.
//   - A quote buys one booking: the first to book with it marks it used (SET NX
//     in Redis, kept for the quote's remaining life); a second gets a
//     *QuoteConflict with a fresh quote. If Redis can't be reached the booking is
//     not rejected: it is priced at the current multiplier instead, so a
//     replayed quote can never lock a stale price while Redis is down.
//     (An Idempotency-Key replay never gets here: it is answered from the
//     stored response first.)
func (p *Pricer) Price(ctx context.Context, riderID string, req domain.BookingRequest) (Pricing, error) {
	quoteID := req.QuoteID
	qr := surge.QuoteRequest{
		RiderID: riderID, Vehicle: string(req.VehicleType),
		PickupLat: req.Pickup.Lat, PickupLng: req.Pickup.Lng,
		DropoffLat: req.Dropoff.Lat, DropoffLng: req.Dropoff.Lng,
	}
	now := p.Now()

	if quoteID != "" {
		q, err := p.Signer.Verify(quoteID, qr, now)
		switch {
		case err == nil:
			if p.Claims != nil {
				first, cerr := p.Claims.Claim(ctx, quoteID, q.ExpiresAt.Sub(now)+time.Second)
				switch {
				case cerr != nil:
					p.Metrics.Pricing("quote_unchecked")
					log.Printf("quote single-use check failed, pricing at the current multiplier: %v", cerr)
					return p.repriced(qr, p.currentMultiplier(ctx, qr), q.BaseFare), nil
				case !first:
					p.Metrics.Pricing("quote_reused")
					return Pricing{}, &QuoteConflict{NewQuote: p.Signer.IssueForBase(qr, q.BaseFare, p.currentMultiplier(ctx, qr), now), Reason: ReasonQuoteUsed}
				}
			}
			p.Metrics.Pricing("quote")
			return Pricing{Multiplier: q.Multiplier, Price: q.Price, ClaimedQuote: quoteID}, nil
		case errors.Is(err, surge.ErrQuoteExpired):
			current := p.currentMultiplier(ctx, qr)
			if math.Abs(current-q.Multiplier) > p.Tolerance+1e-9 {
				p.Metrics.Pricing("requote")
				return Pricing{}, &QuoteConflict{NewQuote: p.Signer.IssueForBase(qr, q.BaseFare, current, now)}
			}
			return p.repriced(qr, current, q.BaseFare), nil
		default:
			p.Metrics.Pricing("invalid_quote")
			return Pricing{}, ErrInvalidQuote
		}
	}
	return p.repriced(qr, p.currentMultiplier(ctx, qr), 0), nil
}

func (p *Pricer) currentMultiplier(ctx context.Context, qr surge.QuoteRequest) float64 {
	cell := surge.CellID(p.CellDeg, qr.PickupLat, qr.PickupLng)
	return p.Mult.Multiplier(ctx, qr.Vehicle, cell)
}

// repriced prices the booking at multiplier. baseFare is the fare before surge
// carried by a quote the rider held (0 when there is none or it carries none),
// kept as it is; without it the fare comes from the straight-line distance.
func (p *Pricer) repriced(qr surge.QuoteRequest, multiplier, baseFare float64) Pricing {
	p.Metrics.Pricing("repriced")
	if baseFare <= 0 {
		baseFare = surge.BaseFare(qr.Vehicle, surge.HaversineKm(qr.PickupLat, qr.PickupLng, qr.DropoffLat, qr.DropoffLng))
	}
	return Pricing{Multiplier: multiplier, Price: surge.Price(baseFare, multiplier)}
}
