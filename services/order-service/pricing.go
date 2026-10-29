package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/surge"
)

// bookingPricing says how the price of each booking was settled.
var bookingPricing = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "orders_pricing_total",
	Help: "How bookings were priced: quote (the rider's quote honoured), repriced (no or stale quote, current multiplier used), requote (409, multiplier moved too far), quote_reused (409, the quote already priced another order), quote_unchecked (Redis down: the single-use check was skipped, current multiplier used), invalid_quote (400).",
}, []string{"result"})

var surgeFailOpen = promauto.NewCounter(prometheus.CounterOpts{
	Name: "orders_surge_fail_open_total",
	Help: "Bookings priced at 1.0 because the surge multiplier could not be read from Redis.",
})

// multiplierSource gives the current surge multiplier of a cell. It cannot
// fail: when it can't know, the answer is 1.0 (a booking is never blocked by
// pricing).
type multiplierSource interface {
	Multiplier(ctx context.Context, vehicle, cell string) float64
}

// redisMultipliers reads what Pricing Service stores: surge:mult:{vehicle}:{cell}.
type redisMultipliers struct{ rdb *redis.Client }

// readTimeout keeps a struggling Redis from slowing every booking.
const readTimeout = 300 * time.Millisecond

func (r redisMultipliers) Multiplier(ctx context.Context, vehicle, cell string) float64 {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	v, err := r.rdb.Get(ctx, surge.MultKey(vehicle, cell)).Result()
	if err == redis.Nil {
		return 1.0
	}
	if err != nil {
		surgeFailOpen.Inc()
		log.Printf("surge multiplier read failed, pricing at 1.0: %v", err)
		return 1.0
	}
	m, err := strconv.ParseFloat(v, 64)
	if err != nil || m < 1 || math.IsNaN(m) || math.IsInf(m, 0) {
		return 1.0
	}
	return m
}

// quoteClaims makes a quote single-use.
type quoteClaims interface {
	// Claim marks the quote used and reports whether this call was the first
	// to. It keeps the mark for ttl (the quote's remaining life).
	Claim(ctx context.Context, quoteID string, ttl time.Duration) (first bool, err error)
	// Release takes the mark back, for a booking that failed after claiming.
	Release(ctx context.Context, quoteID string)
}

// redisClaims stores the marks as quote:used:{sha256(quoteID)}, SET NX with a TTL.
type redisClaims struct{ rdb *redis.Client }

func claimKey(quoteID string) string {
	sum := sha256.Sum256([]byte(quoteID))
	return "quote:used:" + hex.EncodeToString(sum[:])
}

func (r redisClaims) Claim(ctx context.Context, quoteID string, ttl time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	return r.rdb.SetNX(ctx, claimKey(quoteID), "1", ttl).Result()
}

func (r redisClaims) Release(ctx context.Context, quoteID string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), readTimeout)
	defer cancel()
	if err := r.rdb.Del(ctx, claimKey(quoteID)).Err(); err != nil {
		log.Printf("releasing quote after a failed booking: %v (it stays used until it expires)", err)
	}
}

type noSurge struct{}

func (noSurge) Multiplier(context.Context, string, string) float64 { return 1.0 }

// pricer settles the price of a booking.
type pricer struct {
	signer *surge.Signer
	mult   multiplierSource
	// A stale quote is re-quoted (409) when the multiplier has moved by more
	// than this since the rider saw it; within it the booking is simply
	// priced at the current multiplier.
	tolerance float64
	cellDeg   float64
	now       func() time.Time
	// Makes each quote bookable once; nil means a quote can be reused (tests).
	claims quoteClaims
}

// defaultPricer is used by a server built without one (tests): no surge, and
// a development signer.
func defaultPricer() *pricer {
	s, _ := surge.NewSigner(surge.DevQuoteSecret)
	return &pricer{signer: s, mult: noSurge{}, tolerance: 0.2, cellDeg: surge.CellDegrees(), now: time.Now}
}

// pricing is the settled price of an order.
type pricing struct {
	Multiplier float64
	Price      float64
	// The quote this booking used up; give it back with release if the
	// order is not saved.
	ClaimedQuote string
}

// release gives back the quote a failed booking claimed, so the rider's retry
// can still use it.
func (p *pricer) release(ctx context.Context, settled pricing) {
	if p.claims != nil && settled.ClaimedQuote != "" {
		p.claims.Release(ctx, settled.ClaimedQuote)
	}
}

// errInvalidQuote is a quote that is forged, damaged or for another trip.
var errInvalidQuote = errors.New("invalid quote")

// conflict is returned when the multiplier moved past the tolerance since the
// rider's (expired) quote; NewQuote is what they can confirm instead.
type conflict struct {
	NewQuote surge.Quote
	// Why the rider is asked to confirm; empty means the price moved.
	Reason string
}

func (c *conflict) Error() string {
	if c.Reason != "" {
		return c.Reason
	}
	return "the price has changed"
}

const reasonQuoteUsed = "this quote was already used for another booking"

// Price settles the price of a booking by rider for the trip in req.
//
//   - A valid quote is honoured: the rider pays what they were shown.
//   - No quote, or an expired one: the current multiplier is read and the
//     price is locked at booking. If the rider had an expired quote and the
//     multiplier moved by more than the tolerance, nothing is booked and a
//     *conflict carries a fresh quote to confirm.
//   - A forged, damaged or mismatched quote is errInvalidQuote.
//   - A quote buys one booking: the first to book with it marks it used (SET NX
//     in Redis, kept for the quote's remaining life); a second gets a
//     *conflict with a fresh quote. If Redis can't be reached the booking is
//     not rejected: it is priced at the current multiplier instead, so a
//     replayed quote can never lock a stale price while Redis is down.
//     (An Idempotency-Key replay never gets here: it is answered from the
//     stored response first.)
func (p *pricer) Price(ctx context.Context, riderID string, req createOrderRequest, quoteID string) (pricing, error) {
	qr := surge.QuoteRequest{
		RiderID: riderID, Vehicle: string(req.VehicleType),
		PickupLat: req.Pickup.Lat, PickupLng: req.Pickup.Lng,
		DropoffLat: req.Dropoff.Lat, DropoffLng: req.Dropoff.Lng,
	}
	now := p.now()

	if quoteID != "" {
		q, err := p.signer.Verify(quoteID, qr, now)
		switch {
		case err == nil:
			if p.claims != nil {
				first, cerr := p.claims.Claim(ctx, quoteID, q.ExpiresAt.Sub(now)+time.Second)
				switch {
				case cerr != nil:
					bookingPricing.WithLabelValues("quote_unchecked").Inc()
					log.Printf("quote single-use check failed, pricing at the current multiplier: %v", cerr)
					return p.repriced(qr, p.currentMultiplier(ctx, qr)), nil
				case !first:
					bookingPricing.WithLabelValues("quote_reused").Inc()
					return pricing{}, &conflict{NewQuote: p.signer.Issue(qr, p.currentMultiplier(ctx, qr), now), Reason: reasonQuoteUsed}
				}
			}
			bookingPricing.WithLabelValues("quote").Inc()
			return pricing{Multiplier: q.Multiplier, Price: q.Price, ClaimedQuote: quoteID}, nil
		case errors.Is(err, surge.ErrQuoteExpired):
			current := p.currentMultiplier(ctx, qr)
			if math.Abs(current-q.Multiplier) > p.tolerance+1e-9 {
				bookingPricing.WithLabelValues("requote").Inc()
				return pricing{}, &conflict{NewQuote: p.signer.Issue(qr, current, now)}
			}
			return p.repriced(qr, current), nil
		default:
			bookingPricing.WithLabelValues("invalid_quote").Inc()
			return pricing{}, errInvalidQuote
		}
	}
	return p.repriced(qr, p.currentMultiplier(ctx, qr)), nil
}

func (p *pricer) currentMultiplier(ctx context.Context, qr surge.QuoteRequest) float64 {
	cell := surge.CellID(p.cellDeg, qr.PickupLat, qr.PickupLng)
	return p.mult.Multiplier(ctx, qr.Vehicle, cell)
}

func (p *pricer) repriced(qr surge.QuoteRequest, multiplier float64) pricing {
	bookingPricing.WithLabelValues("repriced").Inc()
	base := surge.BaseFare(qr.Vehicle, surge.HaversineKm(qr.PickupLat, qr.PickupLng, qr.DropoffLat, qr.DropoffLng))
	return pricing{Multiplier: multiplier, Price: surge.Price(base, multiplier)}
}
