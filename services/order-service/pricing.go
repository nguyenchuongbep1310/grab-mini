package main

import (
	"context"
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
	Help: "How bookings were priced: quote (the rider's quote honoured), repriced (no or stale quote, current multiplier used), requote (409, multiplier moved too far), invalid_quote (400).",
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
}

// errInvalidQuote is a quote that is forged, damaged or for another trip.
var errInvalidQuote = errors.New("invalid quote")

// conflict is returned when the multiplier moved past the tolerance since the
// rider's (expired) quote; NewQuote is what they can confirm instead.
type conflict struct{ NewQuote surge.Quote }

func (c *conflict) Error() string { return "the price has changed" }

// Price settles the price of a booking by rider for the trip in req.
//
//   - A valid quote is honoured: the rider pays what they were shown.
//   - No quote, or an expired one: the current multiplier is read and the
//     price is locked at booking. If the rider had an expired quote and the
//     multiplier moved by more than the tolerance, nothing is booked and a
//     *conflict carries a fresh quote to confirm.
//   - A forged, damaged or mismatched quote is errInvalidQuote.
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
			bookingPricing.WithLabelValues("quote").Inc()
			return pricing{Multiplier: q.Multiplier, Price: q.Price}, nil
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
