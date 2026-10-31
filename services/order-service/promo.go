package main

import (
	"context"
	"errors"
	"log"
	"math"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/pkg/events"
	"grab-mini/pkg/promo"
)

// bookingPromos says what happened to the promo token of each booking that carried one.
var bookingPromos = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "orders_promo_total",
	Help: "Promo tokens presented at booking, by result: applied, unchecked (Redis down: the single-use check was skipped, the discount was applied), used (409, already used for another booking), or the refusal (invalid, expired, wrong_rider, wrong_price, wrong_vehicle: 400).",
}, []string{"result"})

// promoError is a promo token that cannot be applied. Code is the stable,
// machine-readable reason in the response body.
type promoError struct {
	Code    string
	Message string
	Status  int
	result  string
}

func (e *promoError) Error() string { return e.Code + ": " + e.Message }

// promoChecker verifies the promo token of a booking. It never calls Promotion
// Service: the token was signed by it with a key Order Service shares, and
// carries the code, the discount and what it is bound to.
type promoChecker struct {
	signer *promo.Signer
	// Makes each token good for one booking; nil means a token can be reused (tests).
	claims quoteClaims
	now    func() time.Time
}

// defaultPromoChecker is used by a server built without one (tests): a
// development signer.
func defaultPromoChecker() *promoChecker {
	s, _ := promo.NewSigner(promo.DevSecret)
	return &promoChecker{signer: s, now: time.Now}
}

// applied is a discount that has been verified for a booking.
type applied struct {
	Code     string
	Discount int64
	// The token this booking used up; give it back with release if the order is not saved.
	claimed string
}

// roundVND is a price as the whole number of dong a token is bound to.
func roundVND(price float64) int64 { return int64(math.Round(price)) }

// Apply checks token for a trip booked by riderID, with vehicle, at the settled
// price (VND). A token that is forged, expired, or issued to another rider, for
// another price or another vehicle type is refused with 400 and a code; one
// already used for another booking is a 409. The token buys one booking, like a
// quote: the first to book with it marks it used (SET NX in Redis, kept for its
// remaining life). If Redis cannot be reached the discount is still applied
// rather than failing the booking (counted as "unchecked"): the token is signed
// and short-lived, and Promotion Service enforces the limits when it records the
// redemption.
func (c *promoChecker) Apply(ctx context.Context, riderID string, vehicle events.VehicleType, price float64, token string) (applied, error) {
	now := c.now()
	claims, err := c.signer.Verify(token, promo.Binding{RiderID: riderID, Price: roundVND(price), Vehicle: string(vehicle)}, now)
	if err != nil {
		return applied{}, refusal(err)
	}
	a := applied{Code: claims.Code, Discount: claims.Discount}
	if c.claims != nil {
		first, cerr := c.claims.Claim(ctx, "promo:"+token, claims.ExpiresAt.Sub(now)+time.Second)
		switch {
		case cerr != nil:
			bookingPromos.WithLabelValues("unchecked").Inc()
			log.Printf("promo token single-use check failed, applying the discount: %v", cerr)
			return a, nil
		case !first:
			bookingPromos.WithLabelValues("used").Inc()
			return applied{}, &promoError{Code: "promo_used", Message: "this promo code was already used for another booking: apply it again", Status: http.StatusConflict}
		}
		a.claimed = "promo:" + token
	}
	bookingPromos.WithLabelValues("applied").Inc()
	return a, nil
}

// release gives back the token a failed booking claimed, so the rider's retry can use it.
func (c *promoChecker) release(ctx context.Context, a applied) {
	if c.claims != nil && a.claimed != "" {
		c.claims.Release(ctx, a.claimed)
	}
}

func refusal(err error) *promoError {
	e := &promoError{Status: http.StatusBadRequest}
	switch {
	case errors.Is(err, promo.ErrExpired):
		e.Code, e.result, e.Message = "promo_expired", "expired", "the promo code has expired: apply it again"
	case errors.Is(err, promo.ErrWrongRider):
		e.Code, e.result, e.Message = "promo_wrong_rider", "wrong_rider", "this promo code was issued to another rider"
	case errors.Is(err, promo.ErrWrongPrice):
		e.Code, e.result, e.Message = "promo_wrong_price", "wrong_price", "the price changed since the promo code was applied: apply it again"
	case errors.Is(err, promo.ErrWrongVehicle):
		e.Code, e.result, e.Message = "promo_wrong_vehicle", "wrong_vehicle", "this promo code was issued for another vehicle type"
	default: // forged, damaged or unreadable
		e.Code, e.result, e.Message = "promo_invalid", "invalid", "the promo token is invalid"
	}
	bookingPromos.WithLabelValues(e.result).Inc()
	return e
}
