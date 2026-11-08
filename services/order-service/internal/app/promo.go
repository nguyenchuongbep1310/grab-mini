package app

import (
	"context"
	"errors"
	"log"
	"math"
	"time"

	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/promo"
)

// PromoChecker verifies the promo token of a booking. It never calls Promotion
// Service: the token was signed by it with a key Order Service shares, and
// carries the code, the discount and what it is bound to.
type PromoChecker struct {
	Signer *promo.Signer
	// Makes each token good for one booking; nil means a token can be reused.
	Claims  ClaimStore
	Now     func() time.Time
	Metrics Metrics
}

// Applied is a discount that has been verified for a booking.
type Applied struct {
	Code     string
	Discount int64
	// The token this booking used up; give it back with Release if the order is not saved.
	claimed string
}

// roundVND is a price as the whole number of dong a token is bound to.
func roundVND(price float64) int64 { return int64(math.Round(price)) }

// Apply checks token for a trip booked by riderID, with vehicle, at the settled
// price (VND). A token that is forged, expired, or issued to another rider, for
// another price or another vehicle type is refused (a *PromoError) with a code;
// one already used for another booking is a *PromoError with Conflict set. The
// token buys one booking, like a quote: the first to book with it marks it used
// (SET NX in Redis, kept for its remaining life). If Redis cannot be reached
// the discount is still applied rather than failing the booking (counted as
// "unchecked"): the token is signed and short-lived, and Promotion Service
// enforces the limits when it records the redemption.
func (c *PromoChecker) Apply(ctx context.Context, riderID string, vehicle domain.VehicleType, price float64, token string) (Applied, error) {
	now := c.Now()
	claims, err := c.Signer.Verify(token, promo.Binding{RiderID: riderID, Price: roundVND(price), Vehicle: string(vehicle)}, now)
	if err != nil {
		return Applied{}, c.refusal(err)
	}
	a := Applied{Code: claims.Code, Discount: claims.Discount}
	if c.Claims != nil {
		first, cerr := c.Claims.Claim(ctx, "promo:"+token, claims.ExpiresAt.Sub(now)+time.Second)
		switch {
		case cerr != nil:
			c.Metrics.Promo("unchecked")
			log.Printf("promo token single-use check failed, applying the discount: %v", cerr)
			return a, nil
		case !first:
			c.Metrics.Promo("used")
			return Applied{}, &PromoError{Code: "promo_used", Message: "this promo code was already used for another booking: apply it again", Conflict: true}
		}
		a.claimed = "promo:" + token
	}
	c.Metrics.Promo("applied")
	return a, nil
}

// Release gives back the token a failed booking claimed, so the rider's retry can use it.
func (c *PromoChecker) Release(ctx context.Context, a Applied) {
	if c.Claims != nil && a.claimed != "" {
		c.Claims.Release(ctx, a.claimed)
	}
}

func (c *PromoChecker) refusal(err error) *PromoError {
	e := &PromoError{}
	var result string
	switch {
	case errors.Is(err, promo.ErrExpired):
		e.Code, result, e.Message = "promo_expired", "expired", "the promo code has expired: apply it again"
	case errors.Is(err, promo.ErrWrongRider):
		e.Code, result, e.Message = "promo_wrong_rider", "wrong_rider", "this promo code was issued to another rider"
	case errors.Is(err, promo.ErrWrongPrice):
		e.Code, result, e.Message = "promo_wrong_price", "wrong_price", "the price changed since the promo code was applied: apply it again"
	case errors.Is(err, promo.ErrWrongVehicle):
		e.Code, result, e.Message = "promo_wrong_vehicle", "wrong_vehicle", "this promo code was issued for another vehicle type"
	default: // forged, damaged or unreadable
		e.Code, result, e.Message = "promo_invalid", "invalid", "the promo token is invalid"
	}
	c.Metrics.Promo(result)
	return e
}
