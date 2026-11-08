package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/promo"
)

func testPromoChecker(t *testing.T) (*PromoChecker, *fakeClaims, *fakeMetrics) {
	t.Helper()
	signer, err := promo.NewSigner("test-promo-secret")
	if err != nil {
		t.Fatal(err)
	}
	claims, m := newFakeClaims(), newFakeMetrics()
	return &PromoChecker{Signer: signer, Claims: claims, Now: func() time.Time { return t0 }, Metrics: m}, claims, m
}

func issue(c *PromoChecker, rider string, price int64, vehicle string, discount int64, at time.Time) string {
	tok, _ := c.Signer.Issue("WELCOME10", promo.Binding{RiderID: rider, Price: price, Vehicle: vehicle}, discount, at)
	return tok
}

func TestAValidPromoTokenGivesItsCodeAndDiscount(t *testing.T) {
	c, _, m := testPromoChecker(t)
	tok := issue(c, "rider-1", 87000, "CAR", 8700, t0.Add(-time.Minute))
	got, err := c.Apply(context.Background(), "rider-1", domain.VehicleCar, 87000, tok)
	if err != nil || got.Code != "WELCOME10" || got.Discount != 8700 {
		t.Fatalf("Apply = %+v, %v", got, err)
	}
	if m.promos["applied"] != 1 {
		t.Errorf("promo metrics = %v, want one applied", m.promos)
	}
}

func TestPromoTokensThatDoNotFitTheBookingAreRefusedWithACode(t *testing.T) {
	c, _, m := testPromoChecker(t)
	good := issue(c, "rider-1", 87000, "CAR", 8700, t0.Add(-time.Minute))
	payload, sig, _ := strings.Cut(good, ".")
	other, _ := promo.NewSigner("another-secret")
	foreign, _ := other.Issue("WELCOME10", promo.Binding{RiderID: "rider-1", Price: 87000, Vehicle: "CAR"}, 8700, t0)

	cases := []struct {
		name    string
		token   string
		rider   string
		vehicle domain.VehicleType
		price   float64
		want    string
		result  string
	}{
		{"forged signature", payload + "." + sig[:len(sig)-2] + "AA", "rider-1", domain.VehicleCar, 87000, "promo_invalid", "invalid"},
		{"signed with another key", foreign, "rider-1", domain.VehicleCar, 87000, "promo_invalid", "invalid"},
		{"not a token", "hello", "rider-1", domain.VehicleCar, 87000, "promo_invalid", "invalid"},
		{"expired", issue(c, "rider-1", 87000, "CAR", 8700, t0.Add(-promo.TokenTTL-time.Second)), "rider-1", domain.VehicleCar, 87000, "promo_expired", "expired"},
		{"another rider's token", good, "rider-2", domain.VehicleCar, 87000, "promo_wrong_rider", "wrong_rider"},
		{"another price", good, "rider-1", domain.VehicleCar, 90000, "promo_wrong_price", "wrong_price"},
		{"another vehicle", good, "rider-1", domain.VehicleBike, 87000, "promo_wrong_vehicle", "wrong_vehicle"},
	}
	for _, tc := range cases {
		_, err := c.Apply(context.Background(), tc.rider, tc.vehicle, tc.price, tc.token)
		var pe *PromoError
		if !errors.As(err, &pe) {
			t.Errorf("%s: err = %v, want a *PromoError", tc.name, err)
			continue
		}
		if pe.Code != tc.want || pe.Conflict || pe.Message == "" {
			t.Errorf("%s: %+v, want a refusal %s", tc.name, pe, tc.want)
		}
		if m.promos[tc.result] == 0 {
			t.Errorf("%s: promo metrics = %v, want %s counted", tc.name, m.promos, tc.result)
		}
	}
}

func TestAPromoTokenBooksOnceAndCanBeGivenBack(t *testing.T) {
	c, claims, m := testPromoChecker(t)
	ctx := context.Background()
	tok := issue(c, "rider-1", 87000, "CAR", 8700, t0)

	first, err := c.Apply(ctx, "rider-1", domain.VehicleCar, 87000, tok)
	if err != nil {
		t.Fatal(err)
	}
	if ttl := claims.ttls["promo:"+tok]; ttl <= 0 || ttl > promo.TokenTTL+2*time.Second {
		t.Errorf("the mark lives %v, want the token's remaining life", ttl)
	}
	_, err = c.Apply(ctx, "rider-1", domain.VehicleCar, 87000, tok)
	var pe *PromoError
	if !errors.As(err, &pe) || pe.Code != "promo_used" || !pe.Conflict {
		t.Fatalf("second use = %v, want a conflicting promo_used", err)
	}
	if m.promos["used"] != 1 {
		t.Errorf("promo metrics = %v, want one used", m.promos)
	}
	c.Release(ctx, first) // the booking failed after the check
	if _, err := c.Apply(ctx, "rider-1", domain.VehicleCar, 87000, tok); err != nil {
		t.Errorf("after a failed booking gave it back: %v", err)
	}
}

// Fail-open policy: with Redis down the token can't be marked used, but it is signed and
// short-lived and Promotion Service enforces the limits, so the booking is not turned away.
func TestWithRedisDownThePromoIsStillApplied(t *testing.T) {
	c, claims, m := testPromoChecker(t)
	tok := issue(c, "rider-1", 87000, "CAR", 8700, t0)
	claims.down = true
	got, err := c.Apply(context.Background(), "rider-1", domain.VehicleCar, 87000, tok)
	if err != nil || got.Discount != 8700 {
		t.Fatalf("Apply = %+v, %v; want the discount applied", got, err)
	}
	if got.claimed != "" {
		t.Error("claimed a token it could not mark")
	}
	if m.promos["unchecked"] != 1 {
		t.Errorf("promo metrics = %v, want one unchecked", m.promos)
	}
}

// Who releases what: a refused promo does not touch the quote claim, so the
// caller (the booking use case) gives the quote back.
func TestARefusedPromoLeavesTheQuoteClaimedForTheCallerToRelease(t *testing.T) {
	p, claims, _ := claimingPricer(t, 1.0)
	c, _, _ := testPromoChecker(t)
	c.Claims = claims
	ctx := context.Background()
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.2, t0.Add(-10*time.Second))

	settled, err := p.Price(ctx, "rider-1", withQuote(req, q.QuoteID))
	if err != nil {
		t.Fatal(err)
	}
	bad := issue(c, "rider-1", int64(q.Price)+1000, "CAR", 1000, t0) // not the quoted price
	_, err = c.Apply(ctx, "rider-1", req.VehicleType, settled.Price, bad)
	var pe *PromoError
	if !errors.As(err, &pe) || pe.Code != "promo_wrong_price" {
		t.Fatalf("Apply = %v, want promo_wrong_price", err)
	}
	if !claims.used[q.QuoteID] || len(claims.released) != 0 {
		t.Fatalf("after a refused promo: used=%v released=%v; want the quote still claimed", claims.used, claims.released)
	}
	p.Release(ctx, settled) // what the caller does
	if claims.used[q.QuoteID] {
		t.Error("the quote is still claimed after the caller released it")
	}
}
