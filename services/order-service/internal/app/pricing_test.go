package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/surge"
)

var t0 = time.Unix(1_800_000_000, 0)

func testPricer(t *testing.T, current float64) (*Pricer, *fakeMetrics) {
	t.Helper()
	signer, err := surge.NewSigner("test-secret")
	if err != nil {
		t.Fatal(err)
	}
	m := newFakeMetrics()
	return &Pricer{Signer: signer, Mult: fixedMultiplier(current), Tolerance: 0.2, CellDeg: 0.05,
		Now: func() time.Time { return t0 }, Metrics: m}, m
}

func claimingPricer(t *testing.T, current float64) (*Pricer, *fakeClaims, *fakeMetrics) {
	t.Helper()
	p, m := testPricer(t, current)
	c := newFakeClaims()
	p.Claims = c
	return p, c, m
}

func tripRequest() domain.BookingRequest {
	return domain.BookingRequest{
		Pickup: &domain.LatLng{Lat: 10.776, Lng: 106.72}, Dropoff: &domain.LatLng{Lat: 10.82, Lng: 106.75},
		VehicleType: domain.VehicleCar,
	}
}

func withQuote(req domain.BookingRequest, id string) domain.BookingRequest {
	req.QuoteID = id
	return req
}

func qrOf(rider string, req domain.BookingRequest) surge.QuoteRequest {
	return surge.QuoteRequest{
		RiderID: rider, Vehicle: string(req.VehicleType),
		PickupLat: req.Pickup.Lat, PickupLng: req.Pickup.Lng, DropoffLat: req.Dropoff.Lat, DropoffLng: req.Dropoff.Lng,
	}
}

func quoteFor(p *Pricer, rider string, req domain.BookingRequest, mult float64, at time.Time) surge.Quote {
	return p.Signer.Issue(qrOf(rider, req), mult, at)
}

// routedQuote is a quote whose base fare comes from a distance longer than the
// straight line, as Pricing Service issues them from Routing Service's answer.
func routedQuote(p *Pricer, rider string, req domain.BookingRequest, km, mult float64, at time.Time) surge.Quote {
	return p.Signer.IssueForDistance(qrOf(rider, req), km, mult, at)
}

func straightBase(req domain.BookingRequest) float64 {
	return surge.BaseFare(string(req.VehicleType), surge.HaversineKm(req.Pickup.Lat, req.Pickup.Lng, req.Dropoff.Lat, req.Dropoff.Lng))
}

func TestAValidQuoteLocksItsPriceEvenIfTheMultiplierMoved(t *testing.T) {
	p, m := testPricer(t, 1.9) // the market moved on...
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.4, t0.Add(-10*time.Second))

	got, err := p.Price(context.Background(), "rider-1", withQuote(req, q.QuoteID))
	if err != nil {
		t.Fatal(err)
	}
	if got.Multiplier != 1.4 || got.Price != q.Price { // ...but the rider pays what they were shown
		t.Errorf("priced %+v, want multiplier 1.4 and price %v", got, q.Price)
	}
	if m.pricing["quote"] != 1 {
		t.Errorf("pricing metrics = %v, want one quote", m.pricing)
	}
}

func TestNoQuoteIsPricedAtTheCurrentMultiplier(t *testing.T) {
	p, m := testPricer(t, 1.5)
	got, err := p.Price(context.Background(), "rider-1", tripRequest())
	if err != nil {
		t.Fatal(err)
	}
	base := surge.BaseFare("CAR", surge.HaversineKm(10.776, 106.72, 10.82, 106.75))
	if got.Multiplier != 1.5 || got.Price != surge.Price(base, 1.5) {
		t.Errorf("priced %+v, want 1.5 x base fare %v", got, base)
	}
	if m.pricing["repriced"] != 1 {
		t.Errorf("pricing metrics = %v, want one repriced", m.pricing)
	}
}

func TestAnExpiredQuoteWithinToleranceIsRepricedAtTheCurrentMultiplier(t *testing.T) {
	p, _ := testPricer(t, 1.5)
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.4, t0.Add(-2*time.Minute)) // expired, 1.4 -> 1.5 is within 0.2

	got, err := p.Price(context.Background(), "rider-1", withQuote(req, q.QuoteID))
	if err != nil {
		t.Fatal(err)
	}
	if got.Multiplier != 1.5 {
		t.Errorf("multiplier = %v, want the current 1.5", got.Multiplier)
	}
}

func TestAnExpiredQuoteThatMovedTooFarGivesANewQuoteToConfirm(t *testing.T) {
	for _, current := range []float64{1.9, 1.0} { // up or down by more than 0.2 from 1.4
		p, m := testPricer(t, current)
		req := tripRequest()
		q := quoteFor(p, "rider-1", req, 1.4, t0.Add(-2*time.Minute))

		_, err := p.Price(context.Background(), "rider-1", withQuote(req, q.QuoteID))
		var c *QuoteConflict
		if !errors.As(err, &c) {
			t.Fatalf("current %v: err = %v, want a conflict", current, err)
		}
		if c.NewQuote.Multiplier != current || !c.NewQuote.ExpiresAt.After(t0) {
			t.Errorf("new quote = %+v", c.NewQuote)
		}
		if m.pricing["requote"] != 1 {
			t.Errorf("pricing metrics = %v, want one requote", m.pricing)
		}
		// The new quote is good for booking.
		if _, err := p.Price(context.Background(), "rider-1", withQuote(req, c.NewQuote.QuoteID)); err != nil {
			t.Errorf("the new quote was refused: %v", err)
		}
	}
}

func TestTamperedForgedAndForeignQuotesAreInvalid(t *testing.T) {
	p, m := testPricer(t, 1.0)
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.0, t0)
	payload, sig, _ := strings.Cut(q.QuoteID, ".")

	otherTrip := tripRequest()
	otherTrip.Dropoff = &domain.LatLng{Lat: 11, Lng: 107}
	other, _ := surge.NewSigner("someone-elses-secret")
	forged := other.Issue(qrOf("rider-1", req), 1.0, t0)

	cases := map[string]struct {
		rider string
		req   domain.BookingRequest
		id    string
	}{
		"cheaper multiplier edited in": {"rider-1", req, payload[:len(payload)-3] + "AAA." + sig},
		"signature cut":                {"rider-1", req, payload + "." + sig[:10]},
		"garbage":                      {"rider-1", req, "free-ride"},
		"signed with another key":      {"rider-1", req, forged.QuoteID},
		"another rider's quote":        {"rider-2", req, q.QuoteID},
		"quote for another trip":       {"rider-1", otherTrip, q.QuoteID},
	}
	for name, c := range cases {
		_, err := p.Price(context.Background(), c.rider, withQuote(c.req, c.id))
		if !errors.Is(err, ErrInvalidQuote) {
			t.Errorf("%s: err = %v, want ErrInvalidQuote", name, err)
		}
	}
	if m.pricing["invalid_quote"] != len(cases) {
		t.Errorf("pricing metrics = %v, want %d invalid_quote", m.pricing, len(cases))
	}
}

func TestAQuoteBooksOnceAndThenGetsAFreshQuote(t *testing.T) {
	p, claims, m := claimingPricer(t, 1.5)
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.2, t0.Add(-10*time.Second))
	ctx := context.Background()

	first, err := p.Price(ctx, "rider-1", withQuote(req, q.QuoteID))
	if err != nil || first.Multiplier != 1.2 || first.ClaimedQuote != q.QuoteID {
		t.Fatalf("first booking = %+v, %v; want the quoted 1.2x", first, err)
	}
	if ttl := claims.ttls[q.QuoteID]; ttl <= 0 || ttl > 60*time.Second {
		t.Errorf("the mark lives %v, want the quote's remaining life (about 50s)", ttl)
	}

	_, err = p.Price(ctx, "rider-1", withQuote(req, q.QuoteID))
	var again *QuoteConflict
	if !errors.As(err, &again) {
		t.Fatalf("second booking err = %v, want a *QuoteConflict", err)
	}
	if again.NewQuote.Multiplier != 1.5 || again.NewQuote.QuoteID == q.QuoteID || again.Error() != ReasonQuoteUsed {
		t.Errorf("conflict = %+v (%q), want a fresh quote at the current 1.5x and the reuse reason", again.NewQuote, again.Error())
	}
	if m.pricing["quote"] != 1 || m.pricing["quote_reused"] != 1 {
		t.Errorf("pricing metrics = %v", m.pricing)
	}
}

func TestAQuoteGivenBackCanBookAgain(t *testing.T) {
	p, _, _ := claimingPricer(t, 1.0)
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.2, t0.Add(-10*time.Second))
	ctx := context.Background()

	settled, err := p.Price(ctx, "rider-1", withQuote(req, q.QuoteID))
	if err != nil {
		t.Fatal(err)
	}
	p.Release(ctx, settled) // the booking failed after pricing
	if _, err := p.Price(ctx, "rider-1", withQuote(req, q.QuoteID)); err != nil {
		t.Errorf("retry after a failed booking = %v, want the quote honoured again", err)
	}
}

// Fail-open policy: with Redis down a quote can't be checked, so the booking
// is not rejected; it is priced at the current multiplier, never at the old quote.
func TestWithRedisDownABookingIsRepricedNotRejected(t *testing.T) {
	p, claims, m := claimingPricer(t, 1.5)
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.2, t0.Add(-10*time.Second))
	claims.down = true

	got, err := p.Price(context.Background(), "rider-1", withQuote(req, q.QuoteID))
	if err != nil {
		t.Fatalf("err = %v, want the booking to go ahead", err)
	}
	if got.ClaimedQuote != "" {
		t.Error("claimed a quote it could not mark")
	}
	// The multiplier here is the test's fixed 1.5, standing for "what Redis last said".
	if got.Multiplier != 1.5 {
		t.Errorf("multiplier = %v, want the current 1.5 and not the quoted 1.2", got.Multiplier)
	}
	if m.pricing["quote_unchecked"] != 1 {
		t.Errorf("pricing metrics = %v, want quote_unchecked", m.pricing)
	}
}

func TestAClaimIsReleasedEvenWhenTheRequestContextIsCancelled(t *testing.T) {
	p, claims, _ := claimingPricer(t, 1.0)
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.2, t0.Add(-10*time.Second))
	ctx, cancel := context.WithCancel(context.Background())

	settled, err := p.Price(ctx, "rider-1", withQuote(req, q.QuoteID))
	if err != nil {
		t.Fatal(err)
	}
	cancel()                // the client went away while the booking was failing
	p.Release(ctx, settled) // detaching from the cancelled context is the claim adapter's job (Task 5)

	if len(claims.released) != 1 || claims.released[0] != q.QuoteID {
		t.Fatalf("released = %v, want the quote id", claims.released)
	}
}

// ---- the base fare of a quote survives a re-pricing (design 4e) ----

func TestAnExpiredQuoteIsRepricedOnItsOwnBaseFare(t *testing.T) {
	p, _ := testPricer(t, 1.5)
	req := tripRequest()
	q := routedQuote(p, "rider-1", req, 30, 1.4, t0.Add(-2*time.Minute)) // expired; 1.4 -> 1.5 is within tolerance
	if q.BaseFare == straightBase(req) {
		t.Fatal("test set-up: the routed fare must differ from the straight-line one")
	}

	got, err := p.Price(context.Background(), "rider-1", withQuote(req, q.QuoteID))
	if err != nil {
		t.Fatal(err)
	}
	if want := surge.Price(q.BaseFare, 1.5); got.Price != want || got.Multiplier != 1.5 {
		t.Errorf("priced %+v, want the quote's base fare %v at 1.5 = %v (not the straight line's %v)",
			got, q.BaseFare, want, surge.Price(straightBase(req), 1.5))
	}
}

func TestTheReplacementQuoteOfA409KeepsTheBaseFareAndBooksAtThatPrice(t *testing.T) {
	for _, current := range []float64{1.9, 1.0} {
		p, _ := testPricer(t, current)
		req := tripRequest()
		q := routedQuote(p, "rider-1", req, 30, 1.4, t0.Add(-2*time.Minute))

		_, err := p.Price(context.Background(), "rider-1", withQuote(req, q.QuoteID))
		var c *QuoteConflict
		if !errors.As(err, &c) {
			t.Fatalf("current %v: err = %v, want a conflict", current, err)
		}
		if c.NewQuote.BaseFare != q.BaseFare || c.NewQuote.Price != surge.Price(q.BaseFare, current) {
			t.Errorf("current %v: new quote %+v, want base fare %v and price %v", current, c.NewQuote, q.BaseFare, surge.Price(q.BaseFare, current))
		}
		// Confirming it books at exactly that price.
		got, err := p.Price(context.Background(), "rider-1", withQuote(req, c.NewQuote.QuoteID))
		if err != nil || got.Price != c.NewQuote.Price {
			t.Errorf("current %v: booking the new quote = %+v, %v; want %v", current, got, err, c.NewQuote.Price)
		}
	}
}

func TestTheReplacementOfAReusedQuoteKeepsTheBaseFareToo(t *testing.T) {
	p, _, _ := claimingPricer(t, 1.5)
	req := tripRequest()
	q := routedQuote(p, "rider-1", req, 30, 1.2, t0.Add(-10*time.Second))
	ctx := context.Background()
	if _, err := p.Price(ctx, "rider-1", withQuote(req, q.QuoteID)); err != nil {
		t.Fatal(err)
	}
	_, err := p.Price(ctx, "rider-1", withQuote(req, q.QuoteID))
	var c *QuoteConflict
	if !errors.As(err, &c) {
		t.Fatalf("err = %v, want a conflict", err)
	}
	if c.NewQuote.BaseFare != q.BaseFare || c.NewQuote.Price != surge.Price(q.BaseFare, 1.5) {
		t.Errorf("new quote %+v, want base fare %v at 1.5", c.NewQuote, q.BaseFare)
	}
}

func TestWithRedisDownTheRepricingKeepsTheQuotesBaseFare(t *testing.T) {
	p, claims, _ := claimingPricer(t, 1.5)
	req := tripRequest()
	q := routedQuote(p, "rider-1", req, 30, 1.2, t0.Add(-10*time.Second))
	claims.down = true
	got, err := p.Price(context.Background(), "rider-1", withQuote(req, q.QuoteID))
	if err != nil {
		t.Fatal(err)
	}
	if want := surge.Price(q.BaseFare, 1.5); got.Price != want {
		t.Errorf("priced %v, want %v on the quote's base fare", got.Price, want)
	}
}

func TestWithoutAQuoteTheStraightLineIsTheFallback(t *testing.T) {
	p, _ := testPricer(t, 1.5)
	req := tripRequest()
	got, err := p.Price(context.Background(), "rider-1", req)
	if err != nil {
		t.Fatal(err)
	}
	if want := surge.Price(straightBase(req), 1.5); got.Price != want {
		t.Errorf("priced %v, want %v", got.Price, want)
	}
}

func TestAQuoteForAnotherTripDoesNotLendItsBaseFare(t *testing.T) {
	p, _ := testPricer(t, 1.0)
	req := tripRequest()
	elsewhere := tripRequest()
	elsewhere.Dropoff = &domain.LatLng{Lat: 11.5, Lng: 107.0}
	q := routedQuote(p, "rider-1", elsewhere, 90, 1.0, t0.Add(-2*time.Minute)) // expired, and for another trip
	if _, err := p.Price(context.Background(), "rider-1", withQuote(req, q.QuoteID)); !errors.Is(err, ErrInvalidQuote) {
		t.Errorf("err = %v, want ErrInvalidQuote", err)
	}
}
