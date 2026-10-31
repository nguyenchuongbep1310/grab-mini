package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/promo"
	"grab-mini/pkg/surge"
)

// ---- unit: verifying a promo token (miniredis for the single-use mark) ----

func testPromoChecker(t *testing.T) (*promoChecker, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	signer, err := promo.NewSigner("test-promo-secret")
	if err != nil {
		t.Fatal(err)
	}
	return &promoChecker{
		signer: signer, claims: redisClaims{redis.NewClient(&redis.Options{Addr: mr.Addr()})},
		now: func() time.Time { return t0 },
	}, mr
}

func issue(c *promoChecker, rider string, price int64, vehicle string, discount int64, at time.Time) string {
	tok, _ := c.signer.Issue("WELCOME10", promo.Binding{RiderID: rider, Price: price, Vehicle: vehicle}, discount, at)
	return tok
}

func TestAValidPromoTokenGivesItsCodeAndDiscount(t *testing.T) {
	c, _ := testPromoChecker(t)
	tok := issue(c, "rider-1", 87000, "CAR", 8700, t0.Add(-time.Minute))
	got, err := c.Apply(context.Background(), "rider-1", events.VehicleCar, 87000, tok)
	if err != nil || got.Code != "WELCOME10" || got.Discount != 8700 {
		t.Fatalf("Apply = %+v, %v", got, err)
	}
}

func TestPromoTokensThatDoNotFitTheBookingAreRefusedWithACode(t *testing.T) {
	c, _ := testPromoChecker(t)
	good := issue(c, "rider-1", 87000, "CAR", 8700, t0.Add(-time.Minute))
	payload, sig, _ := strings.Cut(good, ".")
	other, _ := promo.NewSigner("another-secret")
	foreign, _ := other.Issue("WELCOME10", promo.Binding{RiderID: "rider-1", Price: 87000, Vehicle: "CAR"}, 8700, t0)

	cases := []struct {
		name    string
		token   string
		rider   string
		vehicle events.VehicleType
		price   float64
		want    string
	}{
		{"forged signature", payload + "." + sig[:len(sig)-2] + "AA", "rider-1", events.VehicleCar, 87000, "promo_invalid"},
		{"signed with another key", foreign, "rider-1", events.VehicleCar, 87000, "promo_invalid"},
		{"not a token", "hello", "rider-1", events.VehicleCar, 87000, "promo_invalid"},
		{"expired", issue(c, "rider-1", 87000, "CAR", 8700, t0.Add(-promo.TokenTTL-time.Second)), "rider-1", events.VehicleCar, 87000, "promo_expired"},
		{"another rider's token", good, "rider-2", events.VehicleCar, 87000, "promo_wrong_rider"},
		{"another price", good, "rider-1", events.VehicleCar, 90000, "promo_wrong_price"},
		{"another vehicle", good, "rider-1", events.VehicleBike, 87000, "promo_wrong_vehicle"},
	}
	for _, tc := range cases {
		_, err := c.Apply(context.Background(), tc.rider, tc.vehicle, tc.price, tc.token)
		var pe *promoError
		if !errors.As(err, &pe) {
			t.Errorf("%s: err = %v, want a *promoError", tc.name, err)
			continue
		}
		if pe.Code != tc.want || pe.Status != http.StatusBadRequest || pe.Message == "" {
			t.Errorf("%s: %+v, want 400 %s", tc.name, pe, tc.want)
		}
	}
}

func TestAPromoTokenBooksOnceAndCanBeGivenBack(t *testing.T) {
	c, mr := testPromoChecker(t)
	ctx := context.Background()
	tok := issue(c, "rider-1", 87000, "CAR", 8700, t0)

	first, err := c.Apply(ctx, "rider-1", events.VehicleCar, 87000, tok)
	if err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL(claimKey("promo:" + tok)); ttl <= 0 || ttl > promo.TokenTTL+2*time.Second {
		t.Errorf("the mark lives %v, want the token's remaining life", ttl)
	}
	_, err = c.Apply(ctx, "rider-1", events.VehicleCar, 87000, tok)
	var pe *promoError
	if !errors.As(err, &pe) || pe.Code != "promo_used" || pe.Status != http.StatusConflict {
		t.Fatalf("second use = %v, want 409 promo_used", err)
	}
	c.release(ctx, first) // the booking failed after the check
	if _, err := c.Apply(ctx, "rider-1", events.VehicleCar, 87000, tok); err != nil {
		t.Errorf("after a failed booking gave it back: %v", err)
	}
}

// Fail-open policy: with Redis down the token can't be marked used, but it is signed and
// short-lived and Promotion Service enforces the limits, so the booking is not turned away.
func TestWithRedisDownThePromoIsStillApplied(t *testing.T) {
	c, mr := testPromoChecker(t)
	tok := issue(c, "rider-1", 87000, "CAR", 8700, t0)
	mr.Close()
	got, err := c.Apply(context.Background(), "rider-1", events.VehicleCar, 87000, tok)
	if err != nil || got.Discount != 8700 {
		t.Fatalf("Apply = %+v, %v; want the discount applied", got, err)
	}
	if got.claimed != "" {
		t.Error("claimed a token it could not mark")
	}
}

func TestPromoTokenParticipatesInTheIdempotencyFingerprint(t *testing.T) {
	a, b := tripRequest(), tripRequest()
	if fingerprint(a) != fingerprint(b) {
		t.Fatal("identical requests differ")
	}
	b.PromoToken = "some.token"
	if fingerprint(a) == fingerprint(b) {
		t.Error("a booking with a promo token has the fingerprint of one without")
	}
}

// ---- unit: the base fare of a quote survives a re-pricing (design 4e) ----

// routedQuote is a quote whose base fare comes from a distance longer than the
// straight line, as Pricing Service issues them from Routing Service's answer.
func routedQuote(p *pricer, rider string, req createOrderRequest, km, mult float64, at time.Time) surge.Quote {
	return p.signer.IssueForDistance(surge.QuoteRequest{
		RiderID: rider, Vehicle: string(req.VehicleType),
		PickupLat: req.Pickup.Lat, PickupLng: req.Pickup.Lng, DropoffLat: req.Dropoff.Lat, DropoffLng: req.Dropoff.Lng,
	}, km, mult, at)
}

func straightBase(req createOrderRequest) float64 {
	return surge.BaseFare(string(req.VehicleType), surge.HaversineKm(req.Pickup.Lat, req.Pickup.Lng, req.Dropoff.Lat, req.Dropoff.Lng))
}

func TestAnExpiredQuoteIsRepricedOnItsOwnBaseFare(t *testing.T) {
	p := testPricer(t, 1.5)
	req := tripRequest()
	q := routedQuote(p, "rider-1", req, 30, 1.4, t0.Add(-2*time.Minute)) // expired; 1.4 -> 1.5 is within tolerance
	if q.BaseFare == straightBase(req) {
		t.Fatal("test set-up: the routed fare must differ from the straight-line one")
	}

	got, err := p.Price(context.Background(), "rider-1", req, q.QuoteID)
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
		p := testPricer(t, current)
		req := tripRequest()
		q := routedQuote(p, "rider-1", req, 30, 1.4, t0.Add(-2*time.Minute))

		_, err := p.Price(context.Background(), "rider-1", req, q.QuoteID)
		var c *conflict
		if !errors.As(err, &c) {
			t.Fatalf("current %v: err = %v, want a conflict", current, err)
		}
		if c.NewQuote.BaseFare != q.BaseFare || c.NewQuote.Price != surge.Price(q.BaseFare, current) {
			t.Errorf("current %v: new quote %+v, want base fare %v and price %v", current, c.NewQuote, q.BaseFare, surge.Price(q.BaseFare, current))
		}
		// Confirming it books at exactly that price.
		got, err := p.Price(context.Background(), "rider-1", req, c.NewQuote.QuoteID)
		if err != nil || got.Price != c.NewQuote.Price {
			t.Errorf("current %v: booking the new quote = %+v, %v; want %v", current, got, err, c.NewQuote.Price)
		}
	}
}

func TestTheReplacementOfAReusedQuoteKeepsTheBaseFareToo(t *testing.T) {
	p, _ := claimingPricer(t, 1.5)
	req := tripRequest()
	q := routedQuote(p, "rider-1", req, 30, 1.2, t0.Add(-10*time.Second))
	ctx := context.Background()
	if _, err := p.Price(ctx, "rider-1", req, q.QuoteID); err != nil {
		t.Fatal(err)
	}
	_, err := p.Price(ctx, "rider-1", req, q.QuoteID)
	var c *conflict
	if !errors.As(err, &c) {
		t.Fatalf("err = %v, want a conflict", err)
	}
	if c.NewQuote.BaseFare != q.BaseFare || c.NewQuote.Price != surge.Price(q.BaseFare, 1.5) {
		t.Errorf("new quote %+v, want base fare %v at 1.5", c.NewQuote, q.BaseFare)
	}
}

func TestWithRedisDownTheRepricingKeepsTheQuotesBaseFare(t *testing.T) {
	p, mr := claimingPricer(t, 1.5)
	req := tripRequest()
	q := routedQuote(p, "rider-1", req, 30, 1.2, t0.Add(-10*time.Second))
	mr.Close()
	got, err := p.Price(context.Background(), "rider-1", req, q.QuoteID)
	if err != nil {
		t.Fatal(err)
	}
	if want := surge.Price(q.BaseFare, 1.5); got.Price != want {
		t.Errorf("priced %v, want %v on the quote's base fare", got.Price, want)
	}
}

func TestWithoutAQuoteTheStraightLineIsTheFallback(t *testing.T) {
	p := testPricer(t, 1.5)
	req := tripRequest()
	got, err := p.Price(context.Background(), "rider-1", req, "")
	if err != nil {
		t.Fatal(err)
	}
	if want := surge.Price(straightBase(req), 1.5); got.Price != want {
		t.Errorf("priced %v, want %v", got.Price, want)
	}
}

func TestAQuoteForAnotherTripDoesNotLendItsBaseFare(t *testing.T) {
	p := testPricer(t, 1.0)
	req := tripRequest()
	elsewhere := tripRequest()
	elsewhere.Dropoff = &events.LatLng{Lat: 11.5, Lng: 107.0}
	q := routedQuote(p, "rider-1", elsewhere, 90, 1.0, t0.Add(-2*time.Minute)) // expired, and for another trip
	if _, err := p.Price(context.Background(), "rider-1", req, q.QuoteID); !errors.Is(err, errInvalidQuote) {
		t.Errorf("err = %v, want errInvalidQuote", err)
	}
}

// ---- integration: booking with a promo token over a real database ----

func (a *api) token(rider string, price int64, vehicle string, discount int64, at time.Time) string {
	tok, _ := a.promos.signer.Issue("WELCOME10", promo.Binding{RiderID: rider, Price: price, Vehicle: vehicle}, discount, at)
	return tok
}

// bookingPrice is what bookingBody costs with no surge and no quote.
func bookingPrice() int64 {
	return int64(surge.Price(straightBase(tripRequest()), 1.0))
}

func withToken(token string) string {
	return strings.TrimSuffix(bookingBody, "}") + `,"promoToken":"` + token + `"}`
}

func (a *api) orderRow(orderID string) (code string, discount int64) {
	a.t.Helper()
	if err := a.db.QueryRow(context.Background(), `SELECT promo_code, discount FROM orders WHERE id = $1`, orderID).Scan(&code, &discount); err != nil {
		a.t.Fatal(err)
	}
	return
}

func TestBookingWithAPromoLocksTheDiscountOnTheOrderAndTheEvent(t *testing.T) {
	a := newAPI(t)
	price := bookingPrice()
	discount := price / 10
	got := a.book("rider-1", "", withToken(a.token("rider-1", price, "CAR", discount, t0.Add(-time.Minute))))
	if got.Code != http.StatusCreated {
		t.Fatalf("booking = %d %s", got.Code, got.Raw)
	}
	if got.Body["discount"] != float64(discount) || got.Body["finalPrice"] != float64(price-discount) ||
		got.Body["price"] != float64(price) || got.Body["promoCode"] != "WELCOME10" {
		t.Errorf("response = %s", got.Raw)
	}
	if code, d := a.orderRow(got.OrderID); code != "WELCOME10" || d != discount {
		t.Errorf("order row = %q %d", code, d)
	}

	// order.created carries them, as the Avro schema v4 fields.
	var payload []byte
	if err := a.db.QueryRow(context.Background(), `SELECT payload FROM outbox WHERE topic = 'order.created'`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var evt events.OrderCreated
	if err := a.codec.Decode(context.Background(), events.TopicOrderCreated, payload, &evt); err != nil {
		t.Fatal(err)
	}
	if evt.PromoCode != "WELCOME10" || evt.Discount != discount || evt.Price == nil || *evt.Price != float64(price) {
		t.Errorf("order.created = %+v", evt)
	}

	// And the order reads back with them.
	tok, _, _ := a.issuer.Issue(auth.Rider, "rider-1")
	req := httptest.NewRequest(http.MethodGet, "/orders/"+got.OrderID, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	a.mux.ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), `"discount":`) || !strings.Contains(rec.Body.String(), `"promoCode":"WELCOME10"`) ||
		!strings.Contains(rec.Body.String(), `"finalPrice"`) {
		t.Errorf("GET /orders/{id} = %s", rec.Body)
	}
}

func TestABookingWithoutAPromoHasNoDiscount(t *testing.T) {
	a := newAPI(t)
	got := a.book("rider-1", "", bookingBody)
	if got.Code != http.StatusCreated || got.Body["discount"] != float64(0) || got.Body["finalPrice"] != got.Body["price"] {
		t.Fatalf("booking = %d %s", got.Code, got.Raw)
	}
	if _, has := got.Body["promoCode"]; has {
		t.Errorf("promoCode in %s", got.Raw)
	}
	if code, d := a.orderRow(got.OrderID); code != "" || d != 0 {
		t.Errorf("order row = %q %d", code, d)
	}
}

func TestAFullDiscountIsAFreeRide(t *testing.T) {
	a := newAPI(t)
	price := bookingPrice()
	got := a.book("rider-1", "", withToken(a.token("rider-1", price, "CAR", price, t0)))
	if got.Code != http.StatusCreated || got.Body["finalPrice"] != float64(0) || got.Body["discount"] != float64(price) {
		t.Errorf("booking = %d %s", got.Code, got.Raw)
	}
}

func TestBookingRefusesPromoTokensThatDoNotBelongToIt(t *testing.T) {
	a := newAPI(t)
	price := bookingPrice()
	good := a.token("rider-1", price, "CAR", price/10, t0.Add(-time.Minute))
	payload, sig, _ := strings.Cut(good, ".")
	cases := []struct {
		name, rider, token, want string
	}{
		{"forged", "rider-1", payload + "." + sig[:len(sig)-2] + "AA", "promo_invalid"},
		{"expired", "rider-1", a.token("rider-1", price, "CAR", price/10, t0.Add(-time.Hour)), "promo_expired"},
		{"another rider's", "rider-2", good, "promo_wrong_rider"},
		{"for another price", "rider-1", a.token("rider-1", price+1000, "CAR", price/10, t0), "promo_wrong_price"},
		{"for another vehicle", "rider-1", a.token("rider-1", price, "BIKE", price/10, t0), "promo_wrong_vehicle"},
	}
	for _, tc := range cases {
		got := a.book(tc.rider, "", withToken(tc.token))
		if got.Code != http.StatusBadRequest || got.Body["code"] != tc.want || got.Body["error"] == "" {
			t.Errorf("%s: %d %s, want 400 %s", tc.name, got.Code, got.Raw, tc.want)
		}
	}
	if n := a.count("orders") + a.count("outbox"); n != 0 {
		t.Errorf("%d rows saved by refused bookings", n)
	}
}

func TestARefusedPromoGivesTheQuoteBack(t *testing.T) {
	a := newAPI(t)
	q := quoteFor(a.pricer, "rider-1", tripRequest(), 1.2, t0.Add(-10*time.Second))
	bad := a.token("rider-1", int64(q.Price)+1000, "CAR", 1000, t0) // not the quoted price
	body := strings.TrimSuffix(bookingBody, "}") + `,"quoteId":"` + q.QuoteID + `","promoToken":"` + bad + `"}`
	if got := a.book("rider-1", "", body); got.Code != http.StatusBadRequest || got.Body["code"] != "promo_wrong_price" {
		t.Fatalf("booking = %d %s", got.Code, got.Raw)
	}
	// The rider fixes the promo and books with the same quote.
	good := a.token("rider-1", int64(q.Price), "CAR", 1000, t0)
	body = strings.TrimSuffix(bookingBody, "}") + `,"quoteId":"` + q.QuoteID + `","promoToken":"` + good + `"}`
	got := a.book("rider-1", "", body)
	if got.Code != http.StatusCreated || got.Body["price"] != q.Price || got.Body["finalPrice"] != q.Price-1000 {
		t.Errorf("retry = %d %s, want 201 at the quoted price less 1000", got.Code, got.Raw)
	}
}

func TestAPromoTokenBooksOnlyOnce(t *testing.T) {
	a := newAPI(t)
	price := bookingPrice()
	body := withToken(a.token("rider-1", price, "CAR", price/10, t0))
	if first := a.book("rider-1", "", body); first.Code != http.StatusCreated {
		t.Fatalf("first = %d %s", first.Code, first.Raw)
	}
	second := a.book("rider-1", "", body)
	if second.Code != http.StatusConflict || second.Body["code"] != "promo_used" {
		t.Errorf("second = %d %s, want 409 promo_used", second.Code, second.Raw)
	}
	if n := a.count("orders"); n != 1 {
		t.Errorf("%d orders, want 1", n)
	}
}

func TestAnIdempotentRetryOfAPromoBookingReplaysIt(t *testing.T) {
	a := newAPI(t)
	price := bookingPrice()
	body := withToken(a.token("rider-1", price, "CAR", price/10, t0))
	first := a.book("rider-1", "k", body)
	retry := a.book("rider-1", "k", body)
	if first.Code != 201 || retry.Code != 201 || retry.OrderID != first.OrderID || !retry.Replay {
		t.Fatalf("first %+v retry %+v; want the stored response, not a 409 promo_used", first, retry)
	}
	// The same key with another token is another request.
	other := withToken(a.token("rider-1", price, "CAR", price/5, t0))
	if got := a.book("rider-1", "k", other); got.Code != http.StatusUnprocessableEntity {
		t.Errorf("same key, other token = %d %s, want 422", got.Code, got.Raw)
	}
}

func TestAPromoWhoseBookingFailsIsGivenBack(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	price := bookingPrice()
	body := withToken(a.token("rider-1", price, "CAR", price/10, t0))

	a.db.Exec(ctx, `ALTER TABLE outbox RENAME TO outbox_off`)
	failed := a.book("rider-1", "", body)
	a.db.Exec(ctx, `ALTER TABLE outbox_off RENAME TO outbox`)
	if failed.Code != http.StatusInternalServerError {
		t.Fatalf("booking with a broken outbox = %d %s, want 500", failed.Code, failed.Raw)
	}
	if retry := a.book("rider-1", "", body); retry.Code != http.StatusCreated {
		t.Errorf("retry = %d %s, want 201: the token must survive a failed booking", retry.Code, retry.Raw)
	}
}
