package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/promo"
	"grab-mini/pkg/surge"
)

// ---- integration: booking with a promo token over a real database ----

func (a *api) token(rider string, price int64, vehicle string, discount int64, at time.Time) string {
	tok, _ := a.promos.Signer.Issue("WELCOME10", promo.Binding{RiderID: rider, Price: price, Vehicle: vehicle}, discount, at)
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
