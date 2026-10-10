package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
	"grab-mini/pkg/surge"
)

var t0 = time.Unix(1_800_000_000, 0)

type fixedMultiplier float64

func (f fixedMultiplier) Multiplier(context.Context, string, string) float64 { return float64(f) }

func testPricer(t *testing.T, current float64) *pricer {
	t.Helper()
	signer, err := surge.NewSigner("test-secret")
	if err != nil {
		t.Fatal(err)
	}
	return &pricer{signer: signer, mult: fixedMultiplier(current), tolerance: 0.2, cellDeg: 0.05, now: func() time.Time { return t0 }}
}

func tripRequest() createOrderRequest {
	return createOrderRequest{
		Pickup: &events.LatLng{Lat: 10.776, Lng: 106.72}, Dropoff: &events.LatLng{Lat: 10.82, Lng: 106.75},
		VehicleType: events.VehicleCar,
	}
}

func quoteFor(p *pricer, rider string, req createOrderRequest, mult float64, at time.Time) surge.Quote {
	return p.signer.Issue(surge.QuoteRequest{
		RiderID: rider, Vehicle: string(req.VehicleType),
		PickupLat: req.Pickup.Lat, PickupLng: req.Pickup.Lng, DropoffLat: req.Dropoff.Lat, DropoffLng: req.Dropoff.Lng,
	}, mult, at)
}

func TestAValidQuoteLocksItsPriceEvenIfTheMultiplierMoved(t *testing.T) {
	p := testPricer(t, 1.9) // the market moved on...
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.4, t0.Add(-10*time.Second))

	got, err := p.Price(context.Background(), "rider-1", req, q.QuoteID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Multiplier != 1.4 || got.Price != q.Price { // ...but the rider pays what they were shown
		t.Errorf("priced %+v, want multiplier 1.4 and price %v", got, q.Price)
	}
}

func TestNoQuoteIsPricedAtTheCurrentMultiplier(t *testing.T) {
	p := testPricer(t, 1.5)
	req := tripRequest()
	got, err := p.Price(context.Background(), "rider-1", req, "")
	if err != nil {
		t.Fatal(err)
	}
	base := surge.BaseFare("CAR", surge.HaversineKm(10.776, 106.72, 10.82, 106.75))
	if got.Multiplier != 1.5 || got.Price != surge.Price(base, 1.5) {
		t.Errorf("priced %+v, want 1.5 x base fare %v", got, base)
	}
}

func TestAnExpiredQuoteWithinToleranceIsRepricedAtTheCurrentMultiplier(t *testing.T) {
	p := testPricer(t, 1.5)
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.4, t0.Add(-2*time.Minute)) // expired, 1.4 -> 1.5 is within 0.2

	got, err := p.Price(context.Background(), "rider-1", req, q.QuoteID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Multiplier != 1.5 {
		t.Errorf("multiplier = %v, want the current 1.5", got.Multiplier)
	}
}

func TestAnExpiredQuoteThatMovedTooFarGivesANewQuoteToConfirm(t *testing.T) {
	for _, current := range []float64{1.9, 1.0} { // up or down by more than 0.2 from 1.4
		p := testPricer(t, current)
		req := tripRequest()
		q := quoteFor(p, "rider-1", req, 1.4, t0.Add(-2*time.Minute))

		_, err := p.Price(context.Background(), "rider-1", req, q.QuoteID)
		var c *conflict
		if !errors.As(err, &c) {
			t.Fatalf("current %v: err = %v, want a conflict", current, err)
		}
		if c.NewQuote.Multiplier != current || !c.NewQuote.ExpiresAt.After(t0) {
			t.Errorf("new quote = %+v", c.NewQuote)
		}
		// The new quote is good for booking.
		if _, err := p.Price(context.Background(), "rider-1", req, c.NewQuote.QuoteID); err != nil {
			t.Errorf("the new quote was refused: %v", err)
		}
	}
}

func TestTamperedForgedAndForeignQuotesAreInvalid(t *testing.T) {
	p := testPricer(t, 1.0)
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.0, t0)
	payload, sig, _ := strings.Cut(q.QuoteID, ".")

	otherTrip := tripRequest()
	otherTrip.Dropoff = &events.LatLng{Lat: 11, Lng: 107}
	other, _ := surge.NewSigner("someone-elses-secret")
	forged := other.Issue(surge.QuoteRequest{RiderID: "rider-1", Vehicle: "CAR", PickupLat: 10.776, PickupLng: 106.72, DropoffLat: 10.82, DropoffLng: 106.75}, 1.0, t0)

	cases := map[string]struct {
		rider string
		req   createOrderRequest
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
		_, err := p.Price(context.Background(), c.rider, c.req, c.id)
		if !errors.Is(err, errInvalidQuote) {
			t.Errorf("%s: err = %v, want errInvalidQuote", name, err)
		}
	}
}

// --- HTTP: these paths answer before the database is touched ---

func postAs(s *server, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	req = req.WithContext(auth.With(req.Context(), auth.Identity{Subject: "rider-1", Role: auth.Rider}))
	s.handleCreateOrder(rec, req)
	return rec
}

const tripJSON = `"pickup":{"lat":10.776,"lng":106.72},"dropoff":{"lat":10.82,"lng":106.75},"vehicleType":"CAR"`

func TestBookingWithAMovedExpiredQuoteIs409WithANewQuote(t *testing.T) {
	p := testPricer(t, 2.0)
	s := &server{pricer: p}
	q := quoteFor(p, "rider-1", tripRequest(), 1.2, t0.Add(-5*time.Minute))

	rec := postAs(s, `{`+tripJSON+`,"quoteId":"`+q.QuoteID+`"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status %d, want 409: %s", rec.Code, rec.Body)
	}
	var body struct {
		Error    string      `json:"error"`
		NewQuote surge.Quote `json:"newQuote"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.NewQuote.Multiplier != 2.0 || body.NewQuote.QuoteID == "" || body.NewQuote.Price <= 0 {
		t.Errorf("newQuote = %+v", body.NewQuote)
	}
}

func TestBookingWithATamperedQuoteIs400(t *testing.T) {
	s := &server{pricer: testPricer(t, 1.0)}
	rec := postAs(s, `{`+tripJSON+`,"quoteId":"abc.def"}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "quote") {
		t.Errorf("status %d body %s, want 400 about the quote", rec.Code, rec.Body)
	}
}

// --- Redis: the multiplier source, failing open ---

func TestRedisMultipliersReadsAndFailsOpen(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	src := redisMultipliers{rdb}
	ctx := context.Background()

	if m := src.Multiplier(ctx, "CAR", "1:2"); m != 1.0 {
		t.Errorf("missing key = %v, want 1.0", m)
	}
	rdb.Set(ctx, surge.MultKey("CAR", "1:2"), "1.7", time.Minute)
	if m := src.Multiplier(ctx, "CAR", "1:2"); m != 1.7 {
		t.Errorf("stored 1.7 read as %v", m)
	}
	if m := src.Multiplier(ctx, "BIKE", "1:2"); m != 1.0 {
		t.Errorf("another vehicle's key leaked: %v", m)
	}
	rdb.Set(ctx, surge.MultKey("CAR", "1:2"), "0.3", time.Minute)
	if m := src.Multiplier(ctx, "CAR", "1:2"); m != 1.0 {
		t.Errorf("a multiplier below 1 must be ignored, got %v", m)
	}

	mr.Close()
	if m := src.Multiplier(ctx, "CAR", "1:2"); m != 1.0 {
		t.Errorf("Redis down = %v, want 1.0 (fail open)", m)
	}
}

// --- Postgres (TEST_DATABASE_URL) ---

func TestPriceAndMultiplierArePersistedAndOldOrdersReadAsUnpriced(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	s := &server{db: db, relay: outbox.NewRelay(db, &recordingWriter{}, outboxSource)}
	price := 87000.0
	id := uuid.NewString()
	req := tripRequest()
	req.RiderID, req.surgeMultiplier, req.price = "rider-1", 1.4, &price
	if err := s.insertOrder(ctx, id, req, []byte("x")); err != nil {
		t.Fatal(err)
	}
	o, err := queryOrder(ctx, db, "id = $1", id)
	if err != nil || o.SurgeMultiplier != 1.4 || o.Price == nil || *o.Price != 87000 {
		t.Fatalf("order = %+v, %v", o, err)
	}

	// A row from before the migration: defaults, no price.
	legacy := uuid.NewString()
	if _, err := db.Exec(ctx, `INSERT INTO orders (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type, status)
		VALUES ($1, 'rider-9', 1, 2, 3, 4, 'CAR', 'PENDING')`, legacy); err != nil {
		t.Fatal(err)
	}
	o, err = queryOrder(ctx, db, "id = $1", legacy)
	if err != nil || o.SurgeMultiplier != 1.0 || o.Price != nil {
		t.Errorf("legacy order = %+v, %v; want multiplier 1.0 and null price", o, err)
	}
}
