package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"grab-mini/order-service/internal/app"
	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/auth"
	"grab-mini/pkg/surge"
)

// These are the booking paths that answer before any database is touched:
// the service under the API has no ports but the pricer, so reaching one
// would panic.

// noPorts is a service with no ports at all: every request below must be
// answered by the API itself.
func noPorts() *API { return NewAPI(app.New(app.Deps{}), nil) }

// post sends a booking as the rider "r" straight to the handler.
func post(a *API, body string) *httptest.ResponseRecorder {
	return call(a.handleCreateOrder, http.MethodPost, "/orders", "", body, false, nil)
}

func TestCreateOrderRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"not JSON":              `nope`,
		"missing pickup":        `{"riderId":"r","dropoff":{"lat":10.79,"lng":106.71}}`,
		"latitude out of range": `{"riderId":"r","pickup":{"lat":91,"lng":0},"dropoff":{"lat":10.79,"lng":106.71}}`,
		"unknown vehicle type":  `{"riderId":"r",` + where + `,"vehicleType":"TRUCK"}`,
		"lowercase vehicle":     `{"riderId":"r",` + where + `,"vehicleType":"car"}`,
	}
	for name, body := range cases {
		if rec := post(noPorts(), body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
	}
}

func TestCreateOrderWithoutIdentityIsUnauthorized(t *testing.T) {
	rec := call(noPorts().handleCreateOrder, http.MethodPost, "/orders", "", `{}`, true, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status %d, want 401", rec.Code)
	}
}

func TestVehicleTypeValidation(t *testing.T) {
	rec := post(noPorts(), `{"riderId":"r","pickup":{"lat":10.776,"lng":106.7},"dropoff":{"lat":10.79,"lng":106.71},"vehicleType":"TRUCK"}`)
	if !strings.Contains(rec.Body.String(), "CAR or BIKE") {
		t.Errorf("body = %q, want a message naming the allowed types", rec.Body.String())
	}
}

func TestABadKeyIs400BeforeAnythingElse(t *testing.T) {
	rec := call(noPorts().handleCreateOrder, http.MethodPost, "/orders", "",
		`{"pickup":{"lat":10.776,"lng":106.7},"dropoff":{"lat":10.79,"lng":106.71}}`, false,
		http.Header{"Idempotency-Key": {strings.Repeat("k", 200)}}) // no database: it must not get that far
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "Idempotency-Key") {
		t.Errorf("status %d body %q, want a 400 naming the header", rec.Code, rec.Body.String())
	}
}

// --- quotes: priced before the database is touched (no Idempotency-Key) ---

var t0 = time.Unix(1_800_000_000, 0)

type fixedMultiplier float64

func (f fixedMultiplier) Multiplier(context.Context, string, string) float64 { return float64(f) }

type nopMetrics struct{}

func (nopMetrics) OrderCreated(string) {}
func (nopMetrics) OrderCancelled()     {}
func (nopMetrics) Pricing(string)      {}
func (nopMetrics) SurgeFailOpen()      {}
func (nopMetrics) Promo(string)        {}
func (nopMetrics) Idempotent(string)   {}

func testPricer(t *testing.T, current float64) *app.Pricer {
	t.Helper()
	signer, err := surge.NewSigner("test-secret")
	if err != nil {
		t.Fatal(err)
	}
	return &app.Pricer{Signer: signer, Mult: fixedMultiplier(current), Tolerance: 0.2, CellDeg: 0.05,
		Now: func() time.Time { return t0 }, Metrics: nopMetrics{}}
}

// pricingOnly is an API over a service that has a pricer and nothing else.
func pricingOnly(p *app.Pricer) *API { return NewAPI(app.New(app.Deps{Pricer: p}), nil) }

func postAs(a *API, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	req = req.WithContext(auth.With(req.Context(), auth.Identity{Subject: "rider-1", Role: auth.Rider}))
	rec := httptest.NewRecorder()
	a.handleCreateOrder(rec, req)
	return rec
}

const tripJSON = `"pickup":{"lat":10.776,"lng":106.72},"dropoff":{"lat":10.82,"lng":106.75},"vehicleType":"CAR"`

func quoteFor(p *app.Pricer, rider string, mult float64, at time.Time) surge.Quote {
	return p.Signer.Issue(surge.QuoteRequest{
		RiderID: rider, Vehicle: string(domain.VehicleCar),
		PickupLat: 10.776, PickupLng: 106.72, DropoffLat: 10.82, DropoffLng: 106.75,
	}, mult, at)
}

func TestBookingWithAMovedExpiredQuoteIs409WithANewQuote(t *testing.T) {
	p := testPricer(t, 2.0)
	q := quoteFor(p, "rider-1", 1.2, t0.Add(-5*time.Minute))

	rec := postAs(pricingOnly(p), `{`+tripJSON+`,"quoteId":"`+q.QuoteID+`"}`)
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
	rec := postAs(pricingOnly(testPricer(t, 1.0)), `{`+tripJSON+`,"quoteId":"abc.def"}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "quote") {
		t.Errorf("status %d body %s, want 400 about the quote", rec.Code, rec.Body)
	}
}
