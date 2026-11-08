package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"grab-mini/order-service/internal/app"
	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/auth"
	"grab-mini/pkg/auth/authtest"
	"grab-mini/pkg/surge"
)

// These tests pin what clients receive, byte for byte. The expected values are
// the ones recorded from the flat code in services/order-service/golden_test.go.

const where = `"pickup":{"lat":10.776,"lng":106.7},"dropoff":{"lat":10.79,"lng":106.71}`

// call sends one request straight to a handler as the rider "r" (or with no
// identity), so nothing reaches a use case unless the row is meant to.
func call(h http.HandlerFunc, method, path, id, body string, noIdentity bool, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if id != "" {
		req.SetPathValue("id", id)
	}
	if !noIdentity {
		req = req.WithContext(auth.With(req.Context(), auth.Identity{Subject: "r", Role: auth.Rider}))
	}
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func TestErrorResponsesAreStable(t *testing.T) {
	// A service with no ports: every row below is answered before one is used.
	a := NewAPI(app.New(app.Deps{}), nil)
	const (
		jsonCT = "application/json"
		textCT = "text/plain; charset=utf-8"
	)
	hdr := func(kv ...string) http.Header {
		h := http.Header{}
		for i := 0; i < len(kv); i += 2 {
			h.Add(kv[i], kv[i+1])
		}
		return h
	}
	valid := `{"riderId":"r",` + where + `}`
	const someOrder = "00000000-0000-0000-0000-000000000001"
	cases := []struct {
		name       string
		handler    http.HandlerFunc
		method     string
		path, id   string
		body       string
		noIdentity bool
		header     http.Header
	}{
		{name: "create without identity", handler: a.handleCreateOrder, method: "POST", path: "/orders", body: `{}`, noIdentity: true},
		{name: "create naming another rider", handler: a.handleCreateOrder, method: "POST", path: "/orders", body: `{"riderId":"someone-else",` + where + `}`},
		{name: "invalid JSON", handler: a.handleCreateOrder, method: "POST", path: "/orders", body: `nope`},
		{name: "empty Idempotency-Key", handler: a.handleCreateOrder, method: "POST", path: "/orders", body: valid, header: hdr("Idempotency-Key", "")},
		{name: "129 character Idempotency-Key", handler: a.handleCreateOrder, method: "POST", path: "/orders", body: valid, header: hdr("Idempotency-Key", strings.Repeat("k", 129))},
		{name: "control character in Idempotency-Key", handler: a.handleCreateOrder, method: "POST", path: "/orders", body: valid, header: hdr("Idempotency-Key", "a\x01b")},
		{name: "two Idempotency-Key headers", handler: a.handleCreateOrder, method: "POST", path: "/orders", body: valid, header: hdr("Idempotency-Key", "a", "Idempotency-Key", "b")},
		{name: "missing pickup", handler: a.handleCreateOrder, method: "POST", path: "/orders", body: `{"riderId":"r","dropoff":{"lat":10.79,"lng":106.71}}`},
		{name: "latitude 91", handler: a.handleCreateOrder, method: "POST", path: "/orders", body: `{"riderId":"r","pickup":{"lat":91,"lng":0},"dropoff":{"lat":10.79,"lng":106.71}}`},
		{name: "unknown vehicle type", handler: a.handleCreateOrder, method: "POST", path: "/orders", body: `{"riderId":"r",` + where + `,"vehicleType":"TRUCK"}`},
		{name: "lowercase vehicle type", handler: a.handleCreateOrder, method: "POST", path: "/orders", body: `{"riderId":"r",` + where + `,"vehicleType":"car"}`},
		{name: "get a malformed order id", handler: a.handleGetOrder, method: "GET", path: "/orders/not-a-uuid", id: "not-a-uuid"},
		{name: "cancel a malformed order id", handler: a.handleCancelOrder, method: "POST", path: "/orders/not-a-uuid/cancel", id: "not-a-uuid", body: `{}`},
		{name: "cancel reason of 201 runes", handler: a.handleCancelOrder, method: "POST", path: "/orders/" + someOrder + "/cancel", id: someOrder, body: `{"reason":"` + strings.Repeat("é", 201) + `"}`},
		{name: "latest order of another rider", handler: a.handleGetLatestOrder, method: "GET", path: "/riders/someone-else/latest-order", id: "someone-else"},
	}
	idem := `{"error":"Idempotency-Key must be 1 to 128 printable ASCII characters"}` + "\n"
	pickup := `{"error":"pickup {lat,lng} and dropoff {lat,lng} are required"}` + "\n"
	vehicle := `{"error":"vehicleType must be CAR or BIKE"}` + "\n"
	notFound := `{"error":"order not found"}` + "\n"
	expect := map[string]struct {
		status int
		ct     string
		body   string
	}{
		"create without identity":              {401, jsonCT, `{"error":"missing bearer token"}` + "\n"},
		"create naming another rider":          {403, jsonCT, `{"error":"riderId doesn't match the token"}` + "\n"},
		"invalid JSON":                         {400, textCT, `{"error":"invalid JSON body"}` + "\n"},
		"empty Idempotency-Key":                {400, textCT, idem},
		"129 character Idempotency-Key":        {400, textCT, idem},
		"control character in Idempotency-Key": {400, textCT, idem},
		"two Idempotency-Key headers":          {400, textCT, idem},
		"missing pickup":                       {400, textCT, pickup},
		"latitude 91":                          {400, textCT, pickup},
		"unknown vehicle type":                 {400, textCT, vehicle},
		"lowercase vehicle type":               {400, textCT, vehicle},
		"get a malformed order id":             {404, textCT, notFound},
		"cancel a malformed order id":          {404, textCT, notFound},
		"cancel reason of 201 runes":           {400, textCT, `{"error":"reason is at most 200 characters"}` + "\n"},
		"latest order of another rider":        {403, jsonCT, `{"error":"you can only read your own orders"}` + "\n"},
	}
	for _, c := range cases {
		want, ok := expect[c.name]
		if !ok {
			t.Fatalf("%s: no expectation recorded", c.name)
		}
		rec := call(c.handler, c.method, c.path, c.id, c.body, c.noIdentity, c.header)
		if rec.Code != want.status {
			t.Errorf("%s: status = %d, want %d", c.name, rec.Code, want.status)
		}
		if got := rec.Header().Get("Content-Type"); got != want.ct {
			t.Errorf("%s: Content-Type = %q, want %q", c.name, got, want.ct)
		}
		if got := rec.Body.String(); got != want.body {
			t.Errorf("%s: body = %q, want %q", c.name, got, want.body)
		}
	}
}

// The response saved with an idempotent booking is replayed byte for byte,
// including rows written by an older binary, so its encoding is pinned: keys
// sorted, no promoCode unless there is one, and a trailing newline.
func TestTheReceiptEncodingIsStable(t *testing.T) {
	enc := ReceiptEncoder()
	got := string(enc(app.Receipt{OrderID: "o-1", SurgeMultiplier: 1, Price: 25000, FinalPrice: 25000}))
	want := `{"currency":"VND","discount":0,"finalPrice":25000,"orderId":"o-1","price":25000,"status":"PENDING","surgeMultiplier":1}` + "\n"
	if got != want {
		t.Errorf("receipt = %q\nwant      %q", got, want)
	}
	got = string(enc(app.Receipt{OrderID: "o-2", SurgeMultiplier: 1.2, Price: 25000, FinalPrice: 22500, Discount: 2500, PromoCode: "WELCOME10"}))
	want = `{"currency":"VND","discount":2500,"finalPrice":22500,"orderId":"o-2","price":25000,"promoCode":"WELCOME10","status":"PENDING","surgeMultiplier":1.2}` + "\n"
	if got != want {
		t.Errorf("promo receipt = %q\nwant           %q", got, want)
	}
}

// stub is a service that answers what the test tells it to.
type stub struct {
	create    func(app.CreateOrderCommand) (app.CreateOrderResult, error)
	gotCreate app.CreateOrderCommand
	cancel    func() (app.CancelResult, error)
	order     domain.Order
	err       error
	gotActor  domain.Actor
}

func (s *stub) CreateOrder(_ context.Context, c app.CreateOrderCommand) (app.CreateOrderResult, error) {
	s.gotCreate = c
	return s.create(c)
}

func (s *stub) CancelOrder(_ context.Context, a domain.Actor, _, _ string) (app.CancelResult, error) {
	s.gotActor = a
	return s.cancel()
}

func (s *stub) GetOrder(_ context.Context, a domain.Actor, _ string) (domain.Order, error) {
	s.gotActor = a
	return s.order, s.err
}

func (s *stub) LatestOrder(_ context.Context, a domain.Actor, _ string) (domain.Order, error) {
	s.gotActor = a
	return s.order, s.err
}

func book(s *stub, body string, header http.Header) *httptest.ResponseRecorder {
	return call(newAPI(s, nil).handleCreateOrder, "POST", "/orders", "", body, false, header)
}

func TestEveryAppErrorMapsToItsOldResponse(t *testing.T) {
	valid := `{"riderId":"r",` + where + `}`
	newQuote := surge.Quote{QuoteID: "q2", BaseFare: 20000, Multiplier: 1.5, Price: 30000, Currency: surge.Currency,
		ExpiresAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	nq, _ := json.Marshal(newQuote)
	cases := []struct {
		name       string
		res        app.CreateOrderResult
		err        error
		status     int
		ct, body   string
		replayed   bool
		wantHeader bool
	}{
		{name: "quote moved", err: &app.QuoteConflict{NewQuote: newQuote}, status: 409, ct: "application/json",
			body: `{"error":"the price has changed","newQuote":` + string(nq) + `}` + "\n"},
		{name: "quote used", err: &app.QuoteConflict{NewQuote: newQuote, Reason: app.ReasonQuoteUsed}, status: 409, ct: "application/json",
			body: `{"error":"` + app.ReasonQuoteUsed + `","newQuote":` + string(nq) + `}` + "\n"},
		{name: "invalid quote", err: app.ErrInvalidQuote, status: 400, ct: "text/plain; charset=utf-8",
			body: `{"error":"quote is invalid: request a new one"}` + "\n"},
		{name: "promo refused", err: &app.PromoError{Code: "promo_expired", Message: "the promo code has expired: apply it again"}, status: 400, ct: "application/json",
			body: `{"code":"promo_expired","error":"the promo code has expired: apply it again"}` + "\n"},
		{name: "promo used", err: &app.PromoError{Code: "promo_used", Message: "used", Conflict: true}, status: 409, ct: "application/json",
			body: `{"code":"promo_used","error":"used"}` + "\n"},
		{name: "key with another body", err: app.ErrKeyMismatch, status: 422, ct: "application/json",
			body: `{"error":"this Idempotency-Key was already used with a different request body"}` + "\n"},
		{name: "internal", err: app.ErrInternal, status: 500, ct: "text/plain; charset=utf-8", body: `{"error":"internal error"}` + "\n"},
		{name: "anything else", err: errors.New("boom"), status: 500, ct: "text/plain; charset=utf-8", body: `{"error":"internal error"}` + "\n"},
		{name: "created", res: app.CreateOrderResult{Status: 201, Body: []byte("{}\n")}, status: 201, ct: "application/json", body: "{}\n"},
		{name: "replay", res: app.CreateOrderResult{Status: 201, Body: []byte(`{"stored":1}` + "\n"), Replayed: true}, status: 201, ct: "application/json",
			body: `{"stored":1}` + "\n", wantHeader: true},
		{name: "replay of a stored non-201", res: app.CreateOrderResult{Status: 400, Body: []byte("x\n"), Replayed: true}, status: 400, ct: "application/json",
			body: "x\n", wantHeader: true},
	}
	for _, c := range cases {
		s := &stub{create: func(app.CreateOrderCommand) (app.CreateOrderResult, error) { return c.res, c.err }}
		rec := book(s, valid, nil)
		if rec.Code != c.status || rec.Header().Get("Content-Type") != c.ct || rec.Body.String() != c.body {
			t.Errorf("%s: got %d %q %q\nwant %d %q %q", c.name, rec.Code, rec.Header().Get("Content-Type"), rec.Body.String(), c.status, c.ct, c.body)
		}
		_, has := rec.Header()["Idempotent-Replayed"]
		if has != c.wantHeader || (has && rec.Header().Get("Idempotent-Replayed") != "true") {
			t.Errorf("%s: Idempotent-Replayed present = %v, want %v", c.name, has, c.wantHeader)
		}
	}

	// ErrForbidden reaches the client through auth.Deny.
	rec := call(newAPI(&stub{err: app.ErrForbidden}, nil).handleGetLatestOrder, "GET", "/riders/x/latest-order", "x", "", false, nil)
	if rec.Code != 403 || rec.Header().Get("Content-Type") != "application/json" ||
		rec.Body.String() != `{"error":"you can only read your own orders"}`+"\n" {
		t.Errorf("forbidden: %d %q %q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
	// Reads: not found is 404 text, anything else 500.
	for err, want := range map[error]int{domain.ErrOrderNotFound: 404, app.ErrInternal: 500} {
		rec := call(newAPI(&stub{err: err}, nil).handleGetOrder, "GET", "/orders/x", "x", "", false, nil)
		if rec.Code != want {
			t.Errorf("get with %v = %d, want %d", err, rec.Code, want)
		}
	}
}

func TestCreateOrderTakesTheRiderFromTheToken(t *testing.T) {
	s := &stub{create: func(app.CreateOrderCommand) (app.CreateOrderResult, error) {
		return app.CreateOrderResult{Status: 201, Body: []byte("{}\n")}, nil
	}}
	// The body names no rider, and the token says "r".
	rec := book(s, `{`+where+`,"vehicleType":"BIKE","quoteId":"q","promoToken":"p"}`,
		http.Header{"Idempotency-Key": {"key-1"}})
	if rec.Code != 201 {
		t.Fatalf("status = %d %s", rec.Code, rec.Body)
	}
	got := s.gotCreate
	if got.RiderID != "r" || got.IdempotencyKey != "key-1" || got.Request.VehicleType != domain.VehicleBike ||
		got.Request.QuoteID != "q" || got.Request.PromoToken != "p" ||
		*got.Request.Pickup != (domain.LatLng{Lat: 10.776, Lng: 106.7}) || *got.Request.Dropoff != (domain.LatLng{Lat: 10.79, Lng: 106.71}) {
		t.Errorf("command = %+v", got)
	}
	// The vehicle type defaults to CAR.
	book(s, `{"riderId":"r",`+where+`}`, nil)
	if s.gotCreate.Request.VehicleType != domain.VehicleCar || s.gotCreate.IdempotencyKey != "" {
		t.Errorf("command = %+v", s.gotCreate)
	}
}

func TestTheCallerIsNamedForTheUseCases(t *testing.T) {
	s := &stub{order: domain.Order{ID: "o"}}
	for role, want := range map[auth.Role]domain.Role{auth.Rider: domain.RoleRider, auth.Driver: domain.RoleDriver} {
		req := httptest.NewRequest("GET", "/orders/o", nil)
		req.SetPathValue("id", "o")
		req = req.WithContext(auth.With(req.Context(), auth.Identity{Subject: "u", Role: role}))
		newAPI(s, nil).handleGetOrder(httptest.NewRecorder(), req)
		if s.gotActor != (domain.Actor{Subject: "u", Role: want}) {
			t.Errorf("%s: actor = %+v", role, s.gotActor)
		}
	}
}

func TestAnOrderIsShownAsItAlwaysWas(t *testing.T) {
	price := 100000.0
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	s := &stub{order: domain.Order{
		ID: "o", RiderID: "r", Pickup: domain.LatLng{Lat: 1, Lng: 2}, Dropoff: domain.LatLng{Lat: 3, Lng: 4},
		VehicleType: domain.VehicleCar, Status: domain.StatusCancelled, CancelledAt: &at, CancelledBy: "RIDER", CancelReason: "late",
		SurgeMultiplier: 1.5, Price: &price, PromoCode: "P", Discount: 150000, DriverID: "d-secret",
	}}
	rec := call(newAPI(s, nil).handleGetOrder, "GET", "/orders/o", "o", "", false, nil)
	want := `{"id":"o","riderId":"r","pickup":{"lat":1,"lng":2},"dropoff":{"lat":3,"lng":4},"vehicleType":"CAR","status":"CANCELLED",` +
		`"cancelledAt":"2026-01-02T03:04:05Z","cancelledBy":"RIDER","cancelReason":"late","surgeMultiplier":1.5,"price":100000,` +
		`"promoCode":"P","discount":150000,"finalPrice":0}` + "\n"
	if rec.Body.String() != want || rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("body = %q\nwant   %q", rec.Body, want)
	}
	// No price: null price and finalPrice, and the omitempty fields are gone.
	s.order = domain.Order{ID: "o", Status: domain.StatusPending}
	rec = call(newAPI(s, nil).handleGetOrder, "GET", "/orders/o", "o", "", false, nil)
	want = `{"id":"o","riderId":"","pickup":{"lat":0,"lng":0},"dropoff":{"lat":0,"lng":0},"vehicleType":"","status":"PENDING","surgeMultiplier":0,"price":null,"discount":0,"finalPrice":null}` + "\n"
	if rec.Body.String() != want {
		t.Errorf("body = %q\nwant   %q", rec.Body, want)
	}
}

func TestCancelAnswersByWhatTheOrderBecame(t *testing.T) {
	const id = "00000000-0000-0000-0000-000000000001"
	cases := []struct {
		name   string
		res    app.CancelResult
		err    error
		status int
		body   string
	}{
		{"cancelled", app.CancelResult{Order: domain.Order{ID: id, Status: domain.StatusCancelled}}, nil, 200,
			`{"id":"` + id + `","riderId":"","pickup":{"lat":0,"lng":0},"dropoff":{"lat":0,"lng":0},"vehicleType":"","status":"CANCELLED","surgeMultiplier":0,"price":null,"discount":0,"finalPrice":null}` + "\n"},
		{"matched", app.CancelResult{Order: domain.Order{Status: domain.StatusMatched}, Conflict: true, Message: "a driver is already assigned: cancel the trip instead (POST /trips/{id}/cancel)"}, nil, 409,
			`{"error":"a driver is already assigned: cancel the trip instead (POST /trips/{id}/cancel)","status":"MATCHED"}` + "\n"},
		{"no driver", app.CancelResult{Order: domain.Order{Status: domain.StatusNoDriver}, Conflict: true, Message: "this order can no longer be cancelled"}, nil, 409,
			`{"error":"this order can no longer be cancelled","status":"NO_DRIVER"}` + "\n"},
		{"not found", app.CancelResult{}, domain.ErrOrderNotFound, 404, `{"error":"order not found"}` + "\n"},
		{"internal", app.CancelResult{}, app.ErrInternal, 500, `{"error":"internal error"}` + "\n"},
	}
	for _, c := range cases {
		s := &stub{cancel: func() (app.CancelResult, error) { return c.res, c.err }}
		rec := call(newAPI(s, nil).handleCancelOrder, "POST", "/orders/"+id+"/cancel", id, ``, false, nil)
		if rec.Code != c.status || rec.Body.String() != c.body {
			t.Errorf("%s: got %d %q\nwant %d %q", c.name, rec.Code, rec.Body, c.status, c.body)
		}
	}
	// An empty body is a cancel without a reason; a bad body is a 400.
	s := &stub{cancel: func() (app.CancelResult, error) { return app.CancelResult{}, nil }}
	if rec := call(newAPI(s, nil).handleCancelOrder, "POST", "/x", id, `nope`, false, nil); rec.Code != 400 {
		t.Errorf("bad body = %d", rec.Code)
	}
}

func TestTheRouteTableIsTheOldOne(t *testing.T) {
	issuer, verifier := authtest.Pair(t, time.Minute)
	token := func(role auth.Role) string {
		tok, _, err := issuer.Issue(role, "u")
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	mux := NewAPI(app.New(app.Deps{}), verifier).Routes()
	do := func(method, path, tok string) int {
		req := httptest.NewRequest(method, path, strings.NewReader("nope"))
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Code
	}
	const id = "00000000-0000-0000-0000-000000000001"
	// An unauthenticated call is 401 on every route but /health.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/health", nil))
	if rec.Code != 200 || rec.Body.String() != `{"status":"ok"}`+"\n" || rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("health = %d %q %q", rec.Code, rec.Body, rec.Header().Get("Content-Type"))
	}
	for _, r := range [][2]string{{"POST", "/orders"}, {"GET", "/orders/" + id}, {"POST", "/orders/" + id + "/cancel"}, {"GET", "/riders/u/latest-order"}} {
		if got := do(r[0], r[1], ""); got != 401 {
			t.Errorf("%s %s without a token = %d, want 401", r[0], r[1], got)
		}
	}
	// Roles: POST /orders, cancel and latest-order are rider-only; GET /orders/{id} takes a driver too.
	for _, r := range [][2]string{{"POST", "/orders"}, {"POST", "/orders/" + id + "/cancel"}, {"GET", "/riders/u/latest-order"}} {
		if got := do(r[0], r[1], token(auth.Driver)); got != 403 {
			t.Errorf("%s %s as a driver = %d, want 403", r[0], r[1], got)
		}
	}
	if got := do("GET", "/orders/not-a-uuid", token(auth.Driver)); got != 404 {
		t.Errorf("GET /orders/{id} as a driver = %d, want 404 (past the role check)", got)
	}
	if got := do("GET", "/orders/not-a-uuid", token(auth.Admin)); got != 403 {
		t.Errorf("GET /orders/{id} as an admin = %d, want 403", got)
	}
	// Wrong methods are not routes.
	if got := do("GET", "/orders", token(auth.Rider)); got != 405 {
		t.Errorf("GET /orders = %d, want 405", got)
	}
	// The limiter: burst 10 on the writes, then 429.
	rider := token(auth.Rider)
	limited := false
	for i := 0; i < 15; i++ {
		if do("POST", "/orders", rider) == 429 {
			limited = true
		}
	}
	if !limited {
		t.Error("POST /orders is not rate limited")
	}
}
