package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"grab-mini/pkg/auth"
)

// These tests pin what clients receive, byte for byte, so the Clean
// Architecture migration cannot change the wire by tidying up. The expected
// values were recorded from the code before the migration.

const goldenWhere = `"pickup":{"lat":10.776,"lng":106.7},"dropoff":{"lat":10.79,"lng":106.71}`

// goldenCall sends one request straight to a handler as the rider "r" (or with
// no identity), so nothing touches the database.
func goldenCall(handler func(http.ResponseWriter, *http.Request), method, path, id, body string, noIdentity bool, header http.Header) *httptest.ResponseRecorder {
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
	handler(rec, req)
	return rec
}

func TestErrorResponsesAreStable(t *testing.T) {
	s := &server{}
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
	valid := `{"riderId":"r",` + goldenWhere + `}`
	cases := []struct {
		name       string
		handler    func(http.ResponseWriter, *http.Request)
		method     string
		path, id   string
		body       string
		noIdentity bool
		header     http.Header
	}{
		{name: "create without identity", handler: s.handleCreateOrder, method: "POST", path: "/orders", body: `{}`, noIdentity: true},
		{name: "create naming another rider", handler: s.handleCreateOrder, method: "POST", path: "/orders", body: `{"riderId":"someone-else",` + goldenWhere + `}`},
		{name: "invalid JSON", handler: s.handleCreateOrder, method: "POST", path: "/orders", body: `nope`},
		{name: "empty Idempotency-Key", handler: s.handleCreateOrder, method: "POST", path: "/orders", body: valid, header: hdr("Idempotency-Key", "")},
		{name: "129 character Idempotency-Key", handler: s.handleCreateOrder, method: "POST", path: "/orders", body: valid, header: hdr("Idempotency-Key", strings.Repeat("k", 129))},
		{name: "control character in Idempotency-Key", handler: s.handleCreateOrder, method: "POST", path: "/orders", body: valid, header: hdr("Idempotency-Key", "a\x01b")},
		{name: "two Idempotency-Key headers", handler: s.handleCreateOrder, method: "POST", path: "/orders", body: valid, header: hdr("Idempotency-Key", "a", "Idempotency-Key", "b")},
		{name: "missing pickup", handler: s.handleCreateOrder, method: "POST", path: "/orders", body: `{"riderId":"r","dropoff":{"lat":10.79,"lng":106.71}}`},
		{name: "latitude 91", handler: s.handleCreateOrder, method: "POST", path: "/orders", body: `{"riderId":"r","pickup":{"lat":91,"lng":0},"dropoff":{"lat":10.79,"lng":106.71}}`},
		{name: "unknown vehicle type", handler: s.handleCreateOrder, method: "POST", path: "/orders", body: `{"riderId":"r",` + goldenWhere + `,"vehicleType":"TRUCK"}`},
		{name: "lowercase vehicle type", handler: s.handleCreateOrder, method: "POST", path: "/orders", body: `{"riderId":"r",` + goldenWhere + `,"vehicleType":"car"}`},
		{name: "get a malformed order id", handler: s.handleGetOrder, method: "GET", path: "/orders/not-a-uuid", id: "not-a-uuid"},
		{name: "cancel a malformed order id", handler: s.handleCancelOrder, method: "POST", path: "/orders/not-a-uuid/cancel", id: "not-a-uuid", body: `{}`},
		{name: "cancel reason of 201 runes", handler: s.handleCancelOrder, method: "POST", path: "/orders/00000000-0000-0000-0000-000000000001/cancel", id: "00000000-0000-0000-0000-000000000001", body: `{"reason":"` + strings.Repeat("é", 201) + `"}`},
		{name: "latest order of another rider", handler: s.handleGetLatestOrder, method: "GET", path: "/riders/someone-else/latest-order", id: "someone-else"},
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
		rec := goldenCall(c.handler, c.method, c.path, c.id, c.body, c.noIdentity, c.header)
		if rec.Code != want.status {
			t.Errorf("%s: status = %d, want %d", c.name, rec.Code, want.status)
		}
		if got := rec.Header().Get("Content-Type"); got != want.ct {
			t.Errorf("%s: Content-Type = %q, want %q", c.name, got, want.ct)
		}
		wantAuth := ""
		if want.status == http.StatusUnauthorized {
			wantAuth = "Bearer"
		}
		if got := rec.Header().Get("WWW-Authenticate"); got != wantAuth {
			t.Errorf("%s: WWW-Authenticate = %q, want %q", c.name, got, wantAuth)
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
	a := newAPI(t) // skips without TEST_DATABASE_URL

	got := a.book("rider-1", "k1", bookingBody)
	if got.Code != http.StatusCreated {
		t.Fatalf("booking = %d %s", got.Code, got.Raw)
	}
	price := bookingPrice()
	want := fmt.Sprintf(`{"currency":"VND","discount":0,"finalPrice":%d,"orderId":"%s","price":%d,"status":"PENDING","surgeMultiplier":1}`+"\n",
		price, got.OrderID, price)
	if got.Raw != want {
		t.Errorf("receipt = %q\nwant      %q", got.Raw, want)
	}

	replay := a.book("rider-1", "k1", bookingBody)
	if replay.Code != http.StatusCreated || !replay.Replay || replay.Raw != got.Raw {
		t.Errorf("replay = %d replayed=%v %q, want the stored body %q", replay.Code, replay.Replay, replay.Raw, got.Raw)
	}

	discount := price / 10
	promoted := a.book("rider-2", "k2", withToken(a.token("rider-2", price, "CAR", discount, t0.Add(-time.Minute))))
	if promoted.Code != http.StatusCreated {
		t.Fatalf("promo booking = %d %s", promoted.Code, promoted.Raw)
	}
	want = fmt.Sprintf(`{"currency":"VND","discount":%d,"finalPrice":%d,"orderId":"%s","price":%d,"promoCode":"WELCOME10","status":"PENDING","surgeMultiplier":1}`+"\n",
		discount, price-discount, promoted.OrderID, price)
	if promoted.Raw != want {
		t.Errorf("promo receipt = %q\nwant           %q", promoted.Raw, want)
	}

	replay = a.book("rider-2", "k2", withToken(a.token("rider-2", price, "CAR", discount, t0.Add(-time.Minute))))
	if replay.Code != http.StatusCreated || !replay.Replay || replay.Raw != promoted.Raw {
		t.Errorf("promo replay = %d replayed=%v %q, want the stored body %q", replay.Code, replay.Replay, replay.Raw, promoted.Raw)
	}
}
