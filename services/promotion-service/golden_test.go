package main

// Golden tests: they pin what promotion-service answers and logs, byte for
// byte, so that moving the code into layers cannot change the contract. The
// expectations were recorded from the flat pre-migration code.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"log"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/promo"
)

// goldenNow is the frozen clock of every golden test.
var goldenNow = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

// exp is an expected response: status, Content-Type and the exact body.
type exp struct {
	status int
	ctype  string
	body   string
}

// step is one request. subject defaults to a fixed id per role.
type step struct {
	name   string
	method string
	path   string
	role   string
	body   string
	closed bool // answered by a service whose pool was closed first
	want   exp
}

var createdAtRe = regexp.MustCompile(`"createdAt":"[^"]*"`)

func maskCreatedAt(s string) string { return createdAtRe.ReplaceAllString(s, `"createdAt":"<ts>"`) }

type goldenAPI struct {
	issuer *auth.Issuer
	api    http.Handler
}

func newGoldenAPI(t *testing.T, svc *service) *goldenAPI {
	t.Helper()
	t.Setenv(auth.RateLimitScaleEnv, "0")
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	issuer, err := auth.NewIssuer(priv, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return &goldenAPI{issuer: issuer, api: svc.routes(auth.NewVerifier(auth.NewKeySet(pub)))}
}

func (g *goldenAPI) do(t *testing.T, s step) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(s.method, s.path, strings.NewReader(s.body))
	if s.role != "" {
		subject := map[string]string{"rider": "rider-1", "admin": "admin-1"}[s.role]
		tok, _, err := g.issuer.Issue(auth.Role(s.role), subject)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	rec := httptest.NewRecorder()
	g.api.ServeHTTP(rec, req)
	return rec
}

func (g *goldenAPI) check(t *testing.T, s step, mask bool) {
	t.Helper()
	rec := g.do(t, s)
	body := rec.Body.String()
	if mask {
		body = maskCreatedAt(body)
	}
	if rec.Code != s.want.status {
		t.Errorf("%s: status %d, want %d (body %q)", s.name, rec.Code, s.want.status, body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != s.want.ctype {
		t.Errorf("%s: Content-Type %q, want %q", s.name, ct, s.want.ctype)
	}
	if body != s.want.body {
		t.Errorf("%s: body\n got %q\nwant %q", s.name, body, s.want.body)
	}
}

// goldenService is a service whose database is never reached (the pool is built
// from a URL and never used), or is a closed pool.
func goldenService(t *testing.T, closed bool) *service {
	t.Helper()
	pool, err := openPool(context.Background(), "postgres://golden:golden@127.0.0.1:1/golden")
	if err != nil {
		t.Fatal(err)
	}
	if closed {
		pool.Close()
	} else {
		t.Cleanup(pool.Close)
	}
	signer, err := promo.NewSigner("test-promo-secret")
	if err != nil {
		t.Fatal(err)
	}
	svc := newService(pool, signer, jsonEncoder{}, nil)
	svc.now = func() time.Time { return goldenNow }
	return svc
}

// inUTC mutates the global time.Local, so it must not be combined with t.Parallel.
// It makes the process zone UTC for the test: pgx hands timestamps back in
// time.Local, so the admin views would otherwise follow the machine's TZ.
func inUTC(t *testing.T) {
	t.Helper()
	old := time.Local
	time.Local = time.UTC
	t.Cleanup(func() { time.Local = old })
}

const validPromo = `"code":"GOLD10","kind":"PERCENT","value":10,"validTo":"2030-01-01T00:00:00Z"`

func TestErrorResponsesAreStable(t *testing.T) {
	open := newGoldenAPI(t, goldenService(t, false))
	closed := newGoldenAPI(t, goldenService(t, true))
	v := "/promotions/validate?"
	steps := []step{
		{name: "validate no token", method: "GET", path: v + "code=WELCOME10&price=87000", want: exp{401, "application/json", "{\"error\":\"missing bearer token\"}\n"}},
		{name: "validate admin token", method: "GET", path: v + "code=WELCOME10&price=87000", role: "admin", want: exp{403, "application/json", "{\"error\":\"this role can't do that\"}\n"}},
		{name: "validate short code", method: "GET", path: v + "code=ab&price=87000", role: "rider", want: exp{404, "application/json", "{\"code\":\"not_found\",\"error\":\"no such promotion code\"}\n"}},
		{name: "validate empty code", method: "GET", path: v + "code=&price=87000", role: "rider", want: exp{404, "application/json", "{\"code\":\"not_found\",\"error\":\"no such promotion code\"}\n"}},
		{name: "validate 33 char code", method: "GET", path: v + "code=" + strings.Repeat("A", 33) + "&price=87000", role: "rider", want: exp{404, "application/json", "{\"code\":\"not_found\",\"error\":\"no such promotion code\"}\n"}},
		{name: "validate price missing", method: "GET", path: v + "code=WELCOME10", role: "rider", want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"price is required\"}\n"}},
		{name: "validate price empty", method: "GET", path: v + "code=WELCOME10&price=", role: "rider", want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"price is required\"}\n"}},
		{name: "validate price abc", method: "GET", path: v + "code=WELCOME10&price=abc", role: "rider", want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"price must be a number of VND\"}\n"}},
		{name: "validate price NaN", method: "GET", path: v + "code=WELCOME10&price=NaN", role: "rider", want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"price must be a number of VND\"}\n"}},
		{name: "validate price Inf", method: "GET", path: v + "code=WELCOME10&price=Inf", role: "rider", want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"price must be a number of VND\"}\n"}},
		{name: "validate price 0", method: "GET", path: v + "code=WELCOME10&price=0", role: "rider", want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"price must be above 0 VND\"}\n"}},
		{name: "validate price -5", method: "GET", path: v + "code=WELCOME10&price=-5", role: "rider", want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"price must be above 0 VND\"}\n"}},
		{name: "validate price 1e13", method: "GET", path: v + "code=WELCOME10&price=1e13", role: "rider", want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"price must be above 0 VND\"}\n"}},
		{name: "validate vehicle car lower", method: "GET", path: v + "code=WELCOME10&price=87000&vehicleType=car", role: "rider", want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"vehicleType must be CAR or BIKE\"}\n"}},
		{name: "validate vehicle TRUCK", method: "GET", path: v + "code=WELCOME10&price=87000&vehicleType=TRUCK", role: "rider", want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"vehicleType must be CAR or BIKE\"}\n"}},
		{name: "validate order code before price", method: "GET", path: v + "code=ab&price=abc", role: "rider", want: exp{404, "application/json", "{\"code\":\"not_found\",\"error\":\"no such promotion code\"}\n"}},
		{name: "validate order price before vehicle", method: "GET", path: v + "code=WELCOME10&price=abc&vehicleType=TRUCK", role: "rider", want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"price must be a number of VND\"}\n"}},
		{name: "validate order code before vehicle", method: "GET", path: v + "code=ab&price=87000&vehicleType=TRUCK", role: "rider", want: exp{404, "application/json", "{\"code\":\"not_found\",\"error\":\"no such promotion code\"}\n"}},

		{name: "create no token", method: "POST", path: "/promotions", body: `{` + validPromo + `}`, want: exp{401, "application/json", "{\"error\":\"missing bearer token\"}\n"}},
		{name: "create rider token", method: "POST", path: "/promotions", role: "rider", body: `{` + validPromo + `}`, want: exp{403, "application/json", "{\"error\":\"this role can't do that\"}\n"}},
		{name: "create invalid json", method: "POST", path: "/promotions", role: "admin", body: `{not json`, want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"invalid JSON body\"}\n"}},
		{name: "create empty body", method: "POST", path: "/promotions", role: "admin", body: ``, want: exp{400, "application/json", "{\"code\":\"invalid_request\",\"error\":\"invalid JSON body\"}\n"}},
		{name: "create bad code", method: "POST", path: "/promotions", role: "admin", body: `{"code":"a!","kind":"PERCENT","value":10,"validTo":"2030-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"code must be 3 to 32 characters: letters, digits, '-' or '_'\"}\n"}},
		{name: "create bad kind", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"HALF","value":10,"validTo":"2030-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"kind must be PERCENT or FIXED\"}\n"}},
		{name: "create value 0", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"PERCENT","value":0,"validTo":"2030-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"value must be above 0\"}\n"}},
		{name: "create percent over 100", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"PERCENT","value":101,"validTo":"2030-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"a PERCENT value is a percentage from 1 to 100\"}\n"}},
		{name: "create maxDiscount on FIXED", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"FIXED","value":5000,"maxDiscount":1000,"validTo":"2030-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"maxDiscount applies to PERCENT promotions only\"}\n"}},
		{name: "create maxDiscount 0", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"PERCENT","value":10,"maxDiscount":0,"validTo":"2030-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"maxDiscount must be above 0\"}\n"}},
		{name: "create minFare negative", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"PERCENT","value":10,"minFare":-1,"validTo":"2030-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"minFare must be 0 or more\"}\n"}},
		{name: "create bad vehicle", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"PERCENT","value":10,"vehicleType":"TRUCK","validTo":"2030-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"vehicleType must be CAR or BIKE (or omitted for any)\"}\n"}},
		{name: "create no validTo", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"PERCENT","value":10}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"validTo is required\"}\n"}},
		{name: "create validTo before validFrom", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"PERCENT","value":10,"validFrom":"2030-01-01T00:00:00Z","validTo":"2029-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"validTo must be after validFrom\"}\n"}},
		{name: "create validTo in the past", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"PERCENT","value":10,"validFrom":"2019-01-01T00:00:00Z","validTo":"2020-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"validTo is in the past\"}\n"}},
		{name: "create usageLimit 0", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"PERCENT","value":10,"usageLimit":0,"validTo":"2030-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"usageLimit must be above 0 (or omitted for no limit)\"}\n"}},
		{name: "create perRiderLimit 0", method: "POST", path: "/promotions", role: "admin", body: `{"code":"GOLD10","kind":"PERCENT","value":10,"perRiderLimit":0,"validTo":"2030-01-01T00:00:00Z"}`, want: exp{400, "application/json", "{\"code\":\"invalid_promotion\",\"error\":\"perRiderLimit must be 1 or more\"}\n"}},

		{name: "disable no token", method: "POST", path: "/promotions/ABC/disable", want: exp{401, "application/json", "{\"error\":\"missing bearer token\"}\n"}},
		{name: "disable rider token", method: "POST", path: "/promotions/ABC/disable", role: "rider", want: exp{403, "application/json", "{\"error\":\"this role can't do that\"}\n"}},
		{name: "list no token", method: "GET", path: "/promotions", want: exp{401, "application/json", "{\"error\":\"missing bearer token\"}\n"}},
		{name: "list rider token", method: "GET", path: "/promotions", role: "rider", want: exp{403, "application/json", "{\"error\":\"this role can't do that\"}\n"}},

		{name: "health", method: "GET", path: "/health", want: exp{200, "application/json", "{\"status\":\"ok\"}\n"}},
		{name: "closed pool validate", method: "GET", path: v + "code=WELCOME10&price=87000", role: "rider", closed: true, want: exp{500, "application/json", "{\"code\":\"internal\",\"error\":\"internal error\"}\n"}},
		{name: "closed pool list", method: "GET", path: "/promotions", role: "admin", closed: true, want: exp{500, "application/json", "{\"code\":\"internal\",\"error\":\"internal error\"}\n"}},
	}
	for _, s := range steps {
		api := open
		if s.closed {
			api = closed
		}
		api.check(t, s, false)
	}
}

func TestSuccessBodiesAreStable(t *testing.T) {
	inUTC(t)
	f := newFixture(t)
	f.now = goldenNow
	g := &goldenAPI{issuer: f.issuer, api: f.api}
	// Created in the database "now", so the list order (newest first) is fixed.
	steps := []struct {
		step
		mask bool
		do   func() // run before the request
	}{
		{step: step{name: "list empty", method: "GET", path: "/promotions", role: "admin", want: exp{200, "application/json", "{\"promotions\":[]}\n"}}},
		{step: step{name: "validate ok", method: "GET", path: "/promotions/validate?code=WELCOME10&price=87000&vehicleType=CAR", role: "rider", want: exp{200, "application/json", "{\"code\":\"WELCOME10\",\"currency\":\"VND\",\"discount\":8700,\"expiresAt\":\"2026-06-01T12:10:00Z\",\"finalPrice\":78300,\"price\":87000,\"promoToken\":\"eyJjIjoiV0VMQ09NRTEwIiwiciI6InJpZGVyLTEiLCJwIjo4NzAwMCwidiI6IkNBUiIsImQiOjg3MDAsImUiOjE3ODAzMTU4MDB9.JA1P5ggK8q1XKiaABo9u341UMmCVpQldBVdshg2NfXk\"}\n"}},
			do: func() { f.promotion("WELCOME10", nil) }},
		// Reuses the first row's promoToken on purpose: default CAR vehicle, and 87000.4 rounds to 87000.
		{step: step{name: "validate ok lower case code no vehicle", method: "GET", path: "/promotions/validate?code=%20welcome10%20&price=87000.4", role: "rider", want: exp{200, "application/json", "{\"code\":\"WELCOME10\",\"currency\":\"VND\",\"discount\":8700,\"expiresAt\":\"2026-06-01T12:10:00Z\",\"finalPrice\":78300,\"price\":87000,\"promoToken\":\"eyJjIjoiV0VMQ09NRTEwIiwiciI6InJpZGVyLTEiLCJwIjo4NzAwMCwidiI6IkNBUiIsImQiOjg3MDAsImUiOjE3ODAzMTU4MDB9.JA1P5ggK8q1XKiaABo9u341UMmCVpQldBVdshg2NfXk\"}\n"}}},
		{step: step{name: "create 201", method: "POST", path: "/promotions", role: "admin",
			body: `{"code":" summer20 ","kind":"percent","value":20,"maxDiscount":30000,"minFare":50000,"vehicleType":"CAR","validTo":"2026-06-04T12:00:00Z","usageLimit":100,"perRiderLimit":2}`, want: exp{201, "application/json", "{\"code\":\"SUMMER20\",\"kind\":\"PERCENT\",\"value\":20,\"maxDiscount\":30000,\"minFare\":50000,\"vehicleType\":\"CAR\",\"validFrom\":\"2026-06-01T12:00:00Z\",\"validTo\":\"2026-06-04T12:00:00Z\",\"usageLimit\":100,\"perRiderLimit\":2,\"disabledAt\":null,\"status\":\"active\",\"redeemed\":0,\"overLimit\":0,\"createdBy\":\"admin-1\",\"createdAt\":\"<ts>\"}\n"}}, mask: true},
		{step: step{name: "create fixed 201", method: "POST", path: "/promotions", role: "admin",
			body: `{"code":"FLAT5K","kind":"FIXED","value":5000,"validFrom":"2026-06-02T00:00:00Z","validTo":"2026-06-05T12:00:00Z"}`, want: exp{201, "application/json", "{\"code\":\"FLAT5K\",\"kind\":\"FIXED\",\"value\":5000,\"maxDiscount\":null,\"minFare\":0,\"vehicleType\":null,\"validFrom\":\"2026-06-02T00:00:00Z\",\"validTo\":\"2026-06-05T12:00:00Z\",\"usageLimit\":null,\"perRiderLimit\":1,\"disabledAt\":null,\"status\":\"scheduled\",\"redeemed\":0,\"overLimit\":0,\"createdBy\":\"admin-1\",\"createdAt\":\"<ts>\"}\n"}}, mask: true},
		{step: step{name: "create duplicate 409", method: "POST", path: "/promotions", role: "admin",
			body: `{"code":"SUMMER20","kind":"FIXED","value":5000,"validTo":"2026-06-04T12:00:00Z"}`, want: exp{409, "application/json", "{\"code\":\"code_exists\",\"error\":\"a promotion with this code already exists\"}\n"}}},
		{step: step{name: "list two", method: "GET", path: "/promotions", role: "admin", want: exp{200, "application/json", "{\"promotions\":[{\"code\":\"FLAT5K\",\"kind\":\"FIXED\",\"value\":5000,\"maxDiscount\":null,\"minFare\":0,\"vehicleType\":null,\"validFrom\":\"2026-06-02T00:00:00Z\",\"validTo\":\"2026-06-05T12:00:00Z\",\"usageLimit\":null,\"perRiderLimit\":1,\"disabledAt\":null,\"status\":\"scheduled\",\"redeemed\":0,\"overLimit\":0,\"createdBy\":\"admin-1\",\"createdAt\":\"<ts>\"},{\"code\":\"SUMMER20\",\"kind\":\"PERCENT\",\"value\":20,\"maxDiscount\":30000,\"minFare\":50000,\"vehicleType\":\"CAR\",\"validFrom\":\"2026-06-01T12:00:00Z\",\"validTo\":\"2026-06-04T12:00:00Z\",\"usageLimit\":100,\"perRiderLimit\":2,\"disabledAt\":null,\"status\":\"active\",\"redeemed\":0,\"overLimit\":0,\"createdBy\":\"admin-1\",\"createdAt\":\"<ts>\"},{\"code\":\"WELCOME10\",\"kind\":\"PERCENT\",\"value\":10,\"maxDiscount\":20000,\"minFare\":0,\"vehicleType\":null,\"validFrom\":\"2026-06-01T11:00:00Z\",\"validTo\":\"2026-06-02T12:00:00Z\",\"usageLimit\":null,\"perRiderLimit\":1,\"disabledAt\":null,\"status\":\"active\",\"redeemed\":0,\"overLimit\":0,\"createdBy\":\"\",\"createdAt\":\"<ts>\"}]}\n"}}, mask: true},
		{step: step{name: "disable first", method: "POST", path: "/promotions/summer20/disable", role: "admin", want: exp{200, "application/json", "{\"code\":\"SUMMER20\",\"kind\":\"PERCENT\",\"value\":20,\"maxDiscount\":30000,\"minFare\":50000,\"vehicleType\":\"CAR\",\"validFrom\":\"2026-06-01T12:00:00Z\",\"validTo\":\"2026-06-04T12:00:00Z\",\"usageLimit\":100,\"perRiderLimit\":2,\"disabledAt\":\"2026-06-01T12:00:00Z\",\"status\":\"disabled\",\"redeemed\":0,\"overLimit\":0,\"createdBy\":\"admin-1\",\"createdAt\":\"<ts>\"}\n"}}, mask: true},
		{step: step{name: "disable second keeps first disabledAt", method: "POST", path: "/promotions/SUMMER20/disable", role: "admin", want: exp{200, "application/json", "{\"code\":\"SUMMER20\",\"kind\":\"PERCENT\",\"value\":20,\"maxDiscount\":30000,\"minFare\":50000,\"vehicleType\":\"CAR\",\"validFrom\":\"2026-06-01T12:00:00Z\",\"validTo\":\"2026-06-04T12:00:00Z\",\"usageLimit\":100,\"perRiderLimit\":2,\"disabledAt\":\"2026-06-01T12:00:00Z\",\"status\":\"disabled\",\"redeemed\":0,\"overLimit\":0,\"createdBy\":\"admin-1\",\"createdAt\":\"<ts>\"}\n"}}, mask: true,
			do: func() { f.now = goldenNow.Add(time.Hour) }},
		{step: step{name: "disable unknown 404", method: "POST", path: "/promotions/NOPE/disable", role: "admin", want: exp{404, "application/json", "{\"code\":\"not_found\",\"error\":\"no such promotion code\"}\n"}}},
		{step: step{name: "list after disable", method: "GET", path: "/promotions", role: "admin", want: exp{200, "application/json", "{\"promotions\":[{\"code\":\"FLAT5K\",\"kind\":\"FIXED\",\"value\":5000,\"maxDiscount\":null,\"minFare\":0,\"vehicleType\":null,\"validFrom\":\"2026-06-02T00:00:00Z\",\"validTo\":\"2026-06-05T12:00:00Z\",\"usageLimit\":null,\"perRiderLimit\":1,\"disabledAt\":null,\"status\":\"scheduled\",\"redeemed\":0,\"overLimit\":0,\"createdBy\":\"admin-1\",\"createdAt\":\"<ts>\"},{\"code\":\"SUMMER20\",\"kind\":\"PERCENT\",\"value\":20,\"maxDiscount\":30000,\"minFare\":50000,\"vehicleType\":\"CAR\",\"validFrom\":\"2026-06-01T12:00:00Z\",\"validTo\":\"2026-06-04T12:00:00Z\",\"usageLimit\":100,\"perRiderLimit\":2,\"disabledAt\":\"2026-06-01T12:00:00Z\",\"status\":\"disabled\",\"redeemed\":0,\"overLimit\":0,\"createdBy\":\"admin-1\",\"createdAt\":\"<ts>\"},{\"code\":\"WELCOME10\",\"kind\":\"PERCENT\",\"value\":10,\"maxDiscount\":20000,\"minFare\":0,\"vehicleType\":null,\"validFrom\":\"2026-06-01T11:00:00Z\",\"validTo\":\"2026-06-02T12:00:00Z\",\"usageLimit\":null,\"perRiderLimit\":1,\"disabledAt\":null,\"status\":\"active\",\"redeemed\":0,\"overLimit\":0,\"createdBy\":\"\",\"createdAt\":\"<ts>\"}]}\n"}}, mask: true},
	}
	for _, s := range steps {
		if s.do != nil {
			s.do()
		}
		g.check(t, s.step, s.mask)
	}
}

// captureLog returns what log writes until the test ends, with no prefix.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	oldW, oldF := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(oldW); log.SetFlags(oldF) })
	return &buf
}

func TestLogLinesAreStable(t *testing.T) {
	const (
		ev1 = "11111111-1111-4111-8111-111111111111"
		ev2 = "22222222-2222-4222-8222-222222222222"
		ev3 = "33333333-3333-4333-8333-333333333333"
		ev4 = "44444444-4444-4444-8444-444444444444"
		ev5 = "55555555-5555-4555-8555-555555555555"
		ev6 = "66666666-6666-4666-8666-666666666666"
		ev7 = "77777777-7777-4777-8777-777777777777"
		ev8 = "88888888-8888-4888-8888-888888888888"
		ev9 = "99999999-9999-4999-8999-999999999999"
	)
	inUTC(t)
	ctx := context.Background()
	order := func(id, rider, code string, discount int64, eventID string) events.OrderCreated {
		e := created(id, rider, code, discount)
		e.EventID = eventID
		return e
	}
	t.Run("events", func(t *testing.T) {
		f := newFixture(t)
		f.now = goldenNow
		f.promotion("WELCOME10", nil) // once per rider
		buf := captureLog(t)
		expect := func(what string, err error, want string) {
			t.Helper()
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			if got := buf.String(); got != want {
				t.Errorf("%s: log\n got %q\nwant %q", what, got, want)
			}
			buf.Reset()
		}

		err := f.svc.onOrderCreated(ctx, order("order-1", "rider-1", "WELCOME10", 10000, ev1))
		expect("redemption", err, "order order-1 redeemed WELCOME10 for 10000 VND\n")

		err = f.svc.onOrderCreated(ctx, order("order-1", "rider-1", "WELCOME10", 10000, ev1))
		expect("redelivered order.created", err, "duplicate event 11111111-1111-4111-8111-111111111111, skipping\n")

		err = f.svc.onOrderCreated(ctx, order("order-2", "rider-1", "WELCOME10", 10000, ev2))
		expect("over the per-rider limit", err, "order order-2 used WELCOME10 past its per_rider_limit: recorded as OVER_LIMIT (discount 10000 VND stays on the order)\n")

		err = f.svc.onOrderCancelled(ctx, events.OrderCancelled{Envelope: events.Envelope{EventID: ev3}, OrderID: "order-3", RiderID: "rider-3"})
		expect("cancel seen before the order", err, "")
		err = f.svc.onOrderCreated(ctx, order("order-3", "rider-3", "WELCOME10", 10000, ev4))
		expect("redemption of an already cancelled order", err, "order order-3 used WELCOME10 but was already cancelled: recorded as released\n")

		// Each release path on its own order.
		rel := []struct {
			order, rider, create, release string
			end                           func(o, r, e string) error
			want                          string
		}{
			{"order-4", "rider-4", ev5, ev6, func(o, r, e string) error {
				return f.svc.onOrderCancelled(ctx, events.OrderCancelled{Envelope: events.Envelope{EventID: e}, OrderID: o, RiderID: r})
			}, "order order-4 cancelled (order.cancelled): 1 redemption(s) released\n"},
			{"order-5", "rider-5", ev7, ev8, func(o, r, e string) error {
				return f.svc.onTripStatusChanged(ctx, events.TripStatusChanged{Envelope: events.Envelope{EventID: e}, OrderID: o, RiderID: r, Status: events.TripStatusCancelled})
			}, "order order-5 trip cancelled (trip.status.changed): 1 redemption(s) released\n"},
			{"order-6", "rider-6", ev9, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", func(o, r, e string) error {
				return f.svc.onNoDriverFound(ctx, events.OrderNoDriverFound{Envelope: events.Envelope{EventID: e}, OrderID: o, RiderID: r})
			}, "order order-6 found no driver (order.no_driver_found): 1 redemption(s) released\n"},
		}
		for _, c := range rel {
			if err := f.svc.onOrderCreated(ctx, order(c.order, c.rider, "WELCOME10", 10000, c.create)); err != nil {
				t.Fatal(err)
			}
			buf.Reset()
			expect("release "+c.order, c.end(c.order, c.rider, c.release), c.want)
		}
		// A redelivered release.
		err = f.svc.onNoDriverFound(ctx, events.OrderNoDriverFound{Envelope: events.Envelope{EventID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, OrderID: "order-6", RiderID: "rider-6"})
		expect("redelivered release", err, "duplicate event aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa, skipping\n")
	})

	t.Run("admin", func(t *testing.T) {
		f := newFixture(t)
		f.now = goldenNow
		g := &goldenAPI{issuer: f.issuer, api: f.api}
		buf := captureLog(t)
		g.do(t, step{method: "POST", path: "/promotions", role: "admin", body: `{"code":"LOG20","kind":"PERCENT","value":20,"validTo":"2026-06-04T12:00:00Z"}`})
		if got, want := buf.String(), "promotion LOG20 created by admin-1\n"; got != want {
			t.Errorf("create: log %q, want %q", got, want)
		}
		buf.Reset()
		g.do(t, step{method: "POST", path: "/promotions/log20/disable", role: "admin"})
		if got, want := buf.String(), "promotion LOG20 disabled by admin-1\n"; got != want {
			t.Errorf("disable: log %q, want %q", got, want)
		}
	})

	t.Run("internal errors", func(t *testing.T) {
		closed := newGoldenAPI(t, goldenService(t, true))
		buf := captureLog(t)
		closed.do(t, step{method: "GET", path: "/promotions/validate?code=WELCOME10&price=87000", role: "rider"})
		if got, want := buf.String(), "load promotion: closed pool\n"; got != want {
			t.Errorf("validate: log %q, want %q", got, want)
		}
		buf.Reset()
		closed.do(t, step{method: "GET", path: "/promotions", role: "admin"})
		if got, want := buf.String(), "list promotions: closed pool\n"; got != want {
			t.Errorf("list: log %q, want %q", got, want)
		}
	})
}
