package http

// Golden tests: the same tables as the flat service's golden_test.go (copied
// row for row, the expected strings being the bytes recorded before the
// migration), run over NewAPI with an app.Service on in-memory fakes. Where
// the flat test needed a database, the fake returns the same data and the
// exact bytes are compared; the closed-pool rows use a failing fake.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/promo"
	"grab-mini/promotion-service/internal/app"
	"grab-mini/promotion-service/internal/domain"
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
	closed bool // answered by a service whose store fails
	want   exp
}

var createdAtRe = regexp.MustCompile(`"createdAt":"[^"]*"`)

func maskCreatedAt(s string) string { return createdAtRe.ReplaceAllString(s, `"createdAt":"<ts>"`) }

// ---------------------------------------------------------------- fakes ----

// memStore is an in-memory PromotionReader, PromotionWriter and PromotionViews.
// fail, when set, is returned by every call (the "closed pool").
type memStore struct {
	promos    map[string]domain.Promotion
	createdBy map[string]string
	usage     map[string]domain.Usage // key: code + "|" + rider
	fail      error
	failOn    map[string]error // by method name
	now       *time.Time
}

func newMemStore(now *time.Time) *memStore {
	return &memStore{promos: map[string]domain.Promotion{}, createdBy: map[string]string{}, usage: map[string]domain.Usage{}, failOn: map[string]error{}, now: now}
}

func (s *memStore) err(method string) error {
	if s.fail != nil {
		return s.fail
	}
	return s.failOn[method]
}

func (s *memStore) Find(_ context.Context, code string) (domain.Promotion, error) {
	if err := s.err("Find"); err != nil {
		return domain.Promotion{}, err
	}
	p, ok := s.promos[code]
	if !ok {
		return domain.Promotion{}, domain.ErrNotFound
	}
	return p, nil
}

func (s *memStore) Usage(_ context.Context, code, rider string) (domain.Usage, error) {
	if err := s.err("Usage"); err != nil {
		return domain.Usage{}, err
	}
	return s.usage[code+"|"+rider], nil
}

func (s *memStore) Create(_ context.Context, p domain.Promotion, by string) error {
	if err := s.err("Create"); err != nil {
		return err
	}
	if _, ok := s.promos[p.Code]; ok {
		return domain.ErrCodeExists
	}
	s.promos[p.Code] = p
	s.createdBy[p.Code] = by
	return nil
}

func (s *memStore) Disable(_ context.Context, code string, at time.Time) (bool, error) {
	if err := s.err("Disable"); err != nil {
		return false, err
	}
	p, ok := s.promos[code]
	if !ok {
		return false, nil
	}
	if p.DisabledAt == nil { // COALESCE(disabled_at, $at)
		p.DisabledAt = &at
		s.promos[code] = p
	}
	return true, nil
}

func (s *memStore) summary(code string) domain.Summary {
	return domain.Summary{Promotion: s.promos[code], CreatedBy: s.createdBy[code], CreatedAt: goldenNow}
}

func (s *memStore) View(_ context.Context, code string) (domain.Summary, error) {
	if err := s.err("View"); err != nil {
		return domain.Summary{}, err
	}
	if _, ok := s.promos[code]; !ok {
		return domain.Summary{}, domain.ErrNotFound
	}
	return s.summary(code), nil
}

// List is newest first, then by code, as the database orders it.
func (s *memStore) List(_ context.Context) ([]domain.Summary, error) {
	if err := s.err("List"); err != nil {
		return nil, err
	}
	var out []domain.Summary
	for c := range s.promos {
		out = append(out, s.summary(c))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out, nil
}

// signing issues the real token, so the recorded bytes can be compared.
type signing struct{ signer *promo.Signer }

func (s signing) Issue(code, riderID string, price int64, vehicle string, discount int64, now time.Time) (string, time.Time) {
	return s.signer.Issue(code, promo.Binding{RiderID: riderID, Price: price, Vehicle: vehicle}, discount, now)
}

type noMetrics struct{}

func (noMetrics) Validation(string)      {}
func (noMetrics) Redemption(_, _ string) {}
func (noMetrics) Released(int64)         {}
func (noMetrics) Duplicate()             {}

// fixture is an API over a fake-backed service; now is its clock.
type fixture struct {
	t      *testing.T
	now    time.Time
	store  *memStore
	issuer *auth.Issuer
	api    http.Handler
	remux  func() http.Handler // a new mux over the same service and keys
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	t.Setenv(auth.RateLimitScaleEnv, "0") // the cases below ask more often than a person would
	f := &fixture{t: t, now: goldenNow}
	f.store = newMemStore(&f.now)
	signer, err := promo.NewSigner("test-promo-secret")
	if err != nil {
		t.Fatal(err)
	}
	svc := app.New(app.Deps{Reader: f.store, Writer: f.store, Views: f.store, Tokens: signing{signer}, Metrics: noMetrics{},
		Now: func() time.Time { return f.now }})
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	f.issuer, err = auth.NewIssuer(priv, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.remux = func() http.Handler { return NewAPI(svc, auth.NewVerifier(auth.NewKeySet(pub))).Routes() }
	f.api = f.remux()
	return f
}

// promotion stores a promotion that is valid now, edited by fn.
func (f *fixture) promotion(code string, fn func(*domain.Promotion)) {
	f.t.Helper()
	max := int64(20000)
	p := domain.Promotion{Code: code, Kind: domain.KindPercent, Value: 10, MaxDiscount: &max,
		ValidFrom: f.now.Add(-time.Hour), ValidTo: f.now.Add(24 * time.Hour), PerRider: 1}
	if fn != nil {
		fn(&p)
	}
	f.store.promos[p.Code] = p
}

func (f *fixture) do(t *testing.T, s step) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(s.method, s.path, strings.NewReader(s.body))
	if s.role != "" {
		subject := map[string]string{"rider": "rider-1", "admin": "admin-1"}[s.role]
		tok, _, err := f.issuer.Issue(auth.Role(s.role), subject)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	rec := httptest.NewRecorder()
	f.api.ServeHTTP(rec, req)
	return rec
}

func (f *fixture) check(t *testing.T, s step, mask bool) {
	t.Helper()
	rec := f.do(t, s)
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

// failing is a fixture whose store fails every call, like a closed pool.
func failing(t *testing.T) *fixture {
	f := newFixture(t)
	f.store.fail = errors.New("closed pool")
	return f
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

const validPromo = `"code":"GOLD10","kind":"PERCENT","value":10,"validTo":"2030-01-01T00:00:00Z"`

func TestErrorResponsesAreStable(t *testing.T) {
	open := newFixture(t)
	closed := failing(t)
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
	f := newFixture(t)
	f.now = goldenNow
	// Created "now", so the list order (newest first, then by code) is fixed.
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
		f.check(t, s.step, s.mask)
	}
}

func TestInternalErrorsLogTheirStep(t *testing.T) {
	closed := failing(t)
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
}

func TestAdminLogLinesAreStable(t *testing.T) {
	f := newFixture(t)
	buf := captureLog(t)
	f.do(t, step{method: "POST", path: "/promotions", role: "admin", body: `{"code":"LOG20","kind":"PERCENT","value":20,"validTo":"2026-06-04T12:00:00Z"}`})
	if got, want := buf.String(), "promotion LOG20 created by admin-1\n"; got != want {
		t.Errorf("create: log %q, want %q", got, want)
	}
	buf.Reset()
	f.do(t, step{method: "POST", path: "/promotions/log20/disable", role: "admin"})
	if got, want := buf.String(), "promotion LOG20 disabled by admin-1\n"; got != want {
		t.Errorf("disable: log %q, want %q", got, want)
	}
}
