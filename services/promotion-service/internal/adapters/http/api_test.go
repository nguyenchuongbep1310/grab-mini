package http

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/promotion-service/internal/app"
	"grab-mini/promotion-service/internal/domain"
)

const (
	bodyInternal = "{\"code\":\"internal\",\"error\":\"internal error\"}\n"
	jsonType     = "application/json"
)

func TestEveryAppErrorMapsToItsOldResponse(t *testing.T) {
	boom := errors.New("boom")
	const v = "/promotions/validate?price=87000&code="
	type tc struct {
		name  string
		setup func(f *fixture)
		req   step
		want  exp
		log   string
	}
	rej := func(code, msg string, status int) exp {
		return exp{status, jsonType, fmt.Sprintf("{\"code\":%q,\"error\":%q}\n", code, msg)}
	}
	cases := []tc{
		{name: "rejection not_found is 404", req: step{method: "GET", path: v + "NOPE", role: "rider"},
			want: rej("not_found", "no such promotion code", 404)},
		{name: "a disabled code is 404", setup: func(f *fixture) {
			f.promotion("GONE", func(p *domain.Promotion) { at := goldenNow; p.DisabledAt = &at })
		}, req: step{method: "GET", path: v + "GONE", role: "rider"}, want: rej("not_found", "no such promotion code", 404)},
		{name: "not_started is 422", setup: func(f *fixture) {
			f.promotion("SOON", func(p *domain.Promotion) { p.ValidFrom = goldenNow.Add(time.Hour) })
		}, req: step{method: "GET", path: v + "SOON", role: "rider"}, want: rej("not_started", "this code is not valid yet", 422)},
		{name: "expired is 422", setup: func(f *fixture) {
			f.promotion("OLD", func(p *domain.Promotion) { p.ValidTo = goldenNow.Add(-time.Minute) })
		}, req: step{method: "GET", path: v + "OLD", role: "rider"}, want: rej("expired", "this code has expired", 422)},
		{name: "min_fare is 422", setup: func(f *fixture) {
			f.promotion("BIGONLY", func(p *domain.Promotion) { p.MinFare = 100000 })
		}, req: step{method: "GET", path: v + "BIGONLY", role: "rider"}, want: rej("min_fare", "this code needs a fare of at least 100000 VND", 422)},
		{name: "wrong_vehicle is 422", setup: func(f *fixture) {
			f.promotion("BIKEONLY", func(p *domain.Promotion) { b := "BIKE"; p.VehicleType = &b })
		}, req: step{method: "GET", path: v + "BIKEONLY&vehicleType=CAR", role: "rider"}, want: rej("wrong_vehicle", "this code is for BIKE trips only", 422)},
		{name: "limit_reached is 422", setup: func(f *fixture) {
			f.promotion("FULL", func(p *domain.Promotion) { l := int64(5); p.UsageLimit = &l })
			f.store.usage["FULL|rider-1"] = domain.Usage{Total: 5}
		}, req: step{method: "GET", path: v + "FULL", role: "rider"}, want: rej("limit_reached", "this code has been fully redeemed", 422)},
		{name: "already_used is 422", setup: func(f *fixture) {
			f.promotion("ONCE", nil)
			f.store.usage["ONCE|rider-1"] = domain.Usage{Total: 1, ByRider: 1}
		}, req: step{method: "GET", path: v + "ONCE", role: "rider"}, want: rej("already_used", "you have already used this code", 422)},
		{name: "invalid request is 400", req: step{method: "GET", path: "/promotions/validate?code=NOPE1&price=abc", role: "rider"},
			want: rej("invalid_request", "price must be a number of VND", 400)},
		{name: "invalid promotion is 400", req: step{method: "POST", path: "/promotions", role: "admin", body: `{"code":"A"}`},
			want: rej("invalid_promotion", "code must be 3 to 32 characters: letters, digits, '-' or '_'", 400)},
		{name: "code exists is 409", setup: func(f *fixture) { f.promotion("GOLD10", nil) },
			req:  step{method: "POST", path: "/promotions", role: "admin", body: `{` + validPromo + `}`},
			want: rej("code_exists", "a promotion with this code already exists", 409)},
		{name: "disable of an unknown code is 404", req: step{method: "POST", path: "/promotions/NOPE/disable", role: "admin"},
			want: rej("not_found", "no such promotion code", 404)},
		{name: "a store error on validate is a 500 of its step", setup: func(f *fixture) { f.store.failOn["Find"] = boom },
			req: step{method: "GET", path: v + "ABC", role: "rider"}, want: exp{500, jsonType, bodyInternal}, log: "load promotion: boom\n"},
		{name: "a usage error on validate", setup: func(f *fixture) {
			f.promotion("ABC", nil)
			f.store.failOn["Usage"] = boom
		}, req: step{method: "GET", path: v + "ABC", role: "rider"}, want: exp{500, jsonType, bodyInternal}, log: "count redemptions: boom\n"},
		{name: "a create error", setup: func(f *fixture) { f.store.failOn["Create"] = boom },
			req:  step{method: "POST", path: "/promotions", role: "admin", body: `{` + validPromo + `}`},
			want: exp{500, jsonType, bodyInternal}, log: "create promotion: boom\n"},
		{name: "a view that vanishes after a create is a 500, not a 404", setup: func(f *fixture) { f.store.failOn["View"] = domain.ErrNotFound },
			req:  step{method: "POST", path: "/promotions", role: "admin", body: `{` + validPromo + `}`},
			want: exp{500, jsonType, bodyInternal}, log: "load promotion: no such promotion\n"},
		{name: "a view that vanishes after a disable is a 500, not a 404", setup: func(f *fixture) {
			f.promotion("GOLD10", nil)
			f.store.failOn["View"] = domain.ErrNotFound
		}, req: step{method: "POST", path: "/promotions/GOLD10/disable", role: "admin"},
			want: exp{500, jsonType, bodyInternal}, log: "load promotion: no such promotion\n"},
		{name: "a disable error", setup: func(f *fixture) { f.store.failOn["Disable"] = boom },
			req:  step{method: "POST", path: "/promotions/ABC/disable", role: "admin"},
			want: exp{500, jsonType, bodyInternal}, log: "disable promotion: boom\n"},
		{name: "a list error", setup: func(f *fixture) { f.store.failOn["List"] = boom },
			req:  step{method: "GET", path: "/promotions", role: "admin"},
			want: exp{500, jsonType, bodyInternal}, log: "list promotions: boom\n"},
		{name: "a row that failed to scan keeps its step", setup: func(f *fixture) {
			f.store.failOn["List"] = &app.StepError{Step: "read promotion", Err: boom}
		}, req: step{method: "GET", path: "/promotions", role: "admin"},
			want: exp{500, jsonType, bodyInternal}, log: "read promotion: boom\n"},
	}
	for _, c := range cases {
		f := newFixture(t)
		if c.setup != nil {
			c.setup(f)
		}
		buf := captureLog(t)
		c.req.name = c.name
		c.req.want = c.want
		f.check(t, c.req, false)
		if got := buf.String(); got != c.log {
			t.Errorf("%s: log %q, want %q", c.name, got, c.log)
		}
	}
}

func TestAnErrorOfNoKnownTypeIsAnInternalError(t *testing.T) {
	buf := captureLog(t)
	rec := httptest.NewRecorder()
	writeError(rec, "validate promotion", errors.New("odd"))
	if rec.Code != 500 || rec.Body.String() != bodyInternal || buf.String() != "validate promotion: odd\n" {
		t.Errorf("%d %q log %q", rec.Code, rec.Body.String(), buf.String())
	}
}

func TestTheCreateBodyIsReadUpTo16KiB(t *testing.T) {
	f := newFixture(t)
	// A document longer than 16 KiB is cut off in the middle: invalid JSON.
	rec := f.do(t, step{method: "POST", path: "/promotions", role: "admin", body: `{"code":"` + strings.Repeat("A", 17<<10) + `"}`})
	if rec.Code != 400 || rec.Body.String() != "{\"code\":\"invalid_request\",\"error\":\"invalid JSON body\"}\n" {
		t.Errorf("oversized: %d %q", rec.Code, rec.Body.String())
	}
}

func TestTheValidateRouteIsLimitedAndTheAdminRoutesAreNot(t *testing.T) {
	f := newFixture(t)
	t.Setenv(auth.RateLimitScaleEnv, "1") // read when Routes() builds the mux
	f.api = rebuild(t, f)
	validate := step{method: "GET", path: "/promotions/validate?code=ab&price=1", role: "rider"}
	var last int
	for i := 0; i < 12; i++ {
		last = f.do(t, validate).Code
	}
	if last != http.StatusTooManyRequests {
		t.Errorf("12th validate: %d, want 429", last)
	}
	for i := 0; i < 30; i++ {
		if code := f.do(t, step{method: "GET", path: "/promotions", role: "admin"}).Code; code != 200 {
			t.Fatalf("list %d: %d", i, code)
		}
	}
	t.Setenv(auth.RateLimitScaleEnv, "0")
	f.api = rebuild(t, f)
	for i := 0; i < 30; i++ {
		if code := f.do(t, validate).Code; code != 404 {
			t.Fatalf("unlimited validate %d: %d", i, code)
		}
	}
}

func rebuild(t *testing.T, f *fixture) http.Handler {
	t.Helper()
	// The same service and keys, a new mux: Routes() reads the limit again.
	return f.remux()
}

func TestRolesOfEachRoute(t *testing.T) {
	f := newFixture(t)
	f.promotion("GOLD10", nil)
	cases := []struct {
		method, path, role string
		want               int
	}{
		{"GET", "/promotions/validate?code=GOLD10&price=1000", "rider", 200},
		{"GET", "/promotions/validate?code=GOLD10&price=1000", "admin", 403},
		{"POST", "/promotions", "rider", 403},
		{"GET", "/promotions", "rider", 403},
		{"GET", "/promotions", "admin", 200},
		{"POST", "/promotions/GOLD10/disable", "rider", 403},
		{"POST", "/promotions/GOLD10/disable", "admin", 200},
		{"GET", "/promotions", "", 401},
		{"GET", "/health", "", 200},
	}
	for _, c := range cases {
		if got := f.do(t, step{method: c.method, path: c.path, role: c.role}).Code; got != c.want {
			t.Errorf("%s %s as %q: %d, want %d", c.method, c.path, c.role, got, c.want)
		}
	}
}
