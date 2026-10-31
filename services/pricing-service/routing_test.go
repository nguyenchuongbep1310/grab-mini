package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/surge"
)

const riderAuth = "Bearer rider-token"

// straightFare is what a quote of `trip` costs without Routing.
func straightFare() float64 {
	return surge.BaseFare("CAR", surge.HaversineKm(10.776, 106.72, 10.82, 106.75))
}

// quoteWith asks s for a quote of `trip`, as rider-1 presenting authorization.
func quoteWith(t *testing.T, s *quoteServer, authorization string) surge.Quote {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/quotes?"+trip+"&vehicleType=CAR", nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	req = req.WithContext(auth.With(req.Context(), auth.Identity{Subject: "rider-1", Role: auth.Rider}))
	rec := httptest.NewRecorder()
	s.handleQuote(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: a quote must always be issued: %s", rec.Code, rec.Body)
	}
	var q surge.Quote
	if err := json.Unmarshal(rec.Body.Bytes(), &q); err != nil {
		t.Fatal(err)
	}
	return q
}

func routedServer(t *testing.T, routing *routingClient) *quoteServer {
	t.Helper()
	_, rdb := newRedis(t)
	s := quoteServerWith(t, rdb)
	s.routing = routing
	return s
}

func clientFor(t *testing.T, url string, timeout time.Duration) *routingClient {
	t.Helper()
	c, err := newRoutingClient(url, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func failOpens(reason string) float64 {
	return testutil.ToFloat64(routingFailOpen.WithLabelValues(reason))
}

func TestQuoteIsPricedOnTheRoutedDistance(t *testing.T) {
	var gotAuth, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotQuery = r.Header.Get("Authorization"), r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"distanceKm":20,"durationSec":2570,"polyline":[],"vehicle":"CAR","model":"detour-1.3"}`)
	}))
	defer srv.Close()
	before := failOpens(reasonDisabled) + failOpens(reasonTimeout) + failOpens(reasonError)

	s := routedServer(t, clientFor(t, srv.URL, time.Second))
	q := quoteWith(t, s, riderAuth)

	if want := surge.BaseFare("CAR", 20); q.BaseFare != want {
		t.Errorf("BaseFare = %v, want %v (20 km by road)", q.BaseFare, want)
	}
	if q.BaseFare == straightFare() {
		t.Errorf("the quote ignored the routed distance: %v equals the straight-line fare", q.BaseFare)
	}
	if gotAuth != riderAuth {
		t.Errorf("Routing received Authorization %q, want the rider's own %q", gotAuth, riderAuth)
	}
	for _, want := range []string{"fromLat=10.776", "fromLng=106.72", "toLat=10.82", "toLng=106.75", "vehicle=CAR"} {
		if !strings.Contains(gotQuery, want) {
			t.Errorf("Routing was asked %q, missing %q", gotQuery, want)
		}
	}
	if after := failOpens(reasonDisabled) + failOpens(reasonTimeout) + failOpens(reasonError); after != before {
		t.Errorf("a routed quote counted a fail-open (%v -> %v)", before, after)
	}
	// The quote is still verifiable by Order Service with the shared secret.
	signer, _ := surge.NewSigner("test-secret")
	if _, err := signer.Verify(q.QuoteID, surge.QuoteRequest{RiderID: "rider-1", Vehicle: "CAR", PickupLat: 10.776, PickupLng: 106.72, DropoffLat: 10.82, DropoffLng: 106.75}, t0.Add(time.Second)); err != nil {
		t.Errorf("verify: %v", err)
	}
}

func TestQuoteFailsOpenToTheStraightLine(t *testing.T) {
	var calls atomic.Int32
	reply := func(code int, body string, delay time.Duration) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			time.Sleep(delay)
			w.WriteHeader(code)
			fmt.Fprint(w, body)
		}))
	}
	slow := reply(200, `{"distanceKm":20}`, 400*time.Millisecond)
	defer slow.Close()
	broken := reply(500, `oops`, 0)
	defer broken.Close()
	garbage := reply(200, `not json`, 0)
	defer garbage.Close()
	negative := reply(200, `{"distanceKm":-5}`, 0)
	defer negative.Close()
	unauthorized := reply(401, `{"error":"invalid or expired token"}`, 0)
	defer unauthorized.Close()
	tooFar := reply(422, `{"error":"too far"}`, 0)
	defer tooFar.Close()
	gone := httptest.NewServer(http.NotFoundHandler())
	goneURL := gone.URL
	gone.Close() // nothing listens any more: connection refused

	for _, c := range []struct {
		name          string
		client        *routingClient
		authorization string
		reason        string
	}{
		{"routing is disabled", nil, riderAuth, reasonDisabled},
		{"the request has no token to forward", clientFor(t, slow.URL, time.Second), "", reasonDisabled},
		{"routing is slower than the timeout", clientFor(t, slow.URL, 100*time.Millisecond), riderAuth, reasonTimeout},
		{"routing is down", clientFor(t, goneURL, time.Second), riderAuth, reasonError},
		{"routing answers 500", clientFor(t, broken.URL, time.Second), riderAuth, reasonError},
		{"routing answers 401", clientFor(t, unauthorized.URL, time.Second), riderAuth, reasonError},
		{"routing answers garbage", clientFor(t, garbage.URL, time.Second), riderAuth, reasonError},
		{"routing answers a negative distance", clientFor(t, negative.URL, time.Second), riderAuth, reasonError},
		{"the trip is beyond what routing supports", clientFor(t, tooFar.URL, time.Second), riderAuth, reasonUnsupported},
	} {
		before := failOpens(c.reason)
		start := time.Now()
		q := routedServer(t, c.client)
		quote := quoteWith(t, q, c.authorization)
		if quote.BaseFare != straightFare() {
			t.Errorf("%s: BaseFare = %v, want the straight-line fare %v", c.name, quote.BaseFare, straightFare())
		}
		if got := failOpens(c.reason) - before; got != 1 {
			t.Errorf("%s: pricing_routing_fail_open_total{reason=%q} rose by %v, want 1", c.name, c.reason, got)
		}
		if took := time.Since(start); took > 350*time.Millisecond {
			t.Errorf("%s: the quote took %v; Routing must not hold a quote past its timeout", c.name, took)
		}
	}
	if calls.Load() == 0 {
		t.Error("the fake Routing was never called")
	}
}

func TestRoutingURLConfiguration(t *testing.T) {
	if c, err := newRoutingClient("", time.Second); c != nil || err != nil {
		t.Errorf("empty URL = %v, %v; want routing off, no error", c, err)
	}
	for _, bad := range []string{"routing-service:3009", "ftp://x", "http://", "::"} {
		if _, err := newRoutingClient(bad, time.Second); err == nil {
			t.Errorf("%q was accepted as ROUTING_URL", bad)
		}
	}

	t.Setenv("ROUTING_URL", "")
	if got := loadEnv().RoutingURL; got != "" {
		t.Errorf("ROUTING_URL set to empty gave %q, want it off", got)
	}
}

func TestRoutingDefaultsToLocalhost(t *testing.T) {
	// Unset (not empty) is the local default; t.Setenv can't unset, so check the helper.
	if got := getEnvAllowEmpty("ROUTING_URL_NOT_SET_ANYWHERE", "http://localhost:3009"); got != "http://localhost:3009" {
		t.Errorf("default = %q", got)
	}
	if got := loadEnv().RoutingTimeout; got != 300*time.Millisecond {
		t.Errorf("default timeout = %v, want 300ms", got)
	}
}
