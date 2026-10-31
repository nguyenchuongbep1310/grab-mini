package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/auth/authtest"
)

func newServer(t *testing.T, now time.Time) (http.Handler, *auth.Issuer) {
	t.Helper()
	issuer, verifier := authtest.Pair(t, time.Hour)
	s := &routeServer{now: func() time.Time { return now }}
	return s.routes(verifier), issuer
}

func get(t *testing.T, h http.Handler, issuer *auth.Issuer, role auth.Role, query string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/route?"+query, nil)
	if issuer != nil {
		tok, _, err := issuer.Issue(role, "x-1")
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

const trip = "fromLat=10.776&fromLng=106.700&toLat=10.790&toLng=106.710"

func TestRouteAnswersWithDistanceDurationAndPolyline(t *testing.T) {
	h, issuer := newServer(t, at(12, 0))
	rec := get(t, h, issuer, auth.Rider, trip+"&vehicle=BIKE")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "json") {
		t.Errorf("Content-Type = %q", ct)
	}
	var got struct {
		DistanceKm  float64 `json:"distanceKm"`
		DurationSec int     `json:"durationSec"`
		Polyline    []struct {
			Lat float64 `json:"lat"`
			Lng float64 `json:"lng"`
		} `json:"polyline"`
		Vehicle string `json:"vehicle"`
		Model   string `json:"model"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.DistanceKm <= 0 || got.DurationSec <= 0 || len(got.Polyline) < 2 || got.Vehicle != "BIKE" || got.Model != "detour-1.3" {
		t.Errorf("unexpected body: %+v", got)
	}
	if got.Polyline[0].Lat != 10.776 || got.Polyline[len(got.Polyline)-1].Lng != 106.710 {
		t.Errorf("polyline ends: %+v", got.Polyline)
	}
}

func TestRoutePeakMakesTheTripLonger(t *testing.T) {
	duration := func(now time.Time) int {
		h, issuer := newServer(t, now)
		var r Route
		if err := json.NewDecoder(get(t, h, issuer, auth.Rider, trip+"&vehicle=CAR").Body).Decode(&r); err != nil {
			t.Fatal(err)
		}
		return r.DurationSec
	}
	if peak, off := duration(at(8, 0)), duration(at(13, 0)); peak <= off {
		t.Errorf("peak %d s should be longer than off peak %d s", peak, off)
	}
}

func TestVehicleDefaultsToCar(t *testing.T) {
	h, issuer := newServer(t, at(12, 0))
	rec := get(t, h, issuer, auth.Rider, trip)
	var r Route
	json.NewDecoder(rec.Body).Decode(&r)
	if rec.Code != http.StatusOK || r.Vehicle != "CAR" {
		t.Errorf("status %d vehicle %q, want 200 CAR", rec.Code, r.Vehicle)
	}
}

func TestEveryRoleMayAsk(t *testing.T) {
	h, issuer := newServer(t, at(12, 0))
	for _, role := range []auth.Role{auth.Rider, auth.Driver, auth.Admin} {
		if rec := get(t, h, issuer, role, trip); rec.Code != http.StatusOK {
			t.Errorf("%s: status %d, want 200", role, rec.Code)
		}
	}
}

func TestRouteNeedsAValidToken(t *testing.T) {
	h, _ := newServer(t, at(12, 0))
	if rec := get(t, h, nil, "", trip); rec.Code != http.StatusUnauthorized {
		t.Errorf("no token: status %d, want 401", rec.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/route?"+trip, nil)
	req.Header.Set("Authorization", "Bearer not-a-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("bad token: status %d, want 401", rec.Code)
	}
	stranger := authtest.Stranger(t, time.Hour)
	if rec := get(t, h, stranger, auth.Rider, trip); rec.Code != http.StatusUnauthorized {
		t.Errorf("token from another issuer: status %d, want 401", rec.Code)
	}
}

func TestRouteRejectsBadInput(t *testing.T) {
	h, issuer := newServer(t, at(12, 0))
	for name, query := range map[string]string{
		"nothing":            "",
		"missing toLng":      "fromLat=10&fromLng=106&toLat=10.1",
		"not a number":       "fromLat=abc&fromLng=106&toLat=10.1&toLng=106",
		"NaN":                "fromLat=NaN&fromLng=106&toLat=10.1&toLng=106",
		"infinity":           "fromLat=Inf&fromLng=106&toLat=10.1&toLng=106",
		"latitude over 90":   "fromLat=91&fromLng=106&toLat=10.1&toLng=106",
		"latitude under -90": "fromLat=10&fromLng=106&toLat=-90.5&toLng=106",
		"longitude over 180": "fromLat=10&fromLng=181&toLat=10.1&toLng=106",
		"repeated parameter": "fromLat=10&fromLat=11&fromLng=106&toLat=10.1&toLng=106",
		"unknown vehicle":    trip + "&vehicle=TRUCK",
		"lowercase vehicle":  trip + "&vehicle=car",
	} {
		rec := get(t, h, issuer, auth.Rider, query)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400 (%s)", name, rec.Code, rec.Body)
		}
	}
}

func TestRouteRefusesADistanceBeyondTheCap(t *testing.T) {
	h, issuer := newServer(t, at(12, 0))
	rec := get(t, h, issuer, auth.Rider, "fromLat=10.776&fromLng=106.700&toLat=21.028&toLng=105.854")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("status %d, want 422: %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "200 km") {
		t.Errorf("body %q should name the limit", rec.Body)
	}
}

func TestOnlyGETIsServed(t *testing.T) {
	h, issuer := newServer(t, at(12, 0))
	tok, _, _ := issuer.Issue(auth.Rider, "x-1")
	req := httptest.NewRequest(http.MethodPost, "/route?"+trip, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST: status %d, want 405", rec.Code)
	}
}
