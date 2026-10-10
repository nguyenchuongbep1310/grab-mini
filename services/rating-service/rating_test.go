package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
)

func TestParseReviewQuery(t *testing.T) {
	limit, before, err := parseReviewQuery(url.Values{})
	if err != nil || limit != 20 || !before.IsZero() {
		t.Errorf("defaults = %d, %v, %v", limit, before, err)
	}
	if limit, _, _ := parseReviewQuery(url.Values{"limit": {"999"}}); limit != 50 {
		t.Errorf("limit capped at %d, want 50", limit)
	}
	for _, bad := range []url.Values{{"limit": {"0"}}, {"limit": {"x"}}, {"before": {"yesterday"}}} {
		if _, _, err := parseReviewQuery(bad); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
	if _, b, err := parseReviewQuery(url.Values{"before": {"2026-01-02T03:04:05Z"}}); err != nil || b.Year() != 2026 {
		t.Errorf("before = %v, %v", b, err)
	}
}

func TestReviewsAreForTheDriverThemselvesOnly(t *testing.T) {
	h := reviewsHandler(nil) // refused before the database is touched
	for name, id := range map[string]*auth.Identity{
		"no identity":    nil,
		"another driver": {Subject: "d2", Role: auth.Driver},
		"a rider":        {Subject: "d1", Role: auth.Rider},
	} {
		req := httptest.NewRequest(http.MethodGet, "/drivers/d1/reviews", nil)
		req.SetPathValue("id", "d1")
		if id != nil {
			req = req.WithContext(auth.With(req.Context(), *id))
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s: status = %d, want 403", name, rec.Code)
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/drivers/d1/reviews?limit=0", nil)
	req.SetPathValue("id", "d1")
	req = req.WithContext(auth.With(req.Context(), auth.Identity{Subject: "d1", Role: auth.Driver}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("bad limit: status = %d, want 400", rec.Code)
	}
}

func TestAverage(t *testing.T) {
	cases := []struct {
		sum, count int
		want       float64
	}{
		{0, 0, 0},
		{5, 1, 5},
		{14, 3, 4.67},
		{9, 2, 4.5},
		{1, 3, 0.33},
	}
	for _, c := range cases {
		if got := average(c.sum, c.count); got != c.want {
			t.Errorf("average(%d, %d) = %v, want %v", c.sum, c.count, got, c.want)
		}
	}
}

func TestValidateRejectsStarsOutsideTheScaleAsPermanent(t *testing.T) {
	for _, stars := range []int{-1, 0, 6, 99} {
		err := validate(events.TripRated{DriverID: "d", Stars: stars})
		var inv *invalidEvent
		if !errors.As(err, &inv) || !inv.Permanent() {
			t.Errorf("stars=%d: err = %v, want a permanent invalidEvent", stars, err)
		}
	}
	for stars := 1; stars <= 5; stars++ {
		if err := validate(events.TripRated{DriverID: "d", Stars: stars}); err != nil {
			t.Errorf("stars=%d: unexpected error %v", stars, err)
		}
	}
	var inv *invalidEvent
	if err := validate(events.TripRated{DriverID: "d", Stars: 5, TipAmount: -1}); !errors.As(err, &inv) || !inv.Permanent() {
		t.Errorf("negative tip: err = %v, want a permanent invalidEvent", err)
	}
	if err := validate(events.TripRated{DriverID: "d", Stars: 5, TipAmount: 20000}); err != nil {
		t.Errorf("a tip: unexpected error %v", err)
	}
	if validate(events.TripRated{Stars: 5}) == nil {
		t.Error("a rating with no driver should be rejected")
	}
}
