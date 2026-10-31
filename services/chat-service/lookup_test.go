package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"grab-mini/pkg/auth"
)

// fakeTripService answers GET /trips/{id} and records the Authorization it saw.
type fakeTripService struct {
	*httptest.Server
	auth  chan string
	calls atomic.Int32
}

func newFakeTripService(t *testing.T, h func(w http.ResponseWriter, r *http.Request)) *fakeTripService {
	t.Helper()
	f := &fakeTripService{auth: make(chan string, 10)}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		f.auth <- r.Header.Get("Authorization")
		h(w, r)
	}))
	t.Cleanup(f.Close)
	return f
}

func tripJSON(id, rider, driver, status string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"id": id, "orderId": "o", "riderId": rider, "driverId": driver, "status": status})
	}
}

// lookupService is a service with no database: any attempt to store a trip panics,
// which is how the tests below show that nothing was cached.
func lookupService(t *testing.T, base string, timeout time.Duration) *service {
	t.Helper()
	l, err := newTripLookup(base, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return &service{lookup: l, now: time.Now}
}

func lookups(result string) float64 { return testutil.ToFloat64(tripLookups.WithLabelValues(result)) }

func TestLookupForwardsTheCallersTokenAndReadsTheTrip(t *testing.T) {
	fake := newFakeTripService(t, tripJSON("trip-1", "rider-1", "driver-1", "ASSIGNED"))
	c, _ := newTripLookup(fake.URL, time.Second)
	got, err := c.get(context.Background(), "Bearer the-callers-token", "trip-1")
	if err != nil {
		t.Fatal(err)
	}
	if h := <-fake.auth; h != "Bearer the-callers-token" {
		t.Fatalf("Trip Service saw Authorization %q", h)
	}
	if got.ID != "trip-1" || got.RiderID != "rider-1" || got.DriverID != "driver-1" || got.Status != "ASSIGNED" {
		t.Fatalf("got %+v", got)
	}
}

func TestLookupSaysNotFoundOn404AndErrorOnTheRest(t *testing.T) {
	for _, c := range []struct {
		status int
		body   string
		notFnd bool
	}{
		{404, `{"error":"trip not found"}`, true},
		{500, `oops`, false},
		{503, ``, false},
		{401, ``, false},
		{200, `not json`, false},
	} {
		fake := newFakeTripService(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(c.status)
			w.Write([]byte(c.body))
		})
		l, _ := newTripLookup(fake.URL, time.Second)
		_, err := l.get(context.Background(), "Bearer x", "trip-1")
		if err == nil || errors.Is(err, errTripNotFound) != c.notFnd {
			t.Errorf("status %d: err %v, want notFound=%v", c.status, err, c.notFnd)
		}
	}
}

func TestLookupGivesUpAtTheTimeoutAndDoesNotRetry(t *testing.T) {
	fake := newFakeTripService(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	})
	s := lookupService(t, fake.URL, 50*time.Millisecond)
	before := lookups(lookupError)
	start := time.Now()
	_, err := s.learnTrip(context.Background(), auth.Identity{Role: auth.Rider, Subject: "rider-1"}, "Bearer x", "trip-1")
	if !errors.Is(err, errLookupUnavailable) {
		t.Fatalf("err %v, want unavailable", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %s", d)
	}
	if fake.calls.Load() != 1 {
		t.Fatalf("%d calls, want 1", fake.calls.Load())
	}
	if lookups(lookupError) != before+1 {
		t.Fatal("error not counted")
	}
}

func TestLearnTripMapsTheAnswers(t *testing.T) {
	rider := auth.Identity{Role: auth.Rider, Subject: "rider-1"}
	cases := []struct {
		name    string
		handler func(http.ResponseWriter, *http.Request)
		caller  auth.Identity
		want    error
		metric  string
	}{
		{"404", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "{}", 404) }, rider, errNoTrip, lookupNotFound},
		{"5xx", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "{}", 502) }, rider, errLookupUnavailable, lookupError},
		{"an answer for another trip", tripJSON("other", "rider-1", "driver-1", "ASSIGNED"), rider, errLookupUnavailable, lookupError},
		{"a status nobody knows", tripJSON("trip-1", "rider-1", "driver-1", "TELEPORTED"), rider, errLookupUnavailable, lookupError},
		{"a stranger rider", tripJSON("trip-1", "rider-2", "driver-1", "ASSIGNED"), rider, errLookupForbidden, lookupForbidden},
		{"a stranger driver", tripJSON("trip-1", "rider-2", "driver-1", "ASSIGNED"), auth.Identity{Role: auth.Driver, Subject: "driver-9"}, errLookupForbidden, lookupForbidden},
		{"a driver id used as a rider", tripJSON("trip-1", "rider-2", "rider-1", "ASSIGNED"), rider, errLookupForbidden, lookupForbidden},
	}
	for _, c := range cases {
		fake := newFakeTripService(t, c.handler)
		s := lookupService(t, fake.URL, time.Second) // no database: storing anything would panic
		before := lookups(c.metric)
		_, err := s.learnTrip(context.Background(), c.caller, "Bearer x", "trip-1")
		if !errors.Is(err, c.want) {
			t.Errorf("%s: err %v, want %v", c.name, err, c.want)
		}
		if lookups(c.metric) != before+1 {
			t.Errorf("%s: chat_trip_lookups_total{result=%q} did not move", c.name, c.metric)
		}
	}
}

func TestWithoutATripServiceURLAnUnknownTripIsJustUnknown(t *testing.T) {
	s := lookupService(t, "", time.Second)
	if _, err := s.learnTrip(context.Background(), auth.Identity{Role: auth.Rider, Subject: "r"}, "Bearer x", "t"); !errors.Is(err, errNoTrip) {
		t.Fatal(err)
	}
	if _, err := newTripLookup("ftp://x", time.Second); err == nil {
		t.Fatal("a non-http URL was accepted")
	}
}
