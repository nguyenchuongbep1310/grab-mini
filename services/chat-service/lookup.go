package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/tracing"
)

var tripLookups = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "chat_trip_lookups_total",
	Help: "Lookups of a trip this service had not heard of (no trip.status.changed yet: a trip is ASSIGNED without an event) in Trip Service, by result: found (the caller is on it and it is now known), not_found (404), forbidden (the answer does not name the caller), error (Trip Service down, slow or answering something else).",
}, []string{"result"})

// Results of a lookup, as the label of chat_trip_lookups_total.
const (
	lookupFound     = "found"
	lookupNotFound  = "not_found"
	lookupForbidden = "forbidden"
	lookupError     = "error"
)

var errTripNotFound = errors.New("trip service: no such trip")

// tripLookup asks Trip Service about a trip. Trip Service creates a trip as
// ASSIGNED without publishing trip.status.changed (it does so on pickup,
// completion and cancellation), so for the time between the match and the pickup,
// exactly when rider and driver need to talk, the event has not told this service
// who is on the trip. It forwards the caller's own bearer token, as Pricing does
// to Routing: there is no service-to-service credential here, and Trip Service
// answers only a rider or driver of the trip.
type tripLookup struct {
	base    string
	timeout time.Duration
	hc      *http.Client
}

// newTripLookup returns nil when base is empty: lookups are then off and a trip
// is known only from its events.
func newTripLookup(base string, timeout time.Duration) (*tripLookup, error) {
	if base == "" {
		return nil, nil
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("TRIP_SERVICE_URL %q is not an http(s) URL", base)
	}
	return &tripLookup{base: u.String(), timeout: timeout, hc: &http.Client{Transport: tracing.Transport(nil)}}, nil
}

// tripAnswer is the part of Trip Service's GET /trips/{id} this service uses.
type tripAnswer struct {
	ID       string            `json:"id"`
	RiderID  string            `json:"riderId"`
	DriverID string            `json:"driverId"`
	Status   events.TripStatus `json:"status"`
}

// get asks once, with the timeout and no retry. authorization is the caller's
// Authorization header, forwarded as is and never logged.
func (c *tripLookup) get(ctx context.Context, authorization, tripID string) (tripAnswer, error) {
	var out tripAnswer
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/trips/"+url.PathEscape(tripID), nil)
	if err != nil {
		return out, err
	}
	req.Header.Set("Authorization", authorization)
	resp, err := c.hc.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return out, errTripNotFound
	default:
		return out, fmt.Errorf("trip service answered %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return out, fmt.Errorf("trip service answer: %w", err)
	}
	return out, nil
}

// lookupErrors of learnTrip, besides errNoTrip.
var (
	errLookupUnavailable = errors.New("trip lookup unavailable")
	errLookupForbidden   = errors.New("not on this trip")
)

// learnTrip is what happens on a miss in chat_trips: ask Trip Service once and,
// if it names the caller as the trip's rider or driver, remember the trip. It
// returns errNoTrip for a trip Trip Service does not know (or will not show this
// caller: it answers 404 to a stranger), errLookupForbidden for an answer that
// does not name the caller (nothing is stored), errLookupUnavailable when Trip
// Service could not answer in time, or a database error.
func (s *service) learnTrip(ctx context.Context, caller auth.Identity, authorization, tripID string) (chatTrip, error) {
	if s.lookup == nil {
		return chatTrip{}, errNoTrip
	}
	ans, err := s.lookup.get(ctx, authorization, tripID)
	switch {
	case errors.Is(err, errTripNotFound):
		tripLookups.WithLabelValues(lookupNotFound).Inc()
		return chatTrip{}, errNoTrip
	case err != nil:
		tripLookups.WithLabelValues(lookupError).Inc()
		log.Printf("trip %s: lookup in trip service failed: %v", tripID, err)
		return chatTrip{}, errLookupUnavailable
	}
	t := chatTrip{ID: tripID, RiderID: ans.RiderID, DriverID: ans.DriverID, Status: ans.Status}
	if ans.ID != tripID || ans.DriverID == "" || rank(ans.Status) == 0 {
		tripLookups.WithLabelValues(lookupError).Inc()
		log.Printf("trip %s: trip service answered an unusable trip (id %q, status %q)", tripID, ans.ID, ans.Status)
		return chatTrip{}, errLookupUnavailable
	}
	if t.roleOf(string(caller.Role), caller.Subject) == "" {
		tripLookups.WithLabelValues(lookupForbidden).Inc()
		return chatTrip{}, errLookupForbidden
	}

	at := s.now()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return chatTrip{}, err
	}
	defer tx.Rollback(ctx)
	if _, _, err := upsertTrip(ctx, tx, tripID, ans.RiderID, ans.DriverID, ans.Status, at); err != nil {
		return chatTrip{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return chatTrip{}, err
	}
	tripLookups.WithLabelValues(lookupFound).Inc()
	// Read back: an event may have got there first, and its row wins where it is further on.
	return findTrip(ctx, s.db, tripID)
}
