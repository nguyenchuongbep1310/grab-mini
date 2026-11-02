package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/auth/authtest"
	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/events"
	"grab-mini/trip-service/internal/domain"
)

var (
	pickupPoint  = events.LatLng{Lat: 10.7760, Lng: 106.7000}
	dropoffPoint = events.LatLng{Lat: 10.7900, Lng: 106.7100}
)

type proxFixture struct {
	t        *testing.T
	s        *stack
	mux      *http.ServeMux
	issuer   *auth.Issuer
	verifier *auth.Verifier
	trip     domain.Trip
	now      time.Time
}

func newProxFixture(t *testing.T) *proxFixture {
	t.Helper()
	db, rdb := testEnv(t)
	ctx := context.Background()
	s := newStack(t, db, rdb)
	evt := matched("o1", "driver-1")
	evt.Pickup, evt.Dropoff = &pickupPoint, &dropoffPoint
	if err := s.events.OnOrderMatched(ctx, evt); err != nil {
		t.Fatal(err)
	}
	tr, _ := s.store.ByOrderID(ctx, "o1")
	f := &proxFixture{t: t, s: s, trip: tr, now: time.Now()}
	f.useProximity(domain.DefaultProximity())

	f.issuer, f.verifier = authtest.Pair(t, time.Hour)
	f.mux = s.api.Routes(f.verifier)
	return f
}

// useProximity changes the check's configuration, as another deployment would
// run it; the fixture clock stays the service's clock.
func (f *proxFixture) useProximity(p domain.Proximity) {
	f.s.rewire(p, func() time.Time { return f.now })
	if f.verifier != nil {
		f.mux = f.s.api.Routes(f.verifier)
	}
}

// at reports the driver's position as of age ago. Each call moves the fixture clock
// on, because Update ignores a position that is not newer than the last.
func (f *proxFixture) at(p events.LatLng, age time.Duration) {
	f.t.Helper()
	f.now = f.now.Add(10 * time.Minute)
	if _, err := driverlocation.Update(context.Background(), f.s.rdb, "driver-1", p.Lat, p.Lng, f.now.Add(-age).UnixMilli()); err != nil {
		f.t.Fatal(err)
	}
}

// metresNorth moves a point north by about m metres.
func metresNorth(p events.LatLng, m float64) events.LatLng {
	return events.LatLng{Lat: p.Lat + m/111_195, Lng: p.Lng}
}

func (f *proxFixture) post(step string) (int, map[string]any) {
	f.t.Helper()
	tok, _, _ := f.issuer.Issue(auth.Driver, "driver-1")
	req := httptest.NewRequest(http.MethodPost, "/trips/"+f.trip.ID+"/"+step, strings.NewReader(""))
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	f.mux.ServeHTTP(rec, req)
	var body map[string]any
	json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

func (f *proxFixture) status() domain.Status {
	got, _ := f.s.store.ByID(context.Background(), f.trip.ID)
	return got.Status
}

func TestPickupFromFarAwayIsRefused(t *testing.T) {
	f := newProxFixture(t)
	f.at(metresNorth(pickupPoint, 1500), time.Second)

	code, body := f.post("pickup")
	if code != http.StatusConflict || body["code"] != "driver_too_far" {
		t.Fatalf("pickup 1.5 km away = %d %v, want 409 driver_too_far", code, body)
	}
	if d, _ := body["distanceM"].(float64); d < 1400 || d > 1600 {
		t.Errorf("distanceM = %v, want about 1500", body["distanceM"])
	}
	if f.status() != domain.Assigned {
		t.Errorf("a refused pickup moved the trip to %s", f.status())
	}
}

func TestPickupNearThePickupPointWorks(t *testing.T) {
	f := newProxFixture(t)
	f.at(metresNorth(pickupPoint, 150), time.Second)

	if code, body := f.post("pickup"); code != http.StatusOK {
		t.Fatalf("pickup 150 m away = %d %v, want 200", code, body)
	}
	if f.status() != domain.InProgress {
		t.Errorf("trip is %s, want IN_PROGRESS", f.status())
	}
}

func TestThePickupRadiusIsTheEdge(t *testing.T) {
	f := newProxFixture(t)
	f.at(metresNorth(pickupPoint, 230), time.Second)
	if code, _ := f.post("pickup"); code != http.StatusConflict {
		t.Errorf("230 m away = %d, want 409 with the default 200 m", code)
	}
	wide := domain.DefaultProximity()
	wide.PickupRadiusM = 250
	f.useProximity(wide)
	if code, _ := f.post("pickup"); code != http.StatusOK {
		t.Errorf("230 m away with PICKUP_RADIUS_M=250 = %d, want 200", code)
	}
}

func TestPickupWithAStaleLocationIsRefusedDistinctly(t *testing.T) {
	f := newProxFixture(t)
	f.at(pickupPoint, 5*time.Minute) // standing right there, but the report is old

	code, body := f.post("pickup")
	if code != http.StatusConflict || body["code"] != "driver_location_stale" {
		t.Fatalf("pickup with a 5 min old location = %d %v, want 409 driver_location_stale", code, body)
	}
	f.at(pickupPoint, 2*time.Second)
	if code, _ := f.post("pickup"); code != http.StatusOK {
		t.Errorf("after a fresh report = %d, want 200", code)
	}
}

func TestPickupWithNoLocationAtAllIsRefusedDistinctly(t *testing.T) {
	f := newProxFixture(t)
	code, body := f.post("pickup")
	if code != http.StatusConflict || body["code"] != "driver_location_unknown" {
		t.Fatalf("pickup with no location = %d %v, want 409 driver_location_unknown", code, body)
	}
}

func TestCompleteChecksTheDropoff(t *testing.T) {
	f := newProxFixture(t)
	f.at(pickupPoint, time.Second)
	if code, _ := f.post("pickup"); code != http.StatusOK {
		t.Fatal("setup pickup failed")
	}

	// Still at the pickup: 1.9 km from the dropoff.
	code, body := f.post("complete")
	if code != http.StatusConflict || body["code"] != "driver_too_far" {
		t.Fatalf("complete from the pickup point = %d %v, want 409 driver_too_far", code, body)
	}
	if f.status() != domain.InProgress {
		t.Errorf("a refused complete moved the trip to %s", f.status())
	}
	// 250 m from the dropoff is inside DROPOFF_RADIUS_M (300) but outside the pickup radius (200).
	f.at(metresNorth(dropoffPoint, 250), time.Second)
	if code, body := f.post("complete"); code != http.StatusOK {
		t.Fatalf("complete 250 m from the dropoff = %d %v, want 200", code, body)
	}
	if f.status() != domain.Completed {
		t.Errorf("trip is %s, want COMPLETED", f.status())
	}
}

// A retry of a step that already happened is the idempotent no-op it always was,
// wherever the driver is now.
func TestRepeatingAFinishedPickupNeedsNoProximity(t *testing.T) {
	f := newProxFixture(t)
	f.at(pickupPoint, time.Second)
	if code, _ := f.post("pickup"); code != http.StatusOK {
		t.Fatal("setup pickup failed")
	}
	f.at(metresNorth(pickupPoint, 5000), time.Second)
	if code, _ := f.post("pickup"); code != http.StatusOK {
		t.Errorf("repeated pickup from far away = %d, want 200 (already IN_PROGRESS)", code)
	}
}

func TestACheckSwitchedOffLetsAnyPickupThrough(t *testing.T) {
	f := newProxFixture(t)
	f.useProximity(proximityOff())
	if code, _ := f.post("pickup"); code != http.StatusOK {
		t.Errorf("pickup with PROXIMITY_CHECK=off and no location = %d, want 200", code)
	}
}

func TestATripWithoutARouteIsNotBlocked(t *testing.T) {
	f := newProxFixture(t)
	f.s.db.Exec(context.Background(), `UPDATE trips SET pickup_lat = NULL, pickup_lng = NULL, dropoff_lat = NULL, dropoff_lng = NULL WHERE id = $1`, f.trip.ID)
	if code, _ := f.post("pickup"); code != http.StatusOK {
		t.Errorf("pickup of a trip with no recorded route = %d, want 200", code)
	}
}

func TestTheRouteIsStoredWithTheTrip(t *testing.T) {
	f := newProxFixture(t)
	r, err := f.s.store.Route(context.Background(), f.trip.ID)
	if err != nil || r.Pickup == nil || *r.Pickup != domain.LatLng(pickupPoint) || r.Dropoff == nil || *r.Dropoff != domain.LatLng(dropoffPoint) {
		t.Errorf("route = %+v, %v; want %v -> %v", r, err, pickupPoint, dropoffPoint)
	}
}
