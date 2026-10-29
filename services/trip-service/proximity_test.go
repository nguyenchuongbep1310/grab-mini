package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/auth/authtest"
	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadProximityDefaults(t *testing.T) {
	p, err := loadProximity(env(nil))
	if err != nil || p != defaultProximity() {
		t.Fatalf("got %+v, %v; want the defaults", p, err)
	}
	if p.PickupRadiusM != 200 || p.DropoffRadiusM != 300 || p.MaxLocationAge != time.Minute || p.Off {
		t.Errorf("defaults are %+v, want 200m / 300m / 60s / on", p)
	}
}

func TestLoadProximityReadsTheRadii(t *testing.T) {
	p, err := loadProximity(env(map[string]string{"PICKUP_RADIUS_M": "50.5", "DROPOFF_RADIUS_M": "1000", "LOCATION_MAX_AGE": "15s"}))
	if err != nil || p.PickupRadiusM != 50.5 || p.DropoffRadiusM != 1000 || p.MaxLocationAge != 15*time.Second {
		t.Errorf("got %+v, %v", p, err)
	}
	for _, bad := range []map[string]string{
		{"PICKUP_RADIUS_M": "abc"}, {"PICKUP_RADIUS_M": "0"}, {"DROPOFF_RADIUS_M": "-5"},
		{"LOCATION_MAX_AGE": "soon"}, {"LOCATION_MAX_AGE": "0s"}, {"PROXIMITY_CHECK": "maybe"},
	} {
		if _, err := loadProximity(env(bad)); err == nil {
			t.Errorf("%v was accepted", bad)
		}
	}
}

func TestTheCheckCanOnlyBeTurnedOffInDevelopment(t *testing.T) {
	for _, appEnv := range []string{"", "production", "staging", "Development "} {
		if _, err := loadProximity(env(map[string]string{"PROXIMITY_CHECK": "off", "APP_ENV": appEnv})); err == nil {
			t.Errorf("PROXIMITY_CHECK=off was accepted with APP_ENV=%q", appEnv)
		}
	}
	p, err := loadProximity(env(map[string]string{"PROXIMITY_CHECK": "off", "APP_ENV": "development"}))
	if err != nil || !p.Off {
		t.Errorf("development + off = %+v, %v; want the check off", p, err)
	}
	if p, err := loadProximity(env(map[string]string{"PROXIMITY_CHECK": "on", "APP_ENV": "production"})); err != nil || p.Off {
		t.Errorf("explicit on = %+v, %v", p, err)
	}
}

func TestATripWithoutARecordedRouteSkipsTheCheck(t *testing.T) {
	// No Redis client is needed: nothing is read when there is no target.
	if r := checkProximity(context.Background(), nil, defaultProximity(), time.Now(), "pickup", "d1", nil, 200); r != nil {
		t.Errorf("refused a legacy trip: %+v", r)
	}
	off := defaultProximity()
	off.Off = true
	if r := checkProximity(context.Background(), nil, off, time.Now(), "pickup", "d1", &events.LatLng{}, 200); r != nil {
		t.Errorf("refused with the check off: %+v", r)
	}
}

// ---- integration (Postgres and Redis) ----

var (
	pickupPoint  = events.LatLng{Lat: 10.7760, Lng: 106.7000}
	dropoffPoint = events.LatLng{Lat: 10.7900, Lng: 106.7100}
)

// fakeRegistry is just enough Schema Registry for the codec to encode.
func fakeRegistry(t *testing.T) *eventcodec.Codec {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int{"id": 1})
	}))
	t.Cleanup(srv.Close)
	return eventcodec.New(srv.URL)
}

type proxFixture struct {
	t      *testing.T
	db     *pgxpool.Pool
	rdb    *redis.Client
	s      *server
	mux    *http.ServeMux
	issuer *auth.Issuer
	trip   *trip
	now    time.Time
}

func newProxFixture(t *testing.T) *proxFixture {
	t.Helper()
	db, rdb := testEnv(t)
	ctx := context.Background()
	evt := matched("o1", "driver-1")
	evt.Pickup, evt.Dropoff = &pickupPoint, &dropoffPoint
	if err := handleOrderMatched(ctx, db, rdb, evt); err != nil {
		t.Fatal(err)
	}
	tr, _ := getTripByOrderID(ctx, db, "o1")
	s, _ := newServer(db, rdb)
	s.codec = fakeRegistry(t)
	f := &proxFixture{t: t, db: db, rdb: rdb, s: s, trip: tr, now: time.Now()}
	s.now = func() time.Time { return f.now }

	issuer, verifier := authtest.Pair(t, time.Hour)
	f.issuer = issuer
	f.mux = s.routes(verifier)
	return f
}

// at reports the driver's position as of age ago. Each call moves the fixture clock
// on, because Update ignores a position that is not newer than the last.
func (f *proxFixture) at(p events.LatLng, age time.Duration) {
	f.t.Helper()
	f.now = f.now.Add(10 * time.Minute)
	if _, err := driverlocation.Update(context.Background(), f.rdb, "driver-1", p.Lat, p.Lng, f.now.Add(-age).UnixMilli()); err != nil {
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

func (f *proxFixture) status() events.TripStatus {
	got, _ := getTripByID(context.Background(), f.db, f.trip.ID)
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
	if f.status() != events.TripStatusAssigned {
		t.Errorf("a refused pickup moved the trip to %s", f.status())
	}
}

func TestPickupNearThePickupPointWorks(t *testing.T) {
	f := newProxFixture(t)
	f.at(metresNorth(pickupPoint, 150), time.Second)

	if code, body := f.post("pickup"); code != http.StatusOK {
		t.Fatalf("pickup 150 m away = %d %v, want 200", code, body)
	}
	if f.status() != events.TripStatusInProgress {
		t.Errorf("trip is %s, want IN_PROGRESS", f.status())
	}
}

func TestThePickupRadiusIsTheEdge(t *testing.T) {
	f := newProxFixture(t)
	f.at(metresNorth(pickupPoint, 230), time.Second)
	if code, _ := f.post("pickup"); code != http.StatusConflict {
		t.Errorf("230 m away = %d, want 409 with the default 200 m", code)
	}
	f.s.prox = defaultProximity()
	f.s.prox.PickupRadiusM = 250
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
	if f.status() != events.TripStatusInProgress {
		t.Errorf("a refused complete moved the trip to %s", f.status())
	}
	// 250 m from the dropoff is inside DROPOFF_RADIUS_M (300) but outside the pickup radius (200).
	f.at(metresNorth(dropoffPoint, 250), time.Second)
	if code, body := f.post("complete"); code != http.StatusOK {
		t.Fatalf("complete 250 m from the dropoff = %d %v, want 200", code, body)
	}
	if f.status() != events.TripStatusCompleted {
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
	f.s.prox = defaultProximity()
	f.s.prox.Off = true
	if code, _ := f.post("pickup"); code != http.StatusOK {
		t.Errorf("pickup with PROXIMITY_CHECK=off and no location = %d, want 200", code)
	}
}

func TestATripWithoutARouteIsNotBlocked(t *testing.T) {
	f := newProxFixture(t)
	f.db.Exec(context.Background(), `UPDATE trips SET pickup_lat = NULL, pickup_lng = NULL, dropoff_lat = NULL, dropoff_lng = NULL WHERE id = $1`, f.trip.ID)
	if code, _ := f.post("pickup"); code != http.StatusOK {
		t.Errorf("pickup of a trip with no recorded route = %d, want 200", code)
	}
}

func TestTheRouteIsStoredWithTheTrip(t *testing.T) {
	f := newProxFixture(t)
	r, err := getTripRoute(context.Background(), f.db, f.trip.ID)
	if err != nil || r.Pickup == nil || *r.Pickup != pickupPoint || r.Dropoff == nil || *r.Dropoff != dropoffPoint {
		t.Errorf("route = %+v, %v; want %v -> %v", r, err, pickupPoint, dropoffPoint)
	}
}
