package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/surge"
)

const deg = 0.05

var t0 = time.Unix(1_800_000_000, 0)

func testEnv() Env {
	return Env{
		InstanceID: "test", K: 0.5, MaxMultiplier: 3, Alpha: 1, MaxStep: 0.5,
		WindowMinutes: 3, SampleInterval: 5 * time.Second, CellDegrees: deg,
	}
}

func newRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return mr, rdb
}

func order(vehicle events.VehicleType, lat, lng float64) events.OrderCreated {
	return events.OrderCreated{OrderID: "o", RiderID: "r", Pickup: events.LatLng{Lat: lat, Lng: lng}, VehicleType: vehicle}
}

func fixedSupply(counts map[string]int) supplyFunc {
	return func(context.Context, string) (map[string]int, error) { return counts, nil }
}

func TestRecordDemandCountsPerVehicleCellAndMinute(t *testing.T) {
	_, rdb := newRedis(t)
	ctx := context.Background()
	cell := surge.CellID(deg, 10.776, 106.72)

	for i := 0; i < 3; i++ {
		if err := recordDemand(ctx, rdb, deg, order(events.VehicleCar, 10.776, 106.72), t0); err != nil {
			t.Fatal(err)
		}
	}
	recordDemand(ctx, rdb, deg, order(events.VehicleBike, 10.776, 106.72), t0)
	recordDemand(ctx, rdb, deg, order(events.VehicleCar, 10.776, 106.72), t0.Add(time.Minute))

	if n, _ := rdb.Get(ctx, surge.DemandKey("CAR", cell, surge.Minute(t0))).Int(); n != 3 {
		t.Errorf("CAR this minute = %d, want 3", n)
	}
	if n, _ := rdb.Get(ctx, surge.DemandKey("BIKE", cell, surge.Minute(t0))).Int(); n != 1 {
		t.Errorf("BIKE this minute = %d, want 1", n)
	}
	if ttl := rdb.TTL(ctx, surge.DemandKey("CAR", cell, surge.Minute(t0))).Val(); ttl <= 0 || ttl > surge.DemandTTL {
		t.Errorf("demand counter ttl = %v, want up to 5m", ttl)
	}
	members := rdb.SMembers(ctx, surge.CellsKey).Val()
	if len(members) != 2 {
		t.Errorf("active cells = %v, want CAR and BIKE", members)
	}

	// Averaged over the 3-minute window: (1 + 3) / 3 orders a minute.
	d, err := demandPerMin(ctx, rdb, "CAR", cell, 3, t0.Add(time.Minute))
	if err != nil || d < 1.33 || d > 1.34 {
		t.Errorf("demandPerMin = %v, %v, want 4/3", d, err)
	}
}

func TestRecordDemandTreatsAnUnknownVehicleAsCar(t *testing.T) {
	_, rdb := newRedis(t)
	ctx := context.Background()
	recordDemand(ctx, rdb, deg, order("", 10.776, 106.72), t0)
	cell := surge.CellID(deg, 10.776, 106.72)
	if n, _ := rdb.Get(ctx, surge.DemandKey("CAR", cell, surge.Minute(t0))).Int(); n != 1 {
		t.Errorf("CAR = %d, want 1", n)
	}
}

func TestSamplerRaisesTheMultiplierWhereDemandOutrunsSupplyAndStoresItWithATTL(t *testing.T) {
	mr, rdb := newRedis(t)
	ctx := context.Background()
	cell := surge.CellID(deg, 10.776, 106.72)
	for i := 0; i < 6; i++ { // 6 orders in a 3-minute window = 2/min
		recordDemand(ctx, rdb, deg, order(events.VehicleCar, 10.776, 106.72), t0)
	}
	s := &sampler{rdb: rdb, env: testEnv(), supply: fixedSupply(map[string]int{"CAR": 1, "BIKE": 50}), now: func() time.Time { return t0 }}

	if err := s.tick(ctx); err != nil {
		t.Fatal(err)
	}
	// demand 2/min, supply 1: 1 + 0.5*(2-1) = 1.5
	got, err := rdb.Get(ctx, surge.MultKey("CAR", cell)).Result()
	if err != nil || got != "1.5" {
		t.Fatalf("multiplier = %q, %v, want 1.5", got, err)
	}
	if ttl := mr.TTL(surge.MultKey("CAR", cell)); ttl != surge.MultTTL {
		t.Errorf("ttl = %v, want %v", ttl, surge.MultTTL)
	}

	// Without a refresh the key lapses and the price falls back to 1.0.
	mr.FastForward(surge.MultTTL + time.Second)
	if m := currentMultiplier(ctx, rdb, "CAR", cell); m != 1.0 {
		t.Errorf("after expiry multiplier = %v, want 1.0", m)
	}
}

func TestSamplerLimitsTheChangePerTickAndSmooths(t *testing.T) {
	_, rdb := newRedis(t)
	ctx := context.Background()
	cell := surge.CellID(deg, 10.776, 106.72)
	for i := 0; i < 300; i++ {
		recordDemand(ctx, rdb, deg, order(events.VehicleCar, 10.776, 106.72), t0)
	}
	env := testEnv()
	env.MaxStep = 0.3
	env.SampleInterval = 2 * time.Second
	s := &sampler{rdb: rdb, env: env, supply: fixedSupply(nil), now: func() time.Time { return t0 }}

	var seen []string
	for i := 0; i < 3; i++ {
		rdb.Del(ctx, lockKey) // each tick is a new interval
		if err := s.tick(ctx); err != nil {
			t.Fatal(err)
		}
		seen = append(seen, rdb.Get(ctx, surge.MultKey("CAR", cell)).Val())
	}
	if seen[0] != "1.3" || seen[1] != "1.6" || seen[2] != "1.9" {
		t.Errorf("multipliers over three ticks = %v, want a 0.3 step each", seen)
	}
}

func TestOnlyOneInstanceSamplesPerInterval(t *testing.T) {
	_, rdb := newRedis(t)
	ctx := context.Background()
	cell := surge.CellID(deg, 10.776, 106.72)
	for i := 0; i < 300; i++ {
		recordDemand(ctx, rdb, deg, order(events.VehicleCar, 10.776, 106.72), t0)
	}
	a := &sampler{rdb: rdb, env: testEnv(), supply: fixedSupply(nil), now: func() time.Time { return t0 }}
	b := &sampler{rdb: rdb, env: testEnv(), supply: fixedSupply(nil), now: func() time.Time { return t0 }}
	b.env.InstanceID = "other"
	b.env.MaxStep = 0.1
	a.env.MaxStep = 0.1

	a.tick(ctx)
	b.tick(ctx) // lock held by a: must be a no-op
	if got := rdb.Get(ctx, surge.MultKey("CAR", cell)).Val(); got != "1.1" {
		t.Errorf("multiplier = %s, want one step (1.1), not two", got)
	}
}

func TestACooledCellIsDroppedAndItsKeyRemoved(t *testing.T) {
	_, rdb := newRedis(t)
	ctx := context.Background()
	cell := surge.CellID(deg, 10.776, 106.72)
	rdb.SAdd(ctx, surge.CellsKey, surge.CellMember("CAR", cell))
	rdb.Set(ctx, surge.MultKey("CAR", cell), "1.0", time.Minute)
	s := &sampler{rdb: rdb, env: testEnv(), supply: fixedSupply(nil), now: func() time.Time { return t0 }}

	if err := s.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if n := rdb.Exists(ctx, surge.MultKey("CAR", cell)).Val(); n != 0 {
		t.Error("the multiplier key of a cell with no demand should be removed")
	}
	if n := rdb.SCard(ctx, surge.CellsKey).Val(); n != 0 {
		t.Errorf("cells set has %d members, want 0", n)
	}
}

func TestASupplyFailureKeepsTheCellAndCountsAnError(t *testing.T) {
	_, rdb := newRedis(t)
	ctx := context.Background()
	recordDemand(ctx, rdb, deg, order(events.VehicleCar, 10.776, 106.72), t0)
	failing := func(context.Context, string) (map[string]int, error) { return nil, context.DeadlineExceeded }
	s := &sampler{rdb: rdb, env: testEnv(), supply: failing, now: func() time.Time { return t0 }}
	if err := s.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if n := rdb.SCard(ctx, surge.CellsKey).Val(); n != 1 {
		t.Error("a failed sample must not forget the cell")
	}
}

// --- quotes ---

func quoteServerWith(t *testing.T, rdb *redis.Client) *quoteServer {
	t.Helper()
	signer, err := surge.NewSigner("test-secret")
	if err != nil {
		t.Fatal(err)
	}
	return &quoteServer{rdb: rdb, signer: signer, env: testEnv(), now: func() time.Time { return t0 }}
}

func get(s *quoteServer, query string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/quotes?"+query, nil)
	req = req.WithContext(auth.With(req.Context(), auth.Identity{Subject: "rider-1", Role: auth.Rider}))
	s.handleQuote(rec, req)
	return rec
}

const trip = "pickupLat=10.776&pickupLng=106.72&dropoffLat=10.82&dropoffLng=106.75"

func TestQuoteAppliesTheCellMultiplier(t *testing.T) {
	_, rdb := newRedis(t)
	cell := surge.CellID(deg, 10.776, 106.72)
	rdb.Set(context.Background(), surge.MultKey("CAR", cell), "1.4", time.Minute)
	s := quoteServerWith(t, rdb)

	rec := get(s, trip+"&vehicleType=CAR")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var q surge.Quote
	if err := json.Unmarshal(rec.Body.Bytes(), &q); err != nil {
		t.Fatal(err)
	}
	if q.Multiplier != 1.4 || q.Price != surge.Price(q.BaseFare, 1.4) || q.Currency != "VND" || q.QuoteID == "" {
		t.Errorf("quote = %+v", q)
	}
	// The quote can be verified by Order Service with the shared secret.
	signer, _ := surge.NewSigner("test-secret")
	_, err := signer.Verify(q.QuoteID, surge.QuoteRequest{RiderID: "rider-1", Vehicle: "CAR", PickupLat: 10.776, PickupLng: 106.72, DropoffLat: 10.82, DropoffLng: 106.75}, t0.Add(10*time.Second))
	if err != nil {
		t.Errorf("verify: %v", err)
	}

	// The other vehicle type is priced from its own key.
	rec = get(s, trip+"&vehicleType=BIKE")
	json.Unmarshal(rec.Body.Bytes(), &q)
	if q.Multiplier != 1.0 {
		t.Errorf("BIKE multiplier = %v, want 1.0", q.Multiplier)
	}
}

func TestQuoteFailsOpenWhenRedisIsDown(t *testing.T) {
	mr, rdb := newRedis(t)
	s := quoteServerWith(t, rdb)
	mr.Close() // Redis goes away

	rec := get(s, trip)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: a quote must still be issued: %s", rec.Code, rec.Body)
	}
	var q surge.Quote
	json.Unmarshal(rec.Body.Bytes(), &q)
	if q.Multiplier != 1.0 {
		t.Errorf("multiplier = %v, want 1.0", q.Multiplier)
	}
}

func TestQuoteIgnoresAGarbageStoredValue(t *testing.T) {
	_, rdb := newRedis(t)
	cell := surge.CellID(deg, 10.776, 106.72)
	for _, bad := range []string{"abc", "0.2", "-3", "NaN"} {
		rdb.Set(context.Background(), surge.MultKey("CAR", cell), bad, time.Minute)
		if m := currentMultiplier(context.Background(), rdb, "CAR", cell); m != 1.0 {
			t.Errorf("stored %q gave %v, want 1.0", bad, m)
		}
	}
}

func TestQuoteValidation(t *testing.T) {
	_, rdb := newRedis(t)
	s := quoteServerWith(t, rdb)
	cases := map[string]string{
		"no params":             "",
		"missing dropoff":       "pickupLat=10.7&pickupLng=106.7",
		"not a number":          "pickupLat=x&pickupLng=106.7&dropoffLat=10&dropoffLng=106",
		"latitude out of range": "pickupLat=91&pickupLng=106.7&dropoffLat=10&dropoffLng=106",
		"unknown vehicle":       trip + "&vehicleType=TRUCK",
		"lowercase vehicle":     trip + "&vehicleType=car",
	}
	for name, q := range cases {
		if rec := get(s, q); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, rec.Code)
		}
	}
}

func TestQuoteRequiresARider(t *testing.T) {
	_, rdb := newRedis(t)
	s := quoteServerWith(t, rdb)
	rec := httptest.NewRecorder()
	s.handleQuote(rec, httptest.NewRequest(http.MethodGet, "/quotes?"+trip, nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status %d, want 401", rec.Code)
	}
}
