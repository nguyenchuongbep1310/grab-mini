package surge

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestMultiplier(t *testing.T) {
	cases := []struct {
		name                 string
		demand, supply, k, m float64
		want                 float64
	}{
		{"balanced market is 1.0", 10, 10, 0.5, 3, 1.0},
		{"more drivers than orders never discounts", 2, 10, 0.5, 3, 1.0},
		{"double demand adds k", 20, 10, 0.5, 3, 1.5},
		{"rounds to 0.1", 13, 10, 0.5, 3, 1.2}, // 1 + .5*.3 = 1.15 -> 1.2
		{"clamped at max", 100, 1, 0.5, 3, 3.0},
		{"zero supply counts as one driver", 3, 0, 0.5, 3, 2.0},
		{"negative demand is zero", -5, 4, 0.5, 3, 1.0},
		{"no demand", 0, 0, 0.5, 3, 1.0},
	}
	for _, c := range cases {
		if got := Multiplier(c.demand, c.supply, c.k, c.m); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("%s: Multiplier(%v,%v,%v,%v) = %v, want %v", c.name, c.demand, c.supply, c.k, c.m, got, c.want)
		}
	}
}

func TestSmooth(t *testing.T) {
	cases := []struct {
		name                     string
		prev, target, alpha, cap float64
		want                     float64
	}{
		{"no change", 1.5, 1.5, 0.5, 0.3, 1.5},
		{"halfway with alpha .5", 1.0, 2.0, 0.5, 5, 1.5},
		{"step is capped going up", 1.0, 3.0, 1.0, 0.3, 1.3},
		{"step is capped going down", 2.0, 1.0, 1.0, 0.3, 1.7},
		{"never below 1", 1.1, 1.0, 1.0, 0.5, 1.0},
		{"prev below 1 is treated as 1", 0.2, 1.0, 1.0, 0.5, 1.0},
		{"no cap when maxStep is 0", 1.0, 2.5, 1.0, 0, 2.5},
	}
	for _, c := range cases {
		if got := Smooth(c.prev, c.target, c.alpha, c.cap, 3); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("%s: Smooth = %v, want %v", c.name, got, c.want)
		}
	}
	// Never above max, however large the target.
	if got := Smooth(2.9, 10, 1, 5, 3); got != 3.0 {
		t.Errorf("Smooth above max = %v, want 3", got)
	}
}

func TestCellID(t *testing.T) {
	cases := []struct {
		deg, lat, lng float64
		want          string
	}{
		{0.05, 10.776, 106.72, "215:2134"},
		{0.05, -0.01, -0.01, "-1:-1"},
		{0.1, 10.776, 106.72, "107:1067"},
		{0, 10.776, 106.72, "215:2134"}, // 0 means the default 0.05
	}
	for _, c := range cases {
		if got := CellID(c.deg, c.lat, c.lng); got != c.want {
			t.Errorf("CellID(%v,%v,%v) = %s, want %s", c.deg, c.lat, c.lng, got, c.want)
		}
	}
	// Nearby points share a cell; far ones don't.
	if CellID(0.05, 10.776, 106.701) != CellID(0.05, 10.777, 106.702) {
		t.Error("nearby points should share a cell")
	}
	if CellID(0.05, 10.776, 106.7) == CellID(0.05, 10.9, 106.7) {
		t.Error("points 15 km apart should not share a cell")
	}
}

func TestCellCenterRoundTrip(t *testing.T) {
	for _, p := range [][2]float64{{10.776, 106.7}, {-33.9, 151.2}, {0.001, -0.001}} {
		cell := CellID(0.05, p[0], p[1])
		lat, lng, err := CellCenter(0.05, cell)
		if err != nil {
			t.Fatal(err)
		}
		if CellID(0.05, lat, lng) != cell {
			t.Errorf("center of %s is in %s", cell, CellID(0.05, lat, lng))
		}
	}
	for _, bad := range []string{"", "1", "a:b", "1:"} {
		if _, _, err := CellCenter(0.05, bad); err == nil {
			t.Errorf("CellCenter(%q) should fail", bad)
		}
	}
}

func TestCellDegreesFollowsShardEnv(t *testing.T) {
	t.Setenv("SURGE_CELL_DEGREES", "")
	t.Setenv("GEO_SHARD_DEGREES", "")
	if CellDegrees() != DefaultCellDegrees {
		t.Error("default expected")
	}
	t.Setenv("GEO_SHARD_DEGREES", "0.1")
	if CellDegrees() != 0.1 {
		t.Error("should follow GEO_SHARD_DEGREES")
	}
	t.Setenv("SURGE_CELL_DEGREES", "0.02")
	if CellDegrees() != 0.02 {
		t.Error("SURGE_CELL_DEGREES should win")
	}
}

func TestBaseFare(t *testing.T) {
	cases := []struct {
		vehicle string
		km      float64
		want    float64
	}{
		{"CAR", 0, 20000},
		{"CAR", 5, 62000},    // 12000+50000
		{"CAR", 5.04, 62500}, // 62400 -> nearest 500
		{"BIKE", 0, 12000},
		{"BIKE", 5, 38000},
		{"BIKE", -3, 12000},
	}
	for _, c := range cases {
		if got := BaseFare(c.vehicle, c.km); got != c.want {
			t.Errorf("BaseFare(%s,%v) = %v, want %v", c.vehicle, c.km, got, c.want)
		}
	}
	if got := Price(62000, 1.4); got != 87000 { // 86800 -> nearest 1000
		t.Errorf("Price = %v, want 87000", got)
	}
}

func TestHaversine(t *testing.T) {
	if d := HaversineKm(10.776, 106.7, 10.776, 106.7); d != 0 {
		t.Errorf("same point = %v", d)
	}
	// One degree of latitude is about 111.2 km.
	if d := HaversineKm(0, 0, 1, 0); math.Abs(d-111.19) > 0.1 {
		t.Errorf("1 degree = %v km", d)
	}
}

var req = QuoteRequest{RiderID: "rider-1", Vehicle: "CAR", PickupLat: 10.776, PickupLng: 106.7, DropoffLat: 10.82, DropoffLng: 106.75}

func signer(t *testing.T, key string) *Signer {
	t.Helper()
	s, err := NewSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestQuoteRoundTrip(t *testing.T) {
	s := signer(t, "k1")
	now := time.Unix(1_800_000_000, 0)
	q := s.Issue(req, 1.4, now)
	if q.Multiplier != 1.4 || q.Currency != "VND" || q.Price != Price(q.BaseFare, 1.4) || q.BaseFare <= 0 {
		t.Fatalf("bad quote %+v", q)
	}
	if !q.ExpiresAt.Equal(now.Add(QuoteTTL)) {
		t.Errorf("expires %v", q.ExpiresAt)
	}
	got, err := s.Verify(q.QuoteID, req, now.Add(59*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got.Price != q.Price || got.Multiplier != 1.4 || got.BaseFare != q.BaseFare {
		t.Errorf("verified %+v, issued %+v", got, q)
	}
}

func TestQuoteVerifyFailures(t *testing.T) {
	s := signer(t, "k1")
	now := time.Unix(1_800_000_000, 0)
	q := s.Issue(req, 1.4, now)
	payload, sig, _ := strings.Cut(q.QuoteID, ".")

	other := req
	other.DropoffLat = 11
	otherRider := req
	otherRider.RiderID = "rider-2"
	otherVehicle := req
	otherVehicle.Vehicle = "BIKE"

	cases := []struct {
		name string
		id   string
		req  QuoteRequest
		at   time.Time
		want error
	}{
		{"expired", q.QuoteID, req, now.Add(QuoteTTL), ErrQuoteExpired},
		{"different dropoff", q.QuoteID, other, now, ErrQuoteMismatch},
		{"different rider", q.QuoteID, otherRider, now, ErrQuoteMismatch},
		{"different vehicle", q.QuoteID, otherVehicle, now, ErrQuoteMismatch},
		{"tampered signature", payload + "." + sig[:len(sig)-2] + "AA", req, now, ErrQuoteInvalid},
		{"tampered body", payload[:len(payload)-2] + "AA" + "." + sig, req, now, ErrQuoteInvalid},
		{"signed with another key", signer(t, "k2").Issue(req, 1.4, now).QuoteID, req, now, ErrQuoteInvalid},
		{"no dot", "abc", req, now, ErrQuoteMalformed},
		{"empty", "", req, now, ErrQuoteMalformed},
	}
	for _, c := range cases {
		_, err := s.Verify(c.id, c.req, c.at)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.want)
		}
	}

	// An expired quote still reports what it said, to compare with now.
	got, err := s.Verify(q.QuoteID, req, now.Add(time.Hour))
	if !errors.Is(err, ErrQuoteExpired) || got.Multiplier != 1.4 {
		t.Errorf("expired quote should carry its contents, got %+v, %v", got, err)
	}
	// A forged one does not.
	if got, _ := s.Verify(payload+".AAAA", req, now); got.Multiplier != 0 {
		t.Error("a forged quote must not reveal contents as trusted")
	}
}

func TestSignerNeedsKey(t *testing.T) {
	if _, err := NewSigner(""); err == nil {
		t.Error("empty key must be refused")
	}
}

func TestSignerFromEnvRefusesDevDefaultInProduction(t *testing.T) {
	t.Setenv("QUOTE_SECRET", "")
	t.Setenv("QUOTE_SECRET_FILE", "")
	t.Setenv("APP_ENV", "production")
	if _, err := SignerFromEnv(); err == nil {
		t.Error("production without QUOTE_SECRET must fail")
	}
	t.Setenv("APP_ENV", "")
	if _, err := SignerFromEnv(); err != nil {
		t.Errorf("development default expected: %v", err)
	}
	t.Setenv("QUOTE_SECRET", "abc")
	t.Setenv("APP_ENV", "production")
	if _, err := SignerFromEnv(); err != nil {
		t.Errorf("explicit secret expected to work: %v", err)
	}
}
