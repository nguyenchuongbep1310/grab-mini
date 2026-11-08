package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestIdempotencyKeyValidation(t *testing.T) {
	ok := []string{"a", "550e8400-e29b-41d4-a716-446655440000", strings.Repeat("k", 128), "with space and ~!@#"}
	for _, k := range ok {
		if got, err := ParseIdempotencyKey([]string{k}); err != nil || got != k {
			t.Errorf("%q: got %q, %v; want accepted", k, got, err)
		}
	}
	if got, err := ParseIdempotencyKey(nil); got != "" || err != nil {
		t.Errorf("absent header = %q, %v; want empty and no error", got, err)
	}
	bad := map[string][]string{
		"empty":         {""},
		"too long":      {strings.Repeat("k", 129)},
		"newline":       {"a\nb"},
		"tab":           {"a\tb"},
		"DEL":           {"a\x7fb"},
		"non-ASCII":     {"clé"},
		"given twice":   {"a", "b"},
		"only too long": {strings.Repeat("é", 70)},
	}
	for name, v := range bad {
		_, err := ParseIdempotencyKey(v)
		if err != ErrBadIdempotencyKey {
			t.Errorf("%s: err = %v, want ErrBadIdempotencyKey", name, err)
		}
	}
	if ErrBadIdempotencyKey.Error() != "Idempotency-Key must be 1 to 128 printable ASCII characters" {
		t.Errorf("error text changed: %q", ErrBadIdempotencyKey)
	}
}

func tripRequest() BookingRequest {
	return BookingRequest{
		Pickup: &LatLng{Lat: 10.776, Lng: 106.72}, Dropoff: &LatLng{Lat: 10.82, Lng: 106.75},
		VehicleType: VehicleCar,
	}
}

func TestFingerprintIgnoresFormattingButNotMeaning(t *testing.T) {
	base := tripRequest()
	a := `{"pickup":{"lat":10.776,"lng":106.72},"dropoff":{"lat":10.82,"lng":106.75}}`
	b := `{ "dropoff": {"lng":106.75, "lat":10.82}, "vehicleType":"CAR", "pickup":{"lat":1.0776e1,"lng":106.72} }`
	fa, fb := fingerprintOfJSON(t, a), fingerprintOfJSON(t, b)
	if fa != fb {
		t.Errorf("equivalent bodies fingerprint differently:\n%s\n%s", fa, fb)
	}
	if fa != base.Fingerprint() {
		t.Error("the default vehicle changed the fingerprint")
	}

	for name, mutate := range map[string]func(*BookingRequest){
		"pickup":   func(r *BookingRequest) { r.Pickup.Lat += 0.0001 },
		"dropoff":  func(r *BookingRequest) { r.Dropoff.Lng += 0.0001 },
		"vehicle":  func(r *BookingRequest) { r.VehicleType = VehicleBike },
		"quote id": func(r *BookingRequest) { r.QuoteID = "q" },
		"promo":    func(r *BookingRequest) { r.PromoToken = "p" },
	} {
		other := tripRequest()
		mutate(&other)
		if other.Fingerprint() == base.Fingerprint() {
			t.Errorf("a different %s kept the fingerprint", name)
		}
	}
}

// The hash input is the JSON of a fixed struct; these values were recorded
// from the flat code, so a reordered field or renamed key (which would orphan
// keys stored for 24 hours) fails here.
func TestTheFingerprintHashInputIsUnchanged(t *testing.T) {
	r := tripRequest()
	if got, want := r.Fingerprint(), "7a6f387bc606b3ab14136a6e9a157e034a308958a3d15346a47ec2c28276fd3b"; got != want {
		t.Errorf("plain booking fingerprint = %s, want %s", got, want)
	}
	r.QuoteID, r.PromoToken = "q", "p"
	if got, want := r.Fingerprint(), "4b9528de7da7451c925d11c22cb7f0fdcb06cdb578e440bc6317ea1fe98115cf"; got != want {
		t.Errorf("quote and promo fingerprint = %s, want %s", got, want)
	}
}

func fingerprintOfJSON(t *testing.T, body string) string {
	t.Helper()
	var req BookingRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	if req.VehicleType == "" {
		req.VehicleType = VehicleCar
	}
	return req.Fingerprint()
}
