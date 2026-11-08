package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

const (
	idempotencyKeyMaxLen = 128
	// IdempotencyTTL is how long a key is remembered. After this the same key
	// books a new order.
	IdempotencyTTL = 24 * time.Hour
)

// ErrBadIdempotencyKey is returned for a key that is not usable.
var ErrBadIdempotencyKey = errors.New("Idempotency-Key must be 1 to 128 printable ASCII characters")

// ParseIdempotencyKey reads the values of the Idempotency-Key header: "" when
// absent, an error when present but not 1 to 128 printable ASCII characters
// (space to ~) or given twice.
func ParseIdempotencyKey(headerValues []string) (string, error) {
	switch len(headerValues) {
	case 0:
		return "", nil
	case 1:
	default:
		return "", ErrBadIdempotencyKey
	}
	k := headerValues[0]
	if len(k) < 1 || len(k) > idempotencyKeyMaxLen {
		return "", ErrBadIdempotencyKey
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x20 || k[i] > 0x7e {
			return "", ErrBadIdempotencyKey
		}
	}
	return k, nil
}

// Fingerprint identifies a booking request: a hash of the normalized body, so
// two bodies that mean the same (field order, whitespace, an omitted
// vehicleType versus "CAR") get the same value. The rider is not part of it:
// keys are scoped to the rider already, and the body can only name the caller.
// Call it after Normalize.
func (r BookingRequest) Fingerprint() string {
	type latLng struct{ Lat, Lng float64 }
	norm := struct {
		Pickup, Dropoff latLng
		VehicleType     string
		QuoteID         string
		// Left out when empty, so the fingerprint of a booking without a promo
		// code is what it was before promotions existed (keys live for 24 hours).
		PromoToken string `json:",omitempty"`
	}{
		Pickup:      latLng{r.Pickup.Lat, r.Pickup.Lng},
		Dropoff:     latLng{r.Dropoff.Lat, r.Dropoff.Lng},
		VehicleType: string(r.VehicleType),
		QuoteID:     r.QuoteID,
		PromoToken:  r.PromoToken,
	}
	b, _ := json.Marshal(norm) // a fixed struct: the field order is the declaration order
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// KeyRecord is what is stored under an idempotency key: the first attempt's
// fingerprint, order and response.
type KeyRecord struct {
	Key         string
	Fingerprint string
	OrderID     string
	Status      int
	Body        []byte
}
