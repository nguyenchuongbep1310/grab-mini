// Package promo signs and verifies promo tokens, the proof that a promotion
// code was checked for one rider, one price and one vehicle type, and what it
// takes off.
//
// Promotion Service issues a token from GET /promotions/validate; Order Service
// verifies it locally when the rider books, so booking never calls Promotion
// Service. The scheme is the one price quotes use (package surge): a token is
// base64url(JSON body) "." base64url(HMAC-SHA256), so nothing is stored and the
// token itself carries what it covers. The key is separate from the quote key
// (PROMO_SECRET), so whoever can sign quotes cannot sign discounts, and the
// MAC is domain-separated as well.
package promo

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"grab-mini/pkg/secret"
)

// TokenTTL is how long a token can be booked with. It is longer than a quote
// (60 seconds) because the rider types a code and then looks at the price.
const TokenTTL = 10 * time.Minute

// Token errors. A caller tells a stale token (validate again) from a forged one,
// and a token for someone else's trip from either.
var (
	ErrMalformed    = errors.New("promo token is malformed")
	ErrInvalid      = errors.New("promo token signature is invalid")
	ErrExpired      = errors.New("promo token has expired")
	ErrWrongRider   = errors.New("promo token was issued to another rider")
	ErrWrongPrice   = errors.New("promo token was issued for another price")
	ErrWrongVehicle = errors.New("promo token was issued for another vehicle type")
)

// Binding is what a token is bound to: the rider, the price (whole VND) the
// discount was worked out on, and the vehicle type of the trip.
type Binding struct {
	RiderID string
	Price   int64
	Vehicle string
}

// Claims is what a verified token says.
type Claims struct {
	Code      string
	Discount  int64 // whole VND, 0 or more, never above Price
	Price     int64
	Vehicle   string
	RiderID   string
	ExpiresAt time.Time
}

// body is what the token carries (and the signature covers).
type body struct {
	Code     string `json:"c"`
	Rider    string `json:"r"`
	Price    int64  `json:"p"`
	Vehicle  string `json:"v"`
	Discount int64  `json:"d"`
	Exp      int64  `json:"e"` // unix seconds
}

// Signer issues and verifies promo tokens.
type Signer struct{ key []byte }

// DevSecret is used when PROMO_SECRET isn't set and APP_ENV=development. It is
// public, so SignerFromEnv refuses it in every other environment, even when it
// is set explicitly: whoever holds the key can give any ride away.
const DevSecret = "dev-promo-secret-change-me"

// NewSigner returns a Signer over key.
func NewSigner(key string) (*Signer, error) {
	if key == "" {
		return nil, errors.New("promo: empty promo secret")
	}
	return &Signer{key: []byte(key)}, nil
}

// SignerFromEnv reads PROMO_SECRET (or PROMO_SECRET_FILE) via package secret.
// Promotion Service and Order Service must share it. Without it the
// development secret is used only when APP_ENV=development; otherwise it is an
// error.
func SignerFromEnv() (*Signer, error) {
	k, err := secret.GetOrDev("PROMO_SECRET", DevSecret)
	if err != nil {
		return nil, err
	}
	if k == DevSecret && !secret.Development() {
		return nil, errors.New("PROMO_SECRET is the public development secret; it is accepted only with APP_ENV=development")
	}
	return NewSigner(k)
}

func (s *Signer) mac(payload string) string {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte("promo-token|")) // never valid as a quote id, even under the same key
	m.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// Issue signs a discount of discount VND for code on a trip bound by b, valid
// for TokenTTL from now. It returns the token and its expiry.
func (s *Signer) Issue(code string, b Binding, discount int64, now time.Time) (string, time.Time) {
	exp := now.Add(TokenTTL).Truncate(time.Second)
	raw, _ := json.Marshal(body{Code: code, Rider: b.RiderID, Price: b.Price, Vehicle: b.Vehicle, Discount: discount, Exp: exp.Unix()})
	payload := base64.RawURLEncoding.EncodeToString(raw)
	return payload + "." + s.mac(payload), exp.UTC()
}

// Verify checks a token against what the booking is for. On a good signature it
// also returns the claims when it reports ErrExpired or a mismatch, so the
// caller can say what the token was for; a forged or malformed token returns
// zero Claims.
func (s *Signer) Verify(token string, want Binding, now time.Time) (Claims, error) {
	payload, sig, ok := strings.Cut(token, ".")
	if !ok || payload == "" || sig == "" {
		return Claims{}, ErrMalformed
	}
	if !hmac.Equal([]byte(sig), []byte(s.mac(payload))) {
		return Claims{}, ErrInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return Claims{}, ErrMalformed
	}
	var b body
	if err := json.Unmarshal(raw, &b); err != nil || b.Code == "" || b.Discount < 0 || b.Discount > b.Price {
		return Claims{}, ErrMalformed
	}
	c := Claims{Code: b.Code, Discount: b.Discount, Price: b.Price, Vehicle: b.Vehicle, RiderID: b.Rider, ExpiresAt: time.Unix(b.Exp, 0).UTC()}
	switch {
	case !now.Before(c.ExpiresAt):
		return c, ErrExpired
	case b.Rider != want.RiderID:
		return c, ErrWrongRider
	case b.Price != want.Price:
		return c, ErrWrongPrice
	case b.Vehicle != want.Vehicle:
		return c, ErrWrongVehicle
	}
	return c, nil
}
