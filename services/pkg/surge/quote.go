package surge

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"grab-mini/pkg/secret"
)

// QuoteTTL is how long a quote can be booked with.
const QuoteTTL = 60 * time.Second

// Quote errors, so callers can tell a stale quote (re-quote) from a forged one.
var (
	ErrQuoteMalformed = errors.New("quote is malformed")
	ErrQuoteInvalid   = errors.New("quote signature is invalid")
	ErrQuoteExpired   = errors.New("quote has expired")
	ErrQuoteMismatch  = errors.New("quote is for a different rider or trip")
)

// QuoteRequest is what a quote is bound to: who asked, for which trip.
type QuoteRequest struct {
	RiderID                string
	Vehicle                string
	PickupLat, PickupLng   float64
	DropoffLat, DropoffLng float64
}

// route fingerprints the request so a quote can't be reused for another trip.
func (q QuoteRequest) route() string {
	return fmt.Sprintf("%s|%s|%.5f,%.5f|%.5f,%.5f", q.RiderID, q.Vehicle, q.PickupLat, q.PickupLng, q.DropoffLat, q.DropoffLng)
}

// Quote is the priced offer returned to a rider.
type Quote struct {
	QuoteID    string    `json:"quoteId"`
	BaseFare   float64   `json:"baseFare"`
	Multiplier float64   `json:"multiplier"`
	Price      float64   `json:"price"`
	Currency   string    `json:"currency"`
	ExpiresAt  time.Time `json:"expiresAt"`
}

// body is what the quote id carries (and the signature covers).
type body struct {
	Base  float64 `json:"b"`
	Mult  float64 `json:"m"`
	Price float64 `json:"p"`
	Exp   int64   `json:"e"` // unix seconds
	Route string  `json:"r"`
}

// Signer issues and verifies quote ids. A quote id is
// base64url(JSON body) "." base64url(HMAC-SHA256), so nothing is stored: the
// id itself holds the price, its expiry and the trip it is for.
type Signer struct{ key []byte }

// DevQuoteSecret is used only when APP_ENV is not "production".
const DevQuoteSecret = "dev-quote-secret-change-me"

// NewSigner returns a Signer over key.
func NewSigner(key string) (*Signer, error) {
	if key == "" {
		return nil, errors.New("surge: empty quote secret")
	}
	return &Signer{key: []byte(key)}, nil
}

// SignerFromEnv reads QUOTE_SECRET (or QUOTE_SECRET_FILE) via package secret.
// Pricing Service and Order Service must share it.
func SignerFromEnv() (*Signer, error) {
	k, err := secret.GetOr("QUOTE_SECRET", DevQuoteSecret)
	if err != nil {
		return nil, err
	}
	return NewSigner(k)
}

func (s *Signer) mac(payload string) string {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// Issue prices a trip at multiplier and signs the result, valid for QuoteTTL
// from now. The distance comes from the request's own pickup and dropoff.
func (s *Signer) Issue(req QuoteRequest, multiplier float64, now time.Time) Quote {
	base := BaseFare(req.Vehicle, HaversineKm(req.PickupLat, req.PickupLng, req.DropoffLat, req.DropoffLng))
	exp := now.Add(QuoteTTL).Truncate(time.Second)
	b := body{Base: base, Mult: multiplier, Price: Price(base, multiplier), Exp: exp.Unix(), Route: req.route()}
	raw, _ := json.Marshal(b)
	payload := base64.RawURLEncoding.EncodeToString(raw)
	return Quote{
		QuoteID:    payload + "." + s.mac(payload),
		BaseFare:   b.Base,
		Multiplier: b.Mult,
		Price:      b.Price,
		Currency:   Currency,
		ExpiresAt:  exp.UTC(),
	}
}

// Verify checks a quote id. On a good signature it returns the quote's
// contents even when it also reports ErrQuoteExpired or ErrQuoteMismatch, so
// the caller can compare the old multiplier with the current one. A forged or
// malformed id returns the zero Quote.
func (s *Signer) Verify(quoteID string, req QuoteRequest, now time.Time) (Quote, error) {
	payload, sig, ok := strings.Cut(quoteID, ".")
	if !ok || payload == "" || sig == "" {
		return Quote{}, ErrQuoteMalformed
	}
	if !hmac.Equal([]byte(sig), []byte(s.mac(payload))) {
		return Quote{}, ErrQuoteInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return Quote{}, ErrQuoteMalformed
	}
	var b body
	if err := json.Unmarshal(raw, &b); err != nil {
		return Quote{}, ErrQuoteMalformed
	}
	q := Quote{
		QuoteID: quoteID, BaseFare: b.Base, Multiplier: b.Mult, Price: b.Price,
		Currency: Currency, ExpiresAt: time.Unix(b.Exp, 0).UTC(),
	}
	if b.Route != req.route() {
		return q, ErrQuoteMismatch
	}
	if !now.Before(q.ExpiresAt) {
		return q, ErrQuoteExpired
	}
	return q, nil
}
