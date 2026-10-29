package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// PaymentProvider is the seam to a card processor or wallet. The service never
// sees a card number, expiry or CVV: the rider's payment method lives with the
// provider (a tokenised method saved when the rider signed up there), and a
// charge names only the rider. A real implementation would map RiderID to that
// token, call the processor with IdempotencyKey, and map its answer to a
// ChargeResult.
//
// Contract:
//   - Charge is idempotent on IdempotencyKey: asking again with the same key
//     returns the original result and never charges twice.
//   - A decline (insufficient funds, a blocked method) is a normal answer, not
//     an error: return a ChargeResult with Status ChargeDeclined.
//   - An error means "unknown, try again" (timeout, 5xx): the event is retried
//     through the retry topics, and the same key makes the retry safe.
type PaymentProvider interface {
	Name() string
	Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error)
}

type ChargeRequest struct {
	// IdempotencyKey is stable for one logical charge ("fare:<tripId>").
	IdempotencyKey string
	RiderID        string
	// Amount is in minor units of Currency (VND: whole dong), always positive.
	Amount      int64
	Currency    string
	Description string
}

type ChargeStatus string

const (
	ChargeCaptured ChargeStatus = "CAPTURED"
	ChargeDeclined ChargeStatus = "DECLINED"
)

type ChargeResult struct {
	Status ChargeStatus
	// Ref is the provider's own id for the charge, kept for reconciliation.
	Ref           string
	DeclineReason string
}

// FakeProvider is the sandbox implementation: it captures every charge unless
// the rider's id starts with DeclinePrefix, and remembers results by
// idempotency key like a real processor does. It performs no I/O.
type FakeProvider struct {
	// DeclinePrefix makes charges to riders whose id starts with it decline;
	// empty means "decline-" (so tests and demos can create a failing rider).
	DeclinePrefix string

	mu      sync.Mutex
	results map[string]ChargeResult
	// Calls counts the charges that reached the provider (including repeats of
	// a key), for tests.
	Calls int
}

func NewFakeProvider() *FakeProvider {
	return &FakeProvider{results: map[string]ChargeResult{}}
}

func (f *FakeProvider) Name() string { return "fake" }

func (f *FakeProvider) Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error) {
	if err := ctx.Err(); err != nil {
		return ChargeResult{}, err
	}
	if req.IdempotencyKey == "" || req.Amount <= 0 || req.RiderID == "" {
		return ChargeResult{}, errors.New("fake provider: a charge needs a key, a rider and a positive amount")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls++
	if r, ok := f.results[req.IdempotencyKey]; ok {
		return r, nil
	}
	prefix := f.DeclinePrefix
	if prefix == "" {
		prefix = "decline-"
	}
	sum := sha256.Sum256([]byte(req.IdempotencyKey))
	r := ChargeResult{Ref: "fake_" + hex.EncodeToString(sum[:8])}
	if strings.HasPrefix(req.RiderID, prefix) {
		r.Status, r.DeclineReason = ChargeDeclined, "sandbox: rider payment method declined"
	} else {
		r.Status = ChargeCaptured
	}
	f.results[req.IdempotencyKey] = r
	return r, nil
}

// providerFromEnv picks the provider named by PAYMENT_PROVIDER. Only "fake"
// exists; a real processor is a new case here and a new type above.
func providerFromEnv(name string) (PaymentProvider, error) {
	switch name {
	case "", "fake":
		return NewFakeProvider(), nil
	}
	return nil, fmt.Errorf("unknown PAYMENT_PROVIDER %q (only \"fake\" is implemented)", name)
}
