// Package provider holds implementations of app.Provider. Only the sandbox fake exists.
package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"grab-mini/payment-service/internal/app"
)

// Fake is the sandbox implementation: it captures every charge unless
// the rider's id starts with DeclinePrefix, and remembers results by
// idempotency key like a real processor does. It performs no I/O.
type Fake struct {
	// DeclinePrefix makes charges to riders whose id starts with it decline;
	// empty means "decline-" (so tests and demos can create a failing rider).
	DeclinePrefix string

	mu      sync.Mutex
	results map[string]app.ChargeResult
	// Calls counts the charges that reached the provider (including repeats of
	// a key), for tests.
	Calls int
}

func NewFake() *Fake {
	return &Fake{results: map[string]app.ChargeResult{}}
}

func (f *Fake) Name() string { return "fake" }

func (f *Fake) Charge(ctx context.Context, req app.ChargeRequest) (app.ChargeResult, error) {
	if err := ctx.Err(); err != nil {
		return app.ChargeResult{}, err
	}
	if req.IdempotencyKey == "" || req.Amount <= 0 || req.RiderID == "" {
		return app.ChargeResult{}, errors.New("fake provider: a charge needs a key, a rider and a positive amount")
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
	r := app.ChargeResult{Ref: "fake_" + hex.EncodeToString(sum[:8])}
	if strings.HasPrefix(req.RiderID, prefix) {
		r.Status, r.DeclineReason = app.ChargeDeclined, "sandbox: rider payment method declined"
	} else {
		r.Status = app.ChargeCaptured
	}
	f.results[req.IdempotencyKey] = r
	return r, nil
}

// FromEnv picks the provider named by PAYMENT_PROVIDER. Only "fake"
// exists; a real processor is a new case here and a new type above.
func FromEnv(name string) (app.Provider, error) {
	switch name {
	case "", "fake":
		return NewFake(), nil
	}
	return nil, fmt.Errorf("unknown PAYMENT_PROVIDER %q (only \"fake\" is implemented)", name)
}
