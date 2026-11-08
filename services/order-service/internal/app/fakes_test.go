package app

import (
	"context"
	"errors"
	"sync"
	"time"
)

type fixedMultiplier float64

func (f fixedMultiplier) Multiplier(context.Context, string, string) float64 { return float64(f) }

// fakeClaims is an in-memory ClaimStore that records what it was asked.
type fakeClaims struct {
	mu       sync.Mutex
	used     map[string]bool
	ttls     map[string]time.Duration
	released []string
	down     bool
}

func newFakeClaims() *fakeClaims {
	return &fakeClaims{used: map[string]bool{}, ttls: map[string]time.Duration{}}
}

func (f *fakeClaims) Claim(_ context.Context, id string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return false, errors.New("redis down")
	}
	if f.used[id] {
		return false, nil
	}
	f.used[id], f.ttls[id] = true, ttl
	return true, nil
}

func (f *fakeClaims) Release(_ context.Context, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, id)
	delete(f.used, id)
}

// fakeMetrics counts the labelled results.
type fakeMetrics struct {
	mu      sync.Mutex
	pricing map[string]int
	promos  map[string]int
	failOpn int
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{pricing: map[string]int{}, promos: map[string]int{}}
}

func (m *fakeMetrics) OrderCreated(string) {}
func (m *fakeMetrics) OrderCancelled()     {}
func (m *fakeMetrics) Pricing(r string)    { m.mu.Lock(); m.pricing[r]++; m.mu.Unlock() }
func (m *fakeMetrics) SurgeFailOpen()      { m.mu.Lock(); m.failOpn++; m.mu.Unlock() }
func (m *fakeMetrics) Promo(r string)      { m.mu.Lock(); m.promos[r]++; m.mu.Unlock() }
func (m *fakeMetrics) Idempotent(string)   {}
