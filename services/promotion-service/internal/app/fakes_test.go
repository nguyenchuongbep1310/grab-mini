package app

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"sort"
	"testing"
	"time"

	"grab-mini/promotion-service/internal/domain"
)

var t0 = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

// fakeClock returns a fixed time and counts its reads.
type fakeClock struct {
	at    time.Time
	reads int
}

func (c *fakeClock) Now() time.Time { c.reads++; return c.at }

// store is an in-memory PromotionReader, PromotionWriter and PromotionViews.
type store struct {
	promos    map[string]domain.Promotion
	createdBy map[string]string
	createdAt map[string]time.Time
	usage     map[string]domain.Usage // key: code + "|" + rider
	calls     []string
	fail      map[string]error // by method name
	clock     *fakeClock
}

func newStore(clock *fakeClock) *store {
	return &store{
		promos: map[string]domain.Promotion{}, createdBy: map[string]string{}, createdAt: map[string]time.Time{},
		usage: map[string]domain.Usage{}, fail: map[string]error{}, clock: clock,
	}
}

func (s *store) put(p domain.Promotion) { s.promos[p.Code] = p }

func (s *store) Find(_ context.Context, code string) (domain.Promotion, error) {
	s.calls = append(s.calls, "Find")
	if err := s.fail["Find"]; err != nil {
		return domain.Promotion{}, err
	}
	p, ok := s.promos[code]
	if !ok {
		return domain.Promotion{}, domain.ErrNotFound
	}
	return p, nil
}

func (s *store) Usage(_ context.Context, code, rider string) (domain.Usage, error) {
	s.calls = append(s.calls, "Usage")
	if err := s.fail["Usage"]; err != nil {
		return domain.Usage{}, err
	}
	return s.usage[code+"|"+rider], nil
}

func (s *store) Create(_ context.Context, p domain.Promotion, by string) error {
	s.calls = append(s.calls, "Create")
	if err := s.fail["Create"]; err != nil {
		return err
	}
	if _, ok := s.promos[p.Code]; ok {
		return domain.ErrCodeExists
	}
	s.promos[p.Code] = p
	s.createdBy[p.Code] = by
	s.createdAt[p.Code] = s.clock.at
	return nil
}

// Disable keeps COALESCE(disabled_at, $at) semantics.
func (s *store) Disable(_ context.Context, code string, at time.Time) (bool, error) {
	s.calls = append(s.calls, "Disable")
	if err := s.fail["Disable"]; err != nil {
		return false, err
	}
	p, ok := s.promos[code]
	if !ok {
		return false, nil
	}
	if p.DisabledAt == nil {
		p.DisabledAt = &at
		s.promos[code] = p
	}
	return true, nil
}

func (s *store) summary(code string) domain.Summary {
	return domain.Summary{Promotion: s.promos[code], CreatedBy: s.createdBy[code], CreatedAt: s.createdAt[code]}
}

func (s *store) View(_ context.Context, code string) (domain.Summary, error) {
	s.calls = append(s.calls, "View")
	if err := s.fail["View"]; err != nil {
		return domain.Summary{}, err
	}
	if _, ok := s.promos[code]; !ok {
		return domain.Summary{}, domain.ErrNotFound
	}
	return s.summary(code), nil
}

func (s *store) List(_ context.Context) ([]domain.Summary, error) {
	s.calls = append(s.calls, "List")
	if err := s.fail["List"]; err != nil {
		return nil, err
	}
	var out []domain.Summary
	for c := range s.promos {
		out = append(out, s.summary(c))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out, nil
}

// issued is one call to the fake TokenIssuer.
type issued struct {
	Code, Rider, Vehicle string
	Price, Discount      int64
	Now                  time.Time
}

type fakeTokens struct{ calls []issued }

func (f *fakeTokens) Issue(code, riderID string, price int64, vehicle string, discount int64, now time.Time) (string, time.Time) {
	f.calls = append(f.calls, issued{code, riderID, vehicle, price, discount, now})
	return "token-1", now.Add(15 * time.Minute)
}

// fakeMetrics records every counter call, in order.
type fakeMetrics struct{ events []string }

func (m *fakeMetrics) Validation(result string) { m.events = append(m.events, "validation:"+result) }
func (m *fakeMetrics) Redemption(status, reason string) {
	m.events = append(m.events, "redemption:"+status+":"+reason)
}
func (m *fakeMetrics) Released(n int64) { m.events = append(m.events, "released") }
func (m *fakeMetrics) Duplicate()       { m.events = append(m.events, "duplicate") }

type rig struct {
	svc    *Service
	store  *store
	tokens *fakeTokens
	m      *fakeMetrics
	clock  *fakeClock
}

func newRig(t *testing.T) *rig {
	t.Helper()
	clock := &fakeClock{at: t0}
	r := &rig{store: newStore(clock), tokens: &fakeTokens{}, m: &fakeMetrics{}, clock: clock}
	r.svc = New(Deps{Reader: r.store, Writer: r.store, Views: r.store, Tokens: r.tokens, Metrics: r.m, Now: clock.Now})
	return r
}

// capture redirects the standard logger for the test and returns what it wrote.
func capture(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(os.Stderr); log.SetFlags(log.LstdFlags) })
	return &buf
}

func i64(v int64) *int64   { return &v }
func str(v string) *string { return &v }

// percent10 is an active 10% code, usable by anyone, any vehicle.
func percent10(code string) domain.Promotion {
	return domain.Promotion{Code: code, Kind: domain.KindPercent, Value: 10, PerRider: 1,
		ValidFrom: t0.Add(-time.Hour), ValidTo: t0.Add(time.Hour)}
}

func isStep(err error, step string) bool {
	var se *StepError
	return errors.As(err, &se) && se.Step == step
}
