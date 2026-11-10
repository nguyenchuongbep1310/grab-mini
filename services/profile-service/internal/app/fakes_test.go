package app

import (
	"bytes"
	"context"
	"errors"
	"log"
	"sync"
	"testing"
	"time"

	"grab-mini/profile-service/internal/domain"
)

// fakeStore implements RiderStore, DriverReader and ApplicationQueue in memory.
type fakeStore struct {
	riders  map[string]domain.Rider
	drivers map[string]domain.Driver

	loadErr  error // returned by every read
	saveErr  error // returned by SaveRider
	calls    []string
	saved    []domain.Rider
	queueArg queueCall
	queueOut []domain.Driver
	queueN   int
}

type queueCall struct {
	Status string
	Limit  int
	After  *domain.Cursor
}

func newFakeStore() *fakeStore {
	return &fakeStore{riders: map[string]domain.Rider{}, drivers: map[string]domain.Driver{}}
}

func (f *fakeStore) Rider(_ context.Context, id string) (domain.Rider, bool, error) {
	f.calls = append(f.calls, "Rider")
	if f.loadErr != nil {
		return domain.Rider{}, false, f.loadErr
	}
	r, ok := f.riders[id]
	return r, ok, nil
}

func (f *fakeStore) SaveRider(_ context.Context, r domain.Rider) error {
	f.calls = append(f.calls, "SaveRider")
	if f.saveErr != nil {
		return f.saveErr
	}
	f.riders[r.ID] = r
	f.saved = append(f.saved, r)
	return nil
}

func (f *fakeStore) Driver(_ context.Context, id string) (domain.Driver, error) {
	f.calls = append(f.calls, "Driver")
	if f.loadErr != nil {
		return domain.Driver{}, f.loadErr
	}
	d, ok := f.drivers[id]
	if !ok {
		return domain.Driver{}, domain.ErrNoDriver
	}
	return d, nil
}

func (f *fakeStore) ByStatus(_ context.Context, status string, limit int, after *domain.Cursor) ([]domain.Driver, error) {
	f.calls = append(f.calls, "ByStatus")
	f.queueArg = queueCall{status, limit, after}
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	f.queueN++
	return f.queueOut, nil
}

// fakeMetrics counts every Metrics call.
type fakeMetrics struct {
	updated, applications, reviews, public, runs []string
	resync                                       []string
}

func (m *fakeMetrics) Updated(k string)      { m.updated = append(m.updated, k) }
func (m *fakeMetrics) Application(r string)  { m.applications = append(m.applications, r) }
func (m *fakeMetrics) Review(d string)       { m.reviews = append(m.reviews, d) }
func (m *fakeMetrics) PublicLookup(r string) { m.public = append(m.public, r) }
func (m *fakeMetrics) ResyncEvents(s string, n int) {
	m.resync = append(m.resync, s)
}
func (m *fakeMetrics) ResyncRun(r string) { m.runs = append(m.runs, r) }

type fakeWaker struct{ n int }

func (w *fakeWaker) Wake() { w.n++ }

// fakeUoW, fakeEncoder, fakeSource and fakeLocker only satisfy Deps; Tasks 4 and 5 use them.
type fakeUoW struct{}

func (fakeUoW) Do(context.Context, func(Tx) error) error { return errors.New("not used") }

type fakeEncoder struct{}

func (fakeEncoder) Encode(context.Context, ReviewedEvent) ([]byte, error) { return nil, nil }

type fakeSource struct{}

func (fakeSource) ResyncableIDs(context.Context, string, int) ([]string, error) { return nil, nil }

type fakeLocker struct{}

func (fakeLocker) TryLock(context.Context) (func(), bool, error) { return func() {}, true, nil }

// clock is a recording clock: it logs the order of its reads against the store calls.
type clock struct {
	t     time.Time
	reads int
	store *fakeStore
	// atReads records len(store.calls) at each read.
	atReads []int
}

func (c *clock) Now() time.Time {
	c.reads++
	if c.store != nil {
		c.atReads = append(c.atReads, len(c.store.calls))
	}
	return c.t
}

type rig struct {
	svc   *Service
	store *fakeStore
	m     *fakeMetrics
	clk   *clock
	wake  *fakeWaker
}

func newRig(t *testing.T) *rig {
	t.Helper()
	st := newFakeStore()
	clk := &clock{t: time.Date(2026, 3, 1, 12, 0, 0, 123456789, time.FixedZone("x", 3600)), store: st}
	m := &fakeMetrics{}
	w := &fakeWaker{}
	svc := New(Deps{
		Riders: st, Drivers: st, Queue: st, Resync: fakeSource{}, Locker: fakeLocker{},
		UoW: fakeUoW{}, Encoder: fakeEncoder{}, Relay: w, Metrics: m, Now: clk.Now,
	})
	return &rig{svc, st, m, clk, w}
}

var logMu sync.Mutex

// captureLog redirects the standard logger for the test and returns what was written.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	logMu.Lock()
	var buf bytes.Buffer
	old, flags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(old)
		log.SetFlags(flags)
		logMu.Unlock()
	})
	return &buf
}
