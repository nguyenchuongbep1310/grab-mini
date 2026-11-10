package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"grab-mini/profile-service/internal/domain"
)

// recorder is one ordered log shared by the unit of work, the encoder, the clock, the
// metrics, the waker and the standard logger, so a test can assert the order of everything a
// use case does: inside the transaction and after the commit.
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(e string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

func (r *recorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

// Write records each log line as "log: <line>".
func (r *recorder) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		r.add("log: " + line)
	}
	return len(p), nil
}

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

// resyncCount is one ResyncEvents call.
type resyncCount struct {
	Status string
	N      int
}

// fakeMetrics counts every Metrics call (and logs it to rec when set).
type fakeMetrics struct {
	rec                                          *recorder
	updated, applications, reviews, public, runs []string
	resync                                       []resyncCount
}

func (m *fakeMetrics) Updated(k string) {
	m.rec.add("Updated(" + k + ")")
	m.updated = append(m.updated, k)
}
func (m *fakeMetrics) Application(r string) {
	m.rec.add("Application(" + r + ")")
	m.applications = append(m.applications, r)
}
func (m *fakeMetrics) Review(d string) {
	m.rec.add("Review(" + d + ")")
	m.reviews = append(m.reviews, d)
}
func (m *fakeMetrics) PublicLookup(r string) {
	m.rec.add("PublicLookup(" + r + ")")
	m.public = append(m.public, r)
}
func (m *fakeMetrics) ResyncEvents(s string, n int) {
	m.rec.add(fmt.Sprintf("ResyncEvents(%s,%d)", s, n))
	m.resync = append(m.resync, resyncCount{s, n})
}
func (m *fakeMetrics) ResyncRun(r string) {
	m.rec.add("ResyncRun(" + r + ")")
	m.runs = append(m.runs, r)
}

// counted reports whether any counter moved.
func (m *fakeMetrics) counted() bool {
	return len(m.updated)+len(m.applications)+len(m.reviews)+len(m.public)+len(m.runs)+len(m.resync) > 0
}

type fakeWaker struct {
	rec *recorder
	n   int
}

func (w *fakeWaker) Wake() {
	w.rec.add("Wake")
	w.n++
}

// queued is one outbox row the fake transaction kept.
type queued struct {
	DriverID string
	Payload  []byte
}

// fakeUoW is a recording unit of work over an in-memory driver table and outbox. It logs
// Begin, every Tx call, then Commit or Rollback to rec; it can fail one chosen Tx call
// (failOn, failErr) or the commit (commitErr). When fn or the commit fails, the drivers and
// the outbox go back to what they were before Do, as a rolled-back transaction would.
type fakeUoW struct {
	rec     *recorder
	drivers map[string]domain.Driver
	outbox  []queued

	failOn    string
	failErr   error
	commitErr error
	dos       int
}

func newFakeUoW(rec *recorder) *fakeUoW {
	return &fakeUoW{rec: rec, drivers: map[string]domain.Driver{}}
}

func (u *fakeUoW) Do(ctx context.Context, fn func(Tx) error) error {
	u.dos++
	u.rec.add("Begin")
	drivers, outbox := maps.Clone(u.drivers), slices.Clone(u.outbox)
	err := fn(fakeTx{u})
	if err == nil && u.commitErr != nil {
		err = u.commitErr
	}
	if err != nil {
		u.drivers, u.outbox = drivers, outbox
		u.rec.add("Rollback")
		return err
	}
	u.rec.add("Commit")
	return nil
}

type fakeTx struct{ u *fakeUoW }

func (t fakeTx) call(name string) error {
	t.u.rec.add(name)
	if t.u.failOn == name {
		return t.u.failErr
	}
	return nil
}

func (t fakeTx) LockOrCreateDriver(_ context.Context, id string, now time.Time) (domain.Driver, error) {
	if err := t.call("LockOrCreateDriver"); err != nil {
		return domain.Driver{}, err
	}
	d, ok := t.u.drivers[id]
	if !ok { // the column defaults of driver_profiles
		d = domain.Driver{ID: id, Status: domain.StatusNone, StatusAt: now, UpdatedAt: now}
		t.u.drivers[id] = d
	}
	return d, nil
}

func (t fakeTx) LockDriver(_ context.Context, id string) (domain.Driver, error) {
	if err := t.call("LockDriver"); err != nil {
		return domain.Driver{}, err
	}
	d, ok := t.u.drivers[id]
	if !ok {
		return domain.Driver{}, domain.ErrNoDriver
	}
	return d, nil
}

func (t fakeTx) UpdateProfile(_ context.Context, d domain.Driver) error {
	if err := t.call("UpdateProfile"); err != nil {
		return err
	}
	t.u.drivers[d.ID] = d
	return nil
}

func (t fakeTx) MarkPending(_ context.Context, id string, now time.Time) error {
	if err := t.call("MarkPending"); err != nil {
		return err
	}
	d := t.u.drivers[id]
	d.Status, d.Reason, d.StatusAt, d.UpdatedAt = domain.StatusPending, "", now, now
	t.u.drivers[id] = d
	return nil
}

func (t fakeTx) RecordReview(_ context.Context, d domain.Driver, at time.Time) error {
	if err := t.call("RecordReview"); err != nil {
		return err
	}
	row := t.u.drivers[d.ID]
	row.Status, row.Reason, row.ReviewedBy, row.ReviewedAt, row.StatusAt, row.UpdatedAt = d.Status, d.Reason, d.ReviewedBy, &at, d.StatusAt, d.StatusAt
	t.u.drivers[d.ID] = row
	return nil
}

func (t fakeTx) QueueReview(_ context.Context, driverID string, payload []byte) error {
	if err := t.call("QueueReview"); err != nil {
		return err
	}
	t.u.outbox = append(t.u.outbox, queued{driverID, slices.Clone(payload)})
	return nil
}

// fakeEncoder records each event it is given (and "Encode" to rec); the payload is
// "event:<EventID>", so a queued row can be traced back to its event.
type fakeEncoder struct {
	rec    *recorder
	err    error
	events []ReviewedEvent
}

func (e *fakeEncoder) Reviewed(_ context.Context, ev ReviewedEvent) ([]byte, error) {
	e.rec.add("Encode")
	if e.err != nil {
		return nil, e.err
	}
	e.events = append(e.events, ev)
	return []byte("event:" + ev.EventID), nil
}

// fakeSource and fakeLocker only satisfy Deps; Task 5 uses them.
type fakeSource struct{}

func (fakeSource) ResyncableIDs(context.Context, string, int) ([]string, error) { return nil, nil }

type fakeLocker struct{}

func (fakeLocker) TryLock(context.Context) (func(), bool, error) { return func() {}, true, nil }

// clock is a recording clock: it logs the order of its reads against the store calls and,
// when rec is set, as "now" in the shared log.
type clock struct {
	t     time.Time
	reads int
	store *fakeStore
	rec   *recorder
	// atReads records len(store.calls) at each read.
	atReads []int
}

func (c *clock) Now() time.Time {
	c.reads++
	if c.store != nil {
		c.atReads = append(c.atReads, len(c.store.calls))
	}
	c.rec.add("now")
	return c.t
}

type rig struct {
	svc   *Service
	store *fakeStore
	m     *fakeMetrics
	clk   *clock
	wake  *fakeWaker
	rec   *recorder
	uow   *fakeUoW
	enc   *fakeEncoder
}

func newRig(t *testing.T) *rig {
	t.Helper()
	rec := &recorder{}
	st := newFakeStore()
	clk := &clock{t: time.Date(2026, 3, 1, 12, 0, 0, 123456789, time.FixedZone("x", 3600)), store: st, rec: rec}
	m := &fakeMetrics{rec: rec}
	w := &fakeWaker{rec: rec}
	uow := newFakeUoW(rec)
	enc := &fakeEncoder{rec: rec}
	svc := New(Deps{
		Riders: st, Drivers: st, Queue: st, Resync: fakeSource{}, Locker: fakeLocker{},
		UoW: uow, Encoder: enc, Relay: w, Metrics: m, Now: clk.Now,
	})
	return &rig{svc, st, m, clk, w, rec, uow, enc}
}

var logMu sync.Mutex

// captureLog redirects the standard logger for the test and returns what was written.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	return captureLogInto(t, nil)
}

// captureLogInto is captureLog that also records every line in rec (nil: buffer only).
func captureLogInto(t *testing.T, rec *recorder) *bytes.Buffer {
	t.Helper()
	logMu.Lock()
	var buf bytes.Buffer
	var w io.Writer = &buf
	if rec != nil {
		w = io.MultiWriter(&buf, rec)
	}
	old, flags := log.Writer(), log.Flags()
	log.SetOutput(w)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(old)
		log.SetFlags(flags)
		logMu.Unlock()
	})
	return &buf
}

var errBoom = errors.New("boom")
