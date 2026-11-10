package app

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"sort"
	"strconv"
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
func (m *fakeMetrics) Released(n int64) {
	m.events = append(m.events, "released:"+strconv.FormatInt(n, 10))
}
func (m *fakeMetrics) Duplicate() { m.events = append(m.events, "duplicate") }

type rig struct {
	svc    *Service
	store  *store
	tokens *fakeTokens
	m      *fakeMetrics
	clock  *fakeClock
	db     *fakeDB
	enc    *fakeEncoder
	relay  *fakeWaker
}

func newRig(t *testing.T) *rig {
	t.Helper()
	clock := &fakeClock{at: t0}
	r := &rig{store: newStore(clock), tokens: &fakeTokens{}, m: &fakeMetrics{}, clock: clock, db: newFakeDB(clock)}
	r.enc = &fakeEncoder{db: r.db}
	r.relay = &fakeWaker{db: r.db}
	r.svc = New(Deps{Reader: r.store, Writer: r.store, Views: r.store, Tokens: r.tokens, Metrics: r.m, Now: clock.Now,
		UoW: r.db, Encoder: r.enc, Relay: r.relay})
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

// ------------------------------------------------------- unit of work fakes --

// redemptionRow is a redemptions row in the fake database.
type redemptionRow struct {
	NewRedemption
	DBReleasedAt *time.Time // set by ReleaseRedemptions, with the database clock
}

// queued is an outbox row in the fake database.
type queued struct {
	OrderID string
	Payload []byte
}

// dbState is everything a transaction can change; it is copied on Do and put
// back when fn (or the commit) fails.
type dbState struct {
	markers     map[string]bool
	redemptions []redemptionRow // key: (code, order_id)
	outbox      []queued
	cancelled   map[string]bool
}

func (s dbState) clone() dbState {
	c := dbState{markers: map[string]bool{}, cancelled: map[string]bool{}}
	for k, v := range s.markers {
		c.markers[k] = v
	}
	for k, v := range s.cancelled {
		c.cancelled[k] = v
	}
	c.redemptions = append([]redemptionRow(nil), s.redemptions...)
	c.outbox = append([]queued(nil), s.outbox...)
	return c
}

// fakeDB is an in-memory UnitOfWork and Tx. Every call is recorded in calls,
// in order, with "begin", "commit" and "rollback" around each Do; fail makes a
// chosen call (by method name, or "commit") return an error.
type fakeDB struct {
	promos  map[string]domain.Promotion
	usage   map[string]domain.Usage // key: code + "|" + rider
	state   dbState
	calls   []string
	fail    map[string]error
	inTx    bool
	dbClock time.Time // what now() is inside the database
	clock   *fakeClock
}

var errBoom = errors.New("boom")

func newFakeDB(clock *fakeClock) *fakeDB {
	return &fakeDB{
		promos: map[string]domain.Promotion{}, usage: map[string]domain.Usage{}, fail: map[string]error{},
		state:   dbState{markers: map[string]bool{}, cancelled: map[string]bool{}},
		dbClock: t0.Add(7 * time.Minute), clock: clock,
	}
}

func (f *fakeDB) Do(ctx context.Context, fn func(Tx) error) error {
	f.calls = append(f.calls, "begin")
	saved := f.state.clone()
	f.inTx = true
	err := fn(f)
	f.inTx = false
	if err == nil {
		if err = f.fail["commit"]; err == nil {
			f.calls = append(f.calls, "commit")
			return nil
		}
	}
	f.calls = append(f.calls, "rollback")
	f.state = saved
	return err
}

func (f *fakeDB) call(name string) error {
	if !f.inTx {
		panic(name + " called outside a transaction")
	}
	f.calls = append(f.calls, name)
	return f.fail[name]
}

func (f *fakeDB) MarkEventProcessed(_ context.Context, eventID string) (bool, error) {
	if err := f.call("MarkEventProcessed"); err != nil {
		return false, err
	}
	if f.state.markers[eventID] {
		return false, nil
	}
	f.state.markers[eventID] = true
	return true, nil
}

func (f *fakeDB) LockOrder(_ context.Context, orderID string) error { return f.call("LockOrder") }

func (f *fakeDB) FindPromotionForUpdate(_ context.Context, code string) (domain.Promotion, error) {
	if err := f.call("FindPromotionForUpdate"); err != nil {
		return domain.Promotion{}, err
	}
	p, ok := f.promos[code]
	if !ok {
		return domain.Promotion{}, domain.ErrNotFound
	}
	return p, nil
}

func (f *fakeDB) OrderWasCancelled(_ context.Context, orderID string) (bool, error) {
	if err := f.call("OrderWasCancelled"); err != nil {
		return false, err
	}
	return f.state.cancelled[orderID], nil
}

func (f *fakeDB) Usage(_ context.Context, code, riderID string) (domain.Usage, error) {
	if err := f.call("Usage"); err != nil {
		return domain.Usage{}, err
	}
	return f.usage[code+"|"+riderID], nil
}

func (f *fakeDB) InsertRedemption(_ context.Context, r NewRedemption) (bool, error) {
	if err := f.call("InsertRedemption"); err != nil {
		return false, err
	}
	for _, row := range f.state.redemptions {
		if row.Code == r.Code && row.OrderID == r.OrderID {
			return false, nil
		}
	}
	f.state.redemptions = append(f.state.redemptions, redemptionRow{NewRedemption: r})
	return true, nil
}

func (f *fakeDB) QueueRedeemed(_ context.Context, orderID string, payload []byte) error {
	if err := f.call("QueueRedeemed"); err != nil {
		return err
	}
	f.state.outbox = append(f.state.outbox, queued{orderID, payload})
	return nil
}

func (f *fakeDB) ReleaseRedemptions(_ context.Context, orderID string) (int64, error) {
	if err := f.call("ReleaseRedemptions"); err != nil {
		return 0, err
	}
	var n int64
	for i, row := range f.state.redemptions {
		if row.OrderID == orderID && (row.StoredStatus == domain.StatusRedeemed || row.StoredStatus == domain.StatusOverLimit) {
			at := f.dbClock
			f.state.redemptions[i].StoredStatus = domain.StatusReleased
			f.state.redemptions[i].DBReleasedAt = &at
			n++
		}
	}
	return n, nil
}

func (f *fakeDB) RememberCancelled(_ context.Context, orderID string) error {
	if err := f.call("RememberCancelled"); err != nil {
		return err
	}
	f.state.cancelled[orderID] = true
	return nil
}

// row returns the redemption of order under code, or nil.
func (f *fakeDB) row(code, order string) *redemptionRow {
	for i := range f.state.redemptions {
		if r := &f.state.redemptions[i]; r.Code == code && r.OrderID == order {
			return r
		}
	}
	return nil
}

// fakeEncoder records each event, and its call in the database's call list
// along with whether a transaction was open.
type fakeEncoder struct {
	db     *fakeDB
	events []RedeemedEvent
	inTx   []bool
	fail   error
}

func (e *fakeEncoder) Redeemed(_ context.Context, evt RedeemedEvent) ([]byte, error) {
	e.db.calls = append(e.db.calls, "Encode")
	e.inTx = append(e.inTx, e.db.inTx)
	if e.fail != nil {
		return nil, e.fail
	}
	e.events = append(e.events, evt)
	return []byte("avro:" + evt.OrderID + ":" + evt.Status), nil
}

// fakeWaker counts wakes and records them in the database's call list.
type fakeWaker struct {
	db    *fakeDB
	wakes int
}

func (w *fakeWaker) Wake() { w.wakes++; w.db.calls = append(w.db.calls, "Wake") }
