package app

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/promo"
	"grab-mini/pkg/surge"
)

type fixedMultiplier float64

func (f fixedMultiplier) Multiplier(context.Context, string, string) float64 { return float64(f) }

// fakeClaims is an in-memory ClaimStore that records what it was asked.
type fakeClaims struct {
	mu       sync.Mutex
	used     map[string]bool
	ttls     map[string]time.Duration
	released []string
	rec      *recorder // when set, Release is logged as "release"
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
	if f.rec != nil {
		f.rec.add("release")
	}
	delete(f.used, id)
}

// fakeMetrics counts the labelled results.
type fakeMetrics struct {
	mu        sync.Mutex
	pricing   map[string]int
	promos    map[string]int
	created   map[string]int
	idem      map[string]int
	cancelled int
	failOpn   int
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{pricing: map[string]int{}, promos: map[string]int{}, created: map[string]int{}, idem: map[string]int{}}
}

func (m *fakeMetrics) OrderCreated(v string) { m.mu.Lock(); m.created[v]++; m.mu.Unlock() }
func (m *fakeMetrics) OrderCancelled()       { m.mu.Lock(); m.cancelled++; m.mu.Unlock() }
func (m *fakeMetrics) Pricing(r string)      { m.mu.Lock(); m.pricing[r]++; m.mu.Unlock() }
func (m *fakeMetrics) SurgeFailOpen()        { m.mu.Lock(); m.failOpn++; m.mu.Unlock() }
func (m *fakeMetrics) Promo(r string)        { m.mu.Lock(); m.promos[r]++; m.mu.Unlock() }
func (m *fakeMetrics) Idempotent(r string)   { m.mu.Lock(); m.idem[r]++; m.mu.Unlock() }

// recorder keeps the sequence of port calls, for the flow-order tests.
type recorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder) add(call string) { r.mu.Lock(); r.calls = append(r.calls, call); r.mu.Unlock() }

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *recorder) reset() { r.mu.Lock(); r.calls = nil; r.mu.Unlock() }

// recordingMultiplier answers a fixed multiplier and records that pricing read it.
type recordingMultiplier struct {
	rec  *recorder
	mult float64
}

func (m recordingMultiplier) Multiplier(context.Context, string, string) float64 {
	m.rec.add("multiplier")
	return m.mult
}

// queued is an outbox row.
type queued struct {
	topic, orderID string
	payload        []byte
}

// fakeDB is an in-memory database behind OrderReader, KeyStore, Outcomes and
// UnitOfWork. A transaction's writes are kept aside and applied on commit, so
// a failed one leaves nothing behind.
type fakeDB struct {
	mu     sync.Mutex
	rec    *recorder
	orders map[string]domain.Order
	seq    []string // order ids, oldest first
	keys   map[string]domain.KeyRecord
	outbox []queued

	failBegin, failInsert, failQueue, failSaveKey, failCommit, failRead error
	// onSaveKey, when set, answers SaveKey instead (it may commit a winner first).
	onSaveKey func() error
	// beforeCancel runs as CancelPending starts, so a match can land first.
	beforeCancel func()
}

func newFakeDB(rec *recorder) *fakeDB {
	return &fakeDB{rec: rec, orders: map[string]domain.Order{}, keys: map[string]domain.KeyRecord{}}
}

func keyID(riderID, key string) string { return riderID + "\x00" + key }

func (d *fakeDB) seed(o domain.Order) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.orders[o.ID] = o
	d.seq = append(d.seq, o.ID)
	return o.ID
}

func (d *fakeDB) setStatus(id string, s domain.Status) {
	d.mu.Lock()
	defer d.mu.Unlock()
	o := d.orders[id]
	o.Status = s
	d.orders[id] = o
}

func (d *fakeDB) putKey(riderID string, rec domain.KeyRecord) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.keys[keyID(riderID, rec.Key)] = rec
}

func (d *fakeDB) order(id string) domain.Order {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.orders[id]
}

func (d *fakeDB) allOrders() []domain.Order {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []domain.Order
	for _, id := range d.seq {
		out = append(out, d.orders[id])
	}
	return out
}

func (d *fakeDB) queued() []queued {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]queued(nil), d.outbox...)
}

func (d *fakeDB) lookup(riderID, key string) *domain.KeyRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	if rec, ok := d.keys[keyID(riderID, key)]; ok {
		return &rec
	}
	return nil
}

func (d *fakeDB) Do(_ context.Context, fn func(Tx) error) error {
	d.rec.add("begin")
	if d.failBegin != nil {
		return d.failBegin
	}
	tx := &fakeTx{db: d}
	if err := fn(tx); err != nil {
		d.rec.add("rollback")
		return err
	}
	if d.failCommit != nil {
		d.rec.add("rollback")
		return d.failCommit
	}
	d.mu.Lock()
	for _, op := range tx.ops {
		op()
	}
	d.mu.Unlock()
	d.rec.add("commit")
	return nil
}

func (d *fakeDB) Lookup(_ context.Context, riderID, key string) (*domain.KeyRecord, error) {
	d.rec.add("keys.Lookup")
	return d.lookup(riderID, key), nil
}

func (d *fakeDB) Purge(context.Context) (int64, error) { return 0, nil }

func (d *fakeDB) ByID(_ context.Context, id string) (domain.Order, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failRead != nil {
		return domain.Order{}, d.failRead
	}
	o, ok := d.orders[id]
	if !ok {
		return domain.Order{}, domain.ErrOrderNotFound
	}
	return o, nil
}

func (d *fakeDB) LatestByRider(_ context.Context, riderID string) (domain.Order, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := len(d.seq) - 1; i >= 0; i-- {
		if o := d.orders[d.seq[i]]; o.RiderID == riderID {
			return o, nil
		}
	}
	return domain.Order{}, domain.ErrOrderNotFound
}

func (d *fakeDB) Set(_ context.Context, orderID string, status domain.Status, driverID string) (bool, error) {
	d.rec.add("Set")
	d.mu.Lock()
	defer d.mu.Unlock()
	o, ok := d.orders[orderID]
	if !ok || o.Status != domain.StatusPending {
		return false, nil
	}
	o.Status, o.DriverID = status, driverID
	d.orders[orderID] = o
	return true, nil
}

func (d *fakeDB) CancelMatched(_ context.Context, orderID, by, reason string) (bool, error) {
	d.rec.add("CancelMatched")
	d.mu.Lock()
	defer d.mu.Unlock()
	o, ok := d.orders[orderID]
	if !ok || o.Status != domain.StatusMatched {
		return false, nil
	}
	at := t0
	o.Status, o.CancelledAt, o.CancelledBy, o.CancelReason = domain.StatusCancelled, &at, by, reason
	d.orders[orderID] = o
	return true, nil
}

// fakeTx collects writes for fakeDB.Do to apply on commit.
type fakeTx struct {
	db  *fakeDB
	ops []func()
}

func (t *fakeTx) LockKey(context.Context, string, string) error {
	t.db.rec.add("LockKey")
	return nil
}

func (t *fakeTx) Lookup(_ context.Context, riderID, key string) (*domain.KeyRecord, error) {
	t.db.rec.add("Lookup")
	return t.db.lookup(riderID, key), nil
}

func (t *fakeTx) SaveKey(_ context.Context, riderID string, rec domain.KeyRecord) error {
	t.db.rec.add("SaveKey")
	if t.db.onSaveKey != nil {
		return t.db.onSaveKey()
	}
	if t.db.failSaveKey != nil {
		return t.db.failSaveKey
	}
	if t.db.lookup(riderID, rec.Key) != nil {
		return ErrDuplicateKey
	}
	t.ops = append(t.ops, func() { t.db.keys[keyID(riderID, rec.Key)] = rec })
	return nil
}

func (t *fakeTx) InsertOrder(_ context.Context, o NewOrder) error {
	t.db.rec.add("InsertOrder")
	if t.db.failInsert != nil {
		return t.db.failInsert
	}
	mult := o.Multiplier
	if mult == 0 {
		mult = 1.0
	}
	t.ops = append(t.ops, func() {
		t.db.orders[o.ID] = domain.Order{
			ID: o.ID, RiderID: o.RiderID, Pickup: o.Pickup, Dropoff: o.Dropoff, VehicleType: o.VehicleType,
			Status: domain.StatusPending, SurgeMultiplier: mult, Price: o.Price, PromoCode: o.PromoCode, Discount: o.Discount,
		}
		t.db.seq = append(t.db.seq, o.ID)
	})
	return nil
}

func (t *fakeTx) CancelPending(_ context.Context, orderID, reason string) (bool, error) {
	t.db.rec.add("CancelPending")
	if t.db.beforeCancel != nil {
		t.db.beforeCancel()
	}
	if t.db.order(orderID).Status != domain.StatusPending {
		return false, nil
	}
	t.ops = append(t.ops, func() {
		o := t.db.orders[orderID]
		at := t0
		o.Status, o.CancelledAt, o.CancelledBy, o.CancelReason = domain.StatusCancelled, &at, domain.CancelledByRider, reason
		t.db.orders[orderID] = o
	})
	return true, nil
}

func (t *fakeTx) QueueOrderCreated(_ context.Context, orderID string, payload []byte) error {
	t.db.rec.add("QueueOrderCreated")
	if t.db.failQueue != nil {
		return t.db.failQueue
	}
	t.ops = append(t.ops, func() { t.db.outbox = append(t.db.outbox, queued{"order.created", orderID, payload}) })
	return nil
}

func (t *fakeTx) QueueOrderCancelled(_ context.Context, orderID string, payload []byte) error {
	t.db.rec.add("QueueOrderCancelled")
	if t.db.failQueue != nil {
		return t.db.failQueue
	}
	t.ops = append(t.ops, func() { t.db.outbox = append(t.db.outbox, queued{"order.cancelled", orderID, payload}) })
	return nil
}

// fakeEncoder encodes events as JSON and keeps what it was given.
type fakeEncoder struct {
	mu                         sync.Mutex
	rec                        *recorder
	failCreated, failCancelled error
	created                    []OrderCreated
	cancelled                  []OrderCancelled
}

func (e *fakeEncoder) OrderCreated(_ context.Context, ev OrderCreated) ([]byte, error) {
	e.rec.add("encode order.created")
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.failCreated != nil {
		return nil, e.failCreated
	}
	e.created = append(e.created, ev)
	return json.Marshal(ev)
}

func (e *fakeEncoder) OrderCancelled(_ context.Context, ev OrderCancelled) ([]byte, error) {
	e.rec.add("encode order.cancelled")
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.failCancelled != nil {
		return nil, e.failCancelled
	}
	e.cancelled = append(e.cancelled, ev)
	return json.Marshal(ev)
}

// fakeWaker counts the relay wake-ups.
type fakeWaker struct {
	mu  sync.Mutex
	rec *recorder
	n   int
}

func (w *fakeWaker) Wake() { w.rec.add("wake"); w.mu.Lock(); w.n++; w.mu.Unlock() }

func (w *fakeWaker) count() int { w.mu.Lock(); defer w.mu.Unlock(); return w.n }

// testReceipt stands in for the HTTP adapter's receipt encoding.
func testReceipt(r Receipt) []byte {
	b, _ := json.Marshal(r)
	return append(b, '\n')
}

// testEnv is a Service over in-memory fakes of every port. Quotes and promo
// tokens share one claim store; the current multiplier is 1.2.
type testEnv struct {
	svc     *Service
	db      *fakeDB
	enc     *fakeEncoder
	relay   *fakeWaker
	metrics *fakeMetrics
	claims  *fakeClaims
	rec     *recorder
	pricer  *Pricer
	promos  *PromoChecker
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	signer, err := surge.NewSigner("test-secret")
	if err != nil {
		t.Fatal(err)
	}
	promoSigner, err := promo.NewSigner("test-promo-secret")
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	m := newFakeMetrics()
	claims := newFakeClaims()
	claims.rec = rec
	now := func() time.Time { return t0 }
	e := &testEnv{
		db: newFakeDB(rec), enc: &fakeEncoder{rec: rec}, relay: &fakeWaker{rec: rec}, metrics: m, claims: claims, rec: rec,
		pricer: &Pricer{Signer: signer, Mult: recordingMultiplier{rec, 1.2}, Claims: claims, Tolerance: 0.2, CellDeg: 0.05, Now: now, Metrics: m},
		promos: &PromoChecker{Signer: promoSigner, Claims: claims, Now: now, Metrics: m},
	}
	e.svc = New(Deps{
		Orders: e.db, Keys: e.db, Outcomes: e.db, UoW: e.db, Encoder: e.enc, Receipt: testReceipt,
		Pricer: e.pricer, Promos: e.promos, Relay: e.relay, Metrics: m,
	})
	return e
}

func (e *testEnv) book(riderID, key string, req domain.BookingRequest) (CreateOrderResult, error) {
	return e.svc.CreateOrder(context.Background(), CreateOrderCommand{RiderID: riderID, IdempotencyKey: key, Request: req})
}
