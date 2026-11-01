package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"grab-mini/payment-service/internal/domain"
)

// These tests run the use cases against in-memory fakes of the ports: no
// database, no broker. The same behaviour is proved against real Postgres and
// Kafka by the tests in internal/integration.

// ------------------------------------------------------------------ fakes ----

type memStore struct {
	orders      map[string]domain.OrderInfo
	assignments map[string]time.Time
	payments    map[string]domain.Payment // keyed by trip|kind
	events      map[string]bool
	ledger      []domain.Txn
	outbox      [][]byte
}

func newMemStore() *memStore {
	return &memStore{
		orders: map[string]domain.OrderInfo{}, assignments: map[string]time.Time{},
		payments: map[string]domain.Payment{}, events: map[string]bool{},
	}
}

func (m *memStore) SaveOrder(_ context.Context, orderID, riderID string, price *int64, discount int64) error {
	if _, ok := m.orders[orderID]; !ok {
		m.orders[orderID] = domain.OrderInfo{RiderID: riderID, Price: price, Discount: discount}
	}
	return nil
}
func (m *memStore) FindOrder(_ context.Context, orderID string) (domain.OrderInfo, bool, error) {
	o, ok := m.orders[orderID]
	return o, ok, nil
}
func (m *memStore) SaveAssignment(_ context.Context, orderID string, at time.Time) error {
	if _, ok := m.assignments[orderID]; !ok {
		m.assignments[orderID] = at
	}
	return nil
}
func (m *memStore) AssignedAt(_ context.Context, orderID string) (time.Time, bool, error) {
	at, ok := m.assignments[orderID]
	return at, ok, nil
}
func (m *memStore) Exists(_ context.Context, tripID, kind string) (bool, error) {
	_, ok := m.payments[tripID+"|"+kind]
	return ok, nil
}

// Do runs fn against a copy-on-commit view, so an error leaves nothing behind
// like a rolled-back transaction.
func (m *memStore) Do(_ context.Context, fn func(Tx) error) error {
	t := &memTx{m: m, events: map[string]bool{}, payments: map[string]domain.Payment{}}
	if err := fn(t); err != nil {
		return err
	}
	for k := range t.events {
		m.events[k] = true
	}
	for k, p := range t.payments {
		m.payments[k] = p
	}
	m.ledger = append(m.ledger, t.ledger...)
	m.outbox = append(m.outbox, t.outbox...)
	return nil
}

type memTx struct {
	m        *memStore
	events   map[string]bool
	payments map[string]domain.Payment
	ledger   []domain.Txn
	outbox   [][]byte
}

func (t *memTx) MarkEventProcessed(_ context.Context, id string) (bool, error) {
	if t.m.events[id] || t.events[id] {
		return false, nil
	}
	t.events[id] = true
	return true, nil
}
func (t *memTx) InsertPayment(_ context.Context, p domain.Payment) (bool, error) {
	k := p.TripID + "|" + p.Kind
	if _, ok := t.m.payments[k]; ok {
		return false, nil
	}
	t.payments[k] = p
	return true, nil
}
func (t *memTx) PostLedger(_ context.Context, x domain.Txn) error {
	t.ledger = append(t.ledger, x)
	return nil
}
func (t *memTx) EnqueuePaymentRecorded(_ context.Context, _ string, payload []byte) error {
	t.outbox = append(t.outbox, payload)
	return nil
}

type fakeProvider struct {
	err     error
	decline bool
	calls   int
}

func (f *fakeProvider) Name() string { return "test" }
func (f *fakeProvider) Charge(_ context.Context, _ ChargeRequest) (ChargeResult, error) {
	f.calls++
	if f.err != nil {
		return ChargeResult{}, f.err
	}
	if f.decline {
		return ChargeResult{Status: ChargeDeclined, DeclineReason: "no funds"}, nil
	}
	return ChargeResult{Status: ChargeCaptured, Ref: "ref"}, nil
}

type bytesEncoder struct{}

func (bytesEncoder) EncodePaymentRecorded(_ context.Context, rec PaymentRecorded) ([]byte, error) {
	return []byte(rec.Status), nil
}

type spyMetrics struct {
	recorded, duplicate, providerErrors, unbalanced int
	skipped                                         []string
	captured                                        int64
}

func (s *spyMetrics) Recorded(string, string)          { s.recorded++ }
func (s *spyMetrics) CapturedAmount(_ string, a int64) { s.captured += a }
func (s *spyMetrics) Duplicate(string)                 { s.duplicate++ }
func (s *spyMetrics) Skipped(r string)                 { s.skipped = append(s.skipped, r) }
func (s *spyMetrics) ProviderError()                   { s.providerErrors++ }
func (s *spyMetrics) LedgerUnbalanced()                { s.unbalanced++ }

type rig struct {
	svc      *Service
	store    *memStore
	provider *fakeProvider
	metrics  *spyMetrics
}

func newRig(policy domain.Policy) *rig {
	r := &rig{store: newMemStore(), provider: &fakeProvider{}, metrics: &spyMetrics{}}
	r.svc = NewService(Deps{
		Orders: r.store, Assignments: r.store, Payments: r.store, UnitOfWork: r.store,
		Encoder: bytesEncoder{}, Provider: r.provider, Policy: policy, Metrics: r.metrics,
	})
	return r
}

func price(p float64) *float64 { return &p }

func completed(order, trip string) TripStatusChanged {
	return TripStatusChanged{EventID: uuid.NewString(), TripID: trip, OrderID: order, DriverID: "driver-1", RiderID: "rider-1", Status: TripCompleted}
}

// ------------------------------------------------------------------ tests ----

func TestACompletedTripIsChargedOnceAndAnnounced(t *testing.T) {
	r := newRig(domain.Policy{CommissionBps: 2000})
	ctx := context.Background()
	if err := r.svc.RecordOrder(ctx, OrderCreated{OrderID: "o1", RiderID: "rider-1", Price: price(100000)}); err != nil {
		t.Fatal(err)
	}
	evt := completed("o1", "t1")
	for i := 0; i < 2; i++ { // the second is a redelivery of the same event
		if err := r.svc.SettleTrip(ctx, evt); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(r.store.payments); n != 1 {
		t.Fatalf("%d payments, want 1", n)
	}
	p := r.store.payments["t1|FARE"]
	if p.Status != domain.StatusCaptured || p.Amount != 100000 || p.DriverAmount != 80000 || p.PlatformAmount != 20000 || p.LedgerTxnID == nil {
		t.Errorf("payment = %+v", p)
	}
	if len(r.store.ledger) != 1 || r.store.ledger[0].Validate() != nil {
		t.Errorf("ledger = %+v, want one balanced transaction", r.store.ledger)
	}
	if len(r.store.outbox) != 1 {
		t.Errorf("%d events queued, want 1", len(r.store.outbox))
	}
	if r.provider.calls != 1 {
		t.Errorf("the provider was called %d times, want 1", r.provider.calls)
	}
	if r.metrics.recorded != 1 || r.metrics.captured != 100000 || r.metrics.duplicate != 1 {
		t.Errorf("metrics = %+v", r.metrics)
	}
}

func TestADeclinedChargeIsRecordedWithoutALedgerTransaction(t *testing.T) {
	r := newRig(domain.Policy{CommissionBps: 2000})
	r.provider.decline = true
	ctx := context.Background()
	r.svc.RecordOrder(ctx, OrderCreated{OrderID: "o1", RiderID: "rider-1", Price: price(50000)})
	if err := r.svc.SettleTrip(ctx, completed("o1", "t1")); err != nil {
		t.Fatal(err)
	}
	p := r.store.payments["t1|FARE"]
	if p.Status != domain.StatusDeclined || p.LedgerTxnID != nil || p.DriverAmount != 0 || len(r.store.ledger) != 0 {
		t.Errorf("payment = %+v, ledger %d", p, len(r.store.ledger))
	}
	if len(r.store.outbox) != 1 {
		t.Errorf("a decline is announced too: %d events queued", len(r.store.outbox))
	}
}

func TestAProviderErrorLeavesNothingBehindAndIsCounted(t *testing.T) {
	r := newRig(domain.Policy{CommissionBps: 2000})
	r.provider.err = errors.New("timeout")
	ctx := context.Background()
	r.svc.RecordOrder(ctx, OrderCreated{OrderID: "o1", RiderID: "rider-1", Price: price(50000)})
	evt := completed("o1", "t1")
	if err := r.svc.SettleTrip(ctx, evt); err == nil {
		t.Fatal("a provider error must be returned so the event is retried")
	}
	if len(r.store.payments)+len(r.store.events)+len(r.store.outbox) != 0 || r.metrics.providerErrors != 1 {
		t.Errorf("state left behind: %+v, metrics %+v", r.store, r.metrics)
	}
	r.provider.err = nil // recovered: the retry of the same event succeeds
	if err := r.svc.SettleTrip(ctx, evt); err != nil || len(r.store.payments) != 1 {
		t.Errorf("retry: %v, %d payments", err, len(r.store.payments))
	}
}

func TestATripEventBeforeItsOrderAsksForARetry(t *testing.T) {
	r := newRig(domain.Policy{CommissionBps: 2000})
	err := r.svc.SettleTrip(context.Background(), completed("unknown", "t1"))
	var unknown *OrderUnknownError
	if !errors.As(err, &unknown) {
		t.Fatalf("error = %v, want OrderUnknownError", err)
	}
}

func TestACancellationUsesTheRecordedAssignmentTime(t *testing.T) {
	policy := domain.Policy{CommissionBps: 2000, CancellationFee: 15000, CancellationGrace: 3 * time.Minute}
	r := newRig(policy)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC)
	r.svc.RecordOrder(ctx, OrderCreated{OrderID: "o1", RiderID: "rider-1", Price: price(100000)})
	r.svc.RecordAssignment(ctx, OrderMatched{OrderID: "o1", OccurredAt: t0.Format(time.RFC3339Nano)})
	// A second order.matched (a redelivery) must not restart the clock.
	r.svc.RecordAssignment(ctx, OrderMatched{OrderID: "o1", OccurredAt: t0.Add(time.Hour).Format(time.RFC3339Nano)})

	cancel := func(trip string, after time.Duration) TripStatusChanged {
		e := completed("o1", trip)
		e.Status, e.CancelledBy, e.OccurredAt = TripCancelled, domain.CancelledByRider, t0.Add(after).Format(time.RFC3339Nano)
		return e
	}
	if err := r.svc.SettleTrip(ctx, cancel("t-free", time.Minute)); err != nil || len(r.store.payments) != 0 {
		t.Fatalf("a cancel within the grace period: %v, %d payments", err, len(r.store.payments))
	}
	if err := r.svc.SettleTrip(ctx, cancel("t-fee", 10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if p := r.store.payments["t-fee|CANCELLATION_FEE"]; p.Amount != 15000 {
		t.Errorf("fee payment = %+v, want 15000", p)
	}
}

func TestAnInvalidEventIsPermanent(t *testing.T) {
	r := newRig(domain.Policy{CommissionBps: 2000})
	ctx := context.Background()
	for name, err := range map[string]error{
		"order without a rider":     r.svc.RecordOrder(ctx, OrderCreated{OrderID: "o"}),
		"matched without an order":  r.svc.RecordAssignment(ctx, OrderMatched{}),
		"matched with a bad time":   r.svc.RecordAssignment(ctx, OrderMatched{OrderID: "o", OccurredAt: "yesterday"}),
		"completed without a trip":  r.svc.SettleTrip(ctx, TripStatusChanged{Status: TripCompleted}),
		"tip without a rider":       r.svc.SettleTip(ctx, TripRated{TripID: "t", OrderID: "o", DriverID: "d", TipAmount: 5}),
		"tip in a foreign currency": r.svc.SettleTip(ctx, TripRated{TripID: "t", OrderID: "o", DriverID: "d", RiderID: "r", TipAmount: 5, Currency: "USD"}),
		"negative tip":              r.svc.SettleTip(ctx, TripRated{TripID: "t", OrderID: "o", DriverID: "d", RiderID: "r", TipAmount: -5}),
		"discount above the price":  r.svc.RecordOrder(ctx, OrderCreated{OrderID: "o", RiderID: "r", Price: price(1000), Discount: 2000}),
	} {
		p, ok := err.(interface{ Permanent() bool })
		if !ok || !p.Permanent() {
			t.Errorf("%s: error = %v, want a permanent error", name, err)
		}
	}
}

func TestAnEventIdThatIsNotAUUIDIsPermanent(t *testing.T) {
	r := newRig(domain.Policy{CommissionBps: 2000})
	ctx := context.Background()
	r.svc.RecordOrder(ctx, OrderCreated{OrderID: "o1", RiderID: "rider-1", Price: price(50000)})
	evt := completed("o1", "t1")
	evt.EventID = "not-a-uuid"
	err := r.svc.SettleTrip(ctx, evt)
	var inv *InvalidEventError
	if !errors.As(err, &inv) {
		t.Fatalf("error = %v, want InvalidEventError", err)
	}
}

func TestAnUnbalancedLedgerTransactionIsCountedAsTheMetricSays(t *testing.T) {
	r := newRig(domain.Policy{CommissionBps: 2000})
	bad := domain.Txn{ID: "t", Key: "fare:1", Kind: "FARE", Currency: domain.CurrencyVND, Entries: []domain.Entry{
		{Acct: domain.RiderAccount("r"), Amount: -100},
		{Acct: domain.PlatformRevenueAccount(), Amount: 99},
	}}
	if err := r.svc.validateTxn(bad); err == nil {
		t.Fatal("an unbalanced transaction was accepted")
	}
	if r.metrics.unbalanced != 1 {
		t.Errorf("ledger_unbalanced_rejected_total rose by %d, want 1", r.metrics.unbalanced)
	}
	if err := r.svc.validateTxn(domain.ChargeTxn("t", "k", domain.KindFare, "r", "d", 80, 20)); err != nil || r.metrics.unbalanced != 1 {
		t.Errorf("a balanced transaction: %v, count %d", err, r.metrics.unbalanced)
	}
}

// Trip Service stamps events with whole seconds (time.RFC3339); the cancel timing must read them.
func TestParseEventTimeReadsWhatTripServiceWrites(t *testing.T) {
	trip := time.Date(2026, 10, 31, 9, 0, 7, 0, time.UTC).Format(time.RFC3339) // "2026-10-31T09:00:07Z"
	got, err := parseEventTime(trip)
	if err != nil || !got.Equal(time.Date(2026, 10, 31, 9, 0, 7, 0, time.UTC)) {
		t.Errorf("parseEventTime(%q) = %v, %v", trip, got, err)
	}
	if _, err := parseEventTime("2026-10-31T09:00:07.250+07:00"); err != nil {
		t.Errorf("fractional seconds with an offset: %v", err)
	}
	for _, bad := range []string{"", "yesterday", "2026-10-31"} {
		if _, err := parseEventTime(bad); err == nil {
			t.Errorf("parseEventTime(%q) succeeded", bad)
		}
	}
}
