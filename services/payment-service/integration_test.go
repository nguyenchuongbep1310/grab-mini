package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
)

// These tests need Postgres: set TEST_DATABASE_URL to a throwaway database
// (they truncate the payment tables and delete this service's outbox rows).

// jsonEncoder stands in for the Avro codec: the tests read back what was queued.
type jsonEncoder struct{}

func (jsonEncoder) Encode(_ context.Context, _ string, v any) ([]byte, error) { return json.Marshal(v) }

type fixture struct {
	t        *testing.T
	db       *pgxpool.Pool
	provider *FakeProvider
	svc      *service
}

func newFixture(t *testing.T, policy Policy) *fixture {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	db, err := connectDB(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	// TRUNCATE is not a row-level change, so the ledger's append-only triggers allow it.
	if _, err := db.Exec(ctx, `
		TRUNCATE payments, payment_events, order_prices, ledger_entries, ledger_transactions, ledger_accounts;
		DELETE FROM outbox WHERE source = 'payment-service'`); err != nil {
		t.Fatal(err)
	}
	provider := NewFakeProvider()
	return &fixture{t: t, db: db, provider: provider, svc: newService(db, provider, policy, jsonEncoder{}, nil)}
}

var defaultPolicy = Policy{CommissionBps: 2000}

func newID() string { return uuid.NewString() }

func (f *fixture) order(rider string, price *float64) string {
	f.t.Helper()
	orderID := newID()
	err := f.svc.recordOrder(context.Background(), events.OrderCreated{
		Envelope: events.Envelope{EventID: newID()}, OrderID: orderID, RiderID: rider, Price: price, SurgeMultiplier: 1,
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return orderID
}

func fl(v float64) *float64 { return &v }

func status(orderID, tripID, driver, rider string, s events.TripStatus, by string) events.TripStatusChanged {
	return events.TripStatusChanged{
		Envelope: events.Envelope{EventID: newID()}, TripID: tripID, OrderID: orderID, DriverID: driver,
		RiderID: rider, Status: s, CancelledBy: by,
	}
}

func rated(orderID, tripID, driver, rider string, tip int64) events.TripRated {
	return events.TripRated{
		Envelope: events.Envelope{EventID: newID()}, TripID: tripID, OrderID: orderID, DriverID: driver, RiderID: rider,
		Stars: 5, TipAmount: tip, Currency: "VND",
	}
}

func (f *fixture) scalar(sql string, args ...any) int64 {
	f.t.Helper()
	var n int64
	if err := f.db.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		f.t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func (f *fixture) balance(acct account) int64 {
	return f.scalar(`SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE account_id = $1`, acct.id)
}

func (f *fixture) payments(tripID string) int64 {
	return f.scalar(`SELECT count(*) FROM payments WHERE trip_id = $1`, tripID)
}

func (f *fixture) outboxRows() int64 {
	return f.scalar(`SELECT count(*) FROM outbox WHERE source = 'payment-service' AND topic = $1`, events.TopicPaymentRecorded)
}

// assertLedgerBalances is the SQL check: every ledger transaction sums to zero,
// and so does the whole ledger.
func (f *fixture) assertLedgerBalances() {
	f.t.Helper()
	if n := f.scalar(`SELECT count(*) FROM (SELECT txn_id FROM ledger_entries GROUP BY txn_id HAVING SUM(amount) <> 0) bad`); n != 0 {
		f.t.Errorf("%d ledger transactions do not sum to zero", n)
	}
	if n := f.scalar(`SELECT COALESCE(SUM(amount), 0) FROM ledger_entries`); n != 0 {
		f.t.Errorf("the ledger sums to %d, want 0", n)
	}
	if n := f.scalar(`SELECT count(*) FROM ledger_transactions t WHERE NOT EXISTS (SELECT 1 FROM ledger_entries e WHERE e.txn_id = t.id)`); n != 0 {
		f.t.Errorf("%d ledger transactions have no entries", n)
	}
}

func TestCompletedTripCapturesTheLockedFareOnce(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	order, trip := f.order("rider-1", fl(100000)), newID()

	if err := f.svc.onTripStatus(ctx, status(order, trip, "driver-1", "rider-1", events.TripStatusCompleted, "")); err != nil {
		t.Fatal(err)
	}

	if n := f.payments(trip); n != 1 {
		t.Fatalf("%d payments, want 1", n)
	}
	var kind, st string
	var amount, driver, platform int64
	if err := f.db.QueryRow(ctx, `SELECT kind, status, amount, driver_amount, platform_amount FROM payments WHERE trip_id = $1`, trip).
		Scan(&kind, &st, &amount, &driver, &platform); err != nil {
		t.Fatal(err)
	}
	if kind != "FARE" || st != "CAPTURED" || amount != 100000 || driver != 80000 || platform != 20000 {
		t.Errorf("payment = %s %s %d (driver %d, platform %d)", kind, st, amount, driver, platform)
	}
	if got := f.balance(riderAccount("rider-1")); got != -100000 {
		t.Errorf("rider balance = %d, want -100000", got)
	}
	if got := f.balance(driverPayableAccount("driver-1")); got != 80000 {
		t.Errorf("driver payable = %d, want 80000", got)
	}
	if got := f.balance(platformRevenueAccount()); got != 20000 {
		t.Errorf("platform revenue = %d, want 20000", got)
	}
	f.assertLedgerBalances()

	// One payment.recorded, queued in the same transaction.
	if n := f.outboxRows(); n != 1 {
		t.Fatalf("%d payment.recorded events queued, want 1", n)
	}
	var payload []byte
	f.db.QueryRow(ctx, `SELECT payload FROM outbox WHERE source = 'payment-service'`).Scan(&payload)
	var rec events.PaymentRecorded
	if err := json.Unmarshal(payload, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.TripID != trip || rec.Kind != "FARE" || rec.Status != "CAPTURED" || rec.Amount != 100000 ||
		rec.DriverAmount != 80000 || rec.PlatformAmount != 20000 || rec.LedgerTransactionID == "" || rec.RiderID != "rider-1" {
		t.Errorf("payment.recorded = %+v", rec)
	}
}

func TestRedeliveryAndReplayDoNotChargeTwice(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	order, trip := f.order("rider-1", fl(100000)), newID()
	evt := status(order, trip, "driver-1", "rider-1", events.TripStatusCompleted, "")

	for i := 0; i < 3; i++ { // the same event, redelivered
		if err := f.svc.onTripStatus(ctx, evt); err != nil {
			t.Fatal(err)
		}
	}
	// A different event for the same trip (a replay from another producer run).
	replay := evt
	replay.EventID = newID()
	if err := f.svc.onTripStatus(ctx, replay); err != nil {
		t.Fatal(err)
	}

	if n := f.payments(trip); n != 1 {
		t.Errorf("%d payments, want 1", n)
	}
	if n := f.scalar(`SELECT count(*) FROM ledger_transactions`); n != 1 {
		t.Errorf("%d ledger transactions, want 1", n)
	}
	if got := f.balance(riderAccount("rider-1")); got != -100000 {
		t.Errorf("rider balance = %d, want -100000 (charged once)", got)
	}
	if n := f.outboxRows(); n != 1 {
		t.Errorf("%d events queued, want 1", n)
	}
	if f.provider.Calls != 1 {
		t.Errorf("the provider was called %d times, want 1", f.provider.Calls)
	}
	f.assertLedgerBalances()
}

func TestConcurrentDeliveriesOfTheSameTripChargeOnce(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	order, trip := f.order("rider-1", fl(60000)), newID()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine is another instance with its own copy of the event.
			if err := f.svc.onTripStatus(ctx, status(order, trip, "driver-1", "rider-1", events.TripStatusCompleted, "")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if n := f.payments(trip); n != 1 {
		t.Errorf("%d payments, want 1", n)
	}
	if got := f.balance(riderAccount("rider-1")); got != -60000 {
		t.Errorf("rider balance = %d, want -60000", got)
	}
	if n := f.outboxRows(); n != 1 {
		t.Errorf("%d events queued, want 1", n)
	}
	f.assertLedgerBalances()
}

func TestTipIsASeparateChargeAndAllOfItGoesToTheDriver(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	order, trip := f.order("rider-1", fl(100000)), newID()
	f.svc.onTripStatus(ctx, status(order, trip, "driver-1", "rider-1", events.TripStatusCompleted, ""))

	evt := rated(order, trip, "driver-1", "rider-1", 20000)
	for i := 0; i < 2; i++ { // and redelivered
		if err := f.svc.onTripRated(ctx, evt); err != nil {
			t.Fatal(err)
		}
	}

	if n := f.payments(trip); n != 2 {
		t.Fatalf("%d payments for the trip, want fare + tip", n)
	}
	var amount, driver, platform int64
	if err := f.db.QueryRow(ctx, `SELECT amount, driver_amount, platform_amount FROM payments WHERE trip_id = $1 AND kind = 'TIP'`, trip).
		Scan(&amount, &driver, &platform); err != nil {
		t.Fatal(err)
	}
	if amount != 20000 || driver != 20000 || platform != 0 {
		t.Errorf("tip = %d (driver %d, platform %d), want 20000 to the driver", amount, driver, platform)
	}
	if got := f.balance(riderAccount("rider-1")); got != -120000 {
		t.Errorf("rider balance = %d, want -120000 (fare + tip)", got)
	}
	if got := f.balance(driverPayableAccount("driver-1")); got != 100000 {
		t.Errorf("driver payable = %d, want 100000 (80000 + the tip)", got)
	}
	if got := f.balance(platformRevenueAccount()); got != 20000 {
		t.Errorf("platform revenue = %d, want 20000 (no commission on tips)", got)
	}
	f.assertLedgerBalances()
}

func TestZeroTipAndBadTipsChargeNothing(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	order, trip := f.order("rider-1", fl(50000)), newID()

	if err := f.svc.onTripRated(ctx, rated(order, trip, "driver-1", "rider-1", 0)); err != nil {
		t.Fatal(err)
	}
	if n := f.payments(trip); n != 0 {
		t.Errorf("a zero tip produced %d payments", n)
	}

	for name, evt := range map[string]events.TripRated{
		"negative": rated(order, trip, "driver-1", "rider-1", -5),
		"currency": func() events.TripRated {
			e := rated(order, trip, "driver-1", "rider-1", 100)
			e.Currency = "USD"
			return e
		}(),
		"driver": rated(order, trip, "", "rider-1", 100),
		"rider":  rated(order, trip, "driver-1", "", 100),
	} {
		err := f.svc.onTripRated(ctx, evt)
		var p interface{ Permanent() bool }
		if !errors.As(err, &p) || !p.Permanent() {
			t.Errorf("%s: error = %v, want a permanent one (straight to the DLQ)", name, err)
		}
	}
	if n := f.payments(trip); n != 0 {
		t.Errorf("%d payments from invalid tips", n)
	}
}

func TestRiderCancelAfterAssignmentPaysTheConfiguredFee(t *testing.T) {
	f := newFixture(t, Policy{CommissionBps: 2000, CancellationFee: 15000})
	ctx := context.Background()
	order, trip := f.order("rider-1", fl(100000)), newID()

	if err := f.svc.onTripStatus(ctx, status(order, trip, "driver-1", "rider-1", events.TripStatusCancelled, events.CancelledByRider)); err != nil {
		t.Fatal(err)
	}

	var kind string
	var amount, driver, platform int64
	if err := f.db.QueryRow(ctx, `SELECT kind, amount, driver_amount, platform_amount FROM payments WHERE trip_id = $1`, trip).
		Scan(&kind, &amount, &driver, &platform); err != nil {
		t.Fatal(err)
	}
	if kind != "CANCELLATION_FEE" || amount != 15000 || driver != 12000 || platform != 3000 {
		t.Errorf("payment = %s %d (driver %d, platform %d)", kind, amount, driver, platform)
	}
	if got := f.balance(riderAccount("rider-1")); got != -15000 {
		t.Errorf("rider balance = %d, want -15000", got)
	}
	f.assertLedgerBalances()
}

func TestRiderCancelFeeIsOffByDefault(t *testing.T) {
	f := newFixture(t, defaultPolicy) // CancellationFee 0
	order, trip := f.order("rider-1", fl(100000)), newID()
	if err := f.svc.onTripStatus(context.Background(), status(order, trip, "driver-1", "rider-1", events.TripStatusCancelled, events.CancelledByRider)); err != nil {
		t.Fatal(err)
	}
	if n := f.payments(trip); n != 0 {
		t.Errorf("%d payments, want none: the default keeps cancellation free", n)
	}
}

func TestDriverCancelNeverCostsTheRider(t *testing.T) {
	f := newFixture(t, Policy{CommissionBps: 2000, CancellationFee: 15000})
	order, trip := f.order("rider-1", fl(100000)), newID()
	if err := f.svc.onTripStatus(context.Background(), status(order, trip, "driver-1", "rider-1", events.TripStatusCancelled, events.CancelledByDriver)); err != nil {
		t.Fatal(err)
	}
	if n := f.payments(trip); n != 0 {
		t.Errorf("%d payments, want none when the driver cancels", n)
	}
	if n := f.scalar(`SELECT count(*) FROM ledger_entries`); n != 0 {
		t.Errorf("%d ledger entries, want none", n)
	}
}

func TestPendingCancelIsNeverSeenByPaymentService(t *testing.T) {
	// A rider cancelling a PENDING order (no driver yet) publishes order.cancelled
	// and no trip event. Payment Service does not read order.cancelled, so there
	// is nothing to charge by construction.
	for _, topic := range consumedTopics {
		if topic == events.TopicOrderCancelled {
			t.Fatal("payment-service consumes order.cancelled: a PENDING cancel could be charged")
		}
	}
}

func TestAnUnknownOrderIsRetriedUntilOrderCreatedArrives(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	orderID, trip := newID(), newID()

	err := f.svc.onTripStatus(ctx, status(orderID, trip, "driver-1", "rider-1", events.TripStatusCompleted, ""))
	var unknown *errOrderUnknown
	if !errors.As(err, &unknown) {
		t.Fatalf("error = %v, want errOrderUnknown", err)
	}
	var p interface{ Permanent() bool }
	if errors.As(err, &p) {
		t.Fatal("an unknown order must be retried, not dead-lettered at once")
	}
	if n := f.payments(trip); n != 0 {
		t.Fatalf("%d payments before the order is known", n)
	}

	// order.created shows up; the retry now charges.
	if err := f.svc.recordOrder(ctx, events.OrderCreated{Envelope: events.Envelope{EventID: newID()}, OrderID: orderID, RiderID: "rider-1", Price: fl(40000)}); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.onTripStatus(ctx, status(orderID, trip, "driver-1", "rider-1", events.TripStatusCompleted, "")); err != nil {
		t.Fatal(err)
	}
	if n := f.payments(trip); n != 1 {
		t.Errorf("%d payments after the retry, want 1", n)
	}
}

func TestAnOrderWithoutAPriceChargesNothing(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	order, trip := f.order("rider-1", nil), newID() // order.created from before prices existed
	if err := f.svc.onTripStatus(context.Background(), status(order, trip, "driver-1", "rider-1", events.TripStatusCompleted, "")); err != nil {
		t.Fatal(err)
	}
	if n := f.payments(trip); n != 0 {
		t.Errorf("%d payments, want none", n)
	}
}

func TestRecordOrderKeepsTheFirstPrice(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	orderID := newID()
	for _, p := range []float64{80000, 99999} { // a redelivered or conflicting copy must not move the price
		if err := f.svc.recordOrder(ctx, events.OrderCreated{OrderID: orderID, RiderID: "rider-1", Price: fl(p)}); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.scalar(`SELECT price FROM order_prices WHERE order_id = $1`, orderID); got != 80000 {
		t.Errorf("price = %d, want 80000", got)
	}
	err := f.svc.recordOrder(ctx, events.OrderCreated{OrderID: newID(), RiderID: "r", Price: fl(-1)})
	var p interface{ Permanent() bool }
	if !errors.As(err, &p) || !p.Permanent() {
		t.Errorf("a negative price: error = %v, want a permanent one", err)
	}
}

func TestDeclinedFareIsRecordedWithoutLedgerEntries(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	order, trip := f.order("decline-rider", fl(100000)), newID()

	if err := f.svc.onTripStatus(ctx, status(order, trip, "driver-1", "decline-rider", events.TripStatusCompleted, "")); err != nil {
		t.Fatal(err)
	}
	var st string
	f.db.QueryRow(ctx, `SELECT status FROM payments WHERE trip_id = $1`, trip).Scan(&st)
	if st != "DECLINED" {
		t.Errorf("status = %q, want DECLINED", st)
	}
	if n := f.scalar(`SELECT count(*) FROM ledger_entries`); n != 0 {
		t.Errorf("%d ledger entries for a declined charge, want none", n)
	}
	if n := f.outboxRows(); n != 1 {
		t.Errorf("%d events queued, want the DECLINED one", n)
	}
	if got := f.balance(driverPayableAccount("driver-1")); got != 0 {
		t.Errorf("driver payable = %d after a declined fare, want 0", got)
	}
}

func TestProviderOutageIsRetriedAndLeavesNothingBehind(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	order, trip := f.order("rider-1", fl(100000)), newID()
	f.svc.provider = failingProvider{}
	evt := status(order, trip, "driver-1", "rider-1", events.TripStatusCompleted, "")

	if err := f.svc.onTripStatus(context.Background(), evt); err == nil {
		t.Fatal("a provider outage must surface as an error so the event is retried")
	}
	if n := f.payments(trip); n != 0 {
		t.Errorf("%d payments after an outage", n)
	}
	f.svc.provider = f.provider // recovered: the retry of the same event succeeds
	if err := f.svc.onTripStatus(context.Background(), evt); err != nil {
		t.Fatal(err)
	}
	if n := f.payments(trip); n != 1 {
		t.Errorf("%d payments after the retry, want 1", n)
	}
}

type failingProvider struct{}

func (failingProvider) Name() string { return "down" }
func (failingProvider) Charge(context.Context, ChargeRequest) (ChargeResult, error) {
	return ChargeResult{}, errors.New("provider timeout")
}

// ------------------------------------------------ the database's own checks ----

func TestDatabaseRefusesAnUnbalancedLedgerTransaction(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	tx, err := f.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	id := newID()
	for _, sql := range []string{
		`INSERT INTO ledger_accounts (id, kind) VALUES ('rider:x', 'RIDER'), ('platform_revenue', 'PLATFORM_REVENUE') ON CONFLICT DO NOTHING`,
		`INSERT INTO ledger_transactions (id, idempotency_key, kind) VALUES ('` + id + `', 'bad:1', 'FARE')`,
		`INSERT INTO ledger_entries (txn_id, account_id, amount) VALUES ('` + id + `', 'rider:x', -100)`,
		`INSERT INTO ledger_entries (txn_id, account_id, amount) VALUES ('` + id + `', 'platform_revenue', 99)`,
	} {
		if _, err := tx.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v (the check must fire at commit, not per row)", sql, err)
		}
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("commit of an unbalanced ledger transaction succeeded")
	}
	if n := f.scalar(`SELECT count(*) FROM ledger_entries`); n != 0 {
		t.Errorf("%d entries persisted", n)
	}
}

func TestDatabaseRefusesToEditOrDeleteLedgerHistory(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	order, trip := f.order("rider-1", fl(100000)), newID()
	f.svc.onTripStatus(ctx, status(order, trip, "driver-1", "rider-1", events.TripStatusCompleted, ""))

	for _, sql := range []string{
		`UPDATE ledger_entries SET amount = amount + 1`,
		`DELETE FROM ledger_entries`,
		`DELETE FROM ledger_transactions`,
		`UPDATE ledger_transactions SET kind = 'X'`,
	} {
		if _, err := f.db.Exec(ctx, sql); err == nil {
			t.Errorf("%q succeeded on an append-only ledger", sql)
		}
	}
	f.assertLedgerBalances()
}

func TestPaymentsRowsCannotContradictTheirLedger(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	// driver + platform must equal the amount for a captured payment.
	_, err := f.db.Exec(ctx, `INSERT INTO payments (id, trip_id, order_id, rider_id, driver_id, kind, status, amount, driver_amount, platform_amount, provider, source_event_id)
		VALUES ($1, 't', 'o', 'r', 'd', 'FARE', 'CAPTURED', 100, 50, 49, 'fake', $2)`, newID(), newID())
	if err == nil {
		t.Error("an inconsistent payment row was accepted")
	}
}

// -------------------------------------------------------------------- API ----

func (f *fixture) get(url string, id auth.Identity, h http.HandlerFunc, pathID string) (int, map[string]any) {
	f.t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil).WithContext(auth.With(context.Background(), id))
	if pathID != "" {
		req.SetPathValue("id", pathID)
	}
	rec := httptest.NewRecorder()
	h(rec, req)
	var body map[string]any
	json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

func TestPaymentsAPIShowsEachSideTheirOwnView(t *testing.T) {
	f := newFixture(t, defaultPolicy)
	ctx := context.Background()
	order, trip := f.order("rider-1", fl(100000)), newID()
	f.svc.onTripStatus(ctx, status(order, trip, "driver-1", "rider-1", events.TripStatusCompleted, ""))
	f.svc.onTripRated(ctx, rated(order, trip, "driver-1", "rider-1", 20000))

	rider := auth.Identity{Subject: "rider-1", Role: auth.Rider}
	code, body := f.get("/payments?tripId="+trip, rider, f.svc.handleGetPayments, "")
	if code != 200 || body["total"] != float64(120000) || len(body["payments"].([]any)) != 2 {
		t.Errorf("rider view = %d %v, want 2 payments totalling 120000", code, body)
	}

	driver := auth.Identity{Subject: "driver-1", Role: auth.Driver}
	code, body = f.get("/payments?tripId="+trip, driver, f.svc.handleGetPayments, "")
	if code != 200 || body["total"] != float64(100000) {
		t.Errorf("driver view = %d %v, want payouts totalling 100000", code, body)
	}
	payouts := body["payouts"].([]any)
	if len(payouts) != 2 || payouts[0].(map[string]any)["commission"] != float64(20000) {
		t.Errorf("payouts = %v", payouts)
	}
	if _, leaks := body["payments"]; leaks {
		t.Error("the driver's response carries the rider's payments")
	}

	// Someone else's trip looks the same as a trip with no payments.
	for _, other := range []auth.Identity{{Subject: "rider-2", Role: auth.Rider}, {Subject: "driver-2", Role: auth.Driver}} {
		code, body = f.get("/payments?tripId="+trip, other, f.svc.handleGetPayments, "")
		if code != 200 || body["total"] != float64(0) {
			t.Errorf("%s sees %d %v of a trip that is not theirs", other.Subject, code, body)
		}
	}
	if code, _ := f.get("/payments", rider, f.svc.handleGetPayments, ""); code != 400 {
		t.Errorf("no tripId: %d, want 400", code)
	}
}

func TestEarningsSummaryAndOwnership(t *testing.T) {
	f := newFixture(t, Policy{CommissionBps: 2000, CancellationFee: 10000})
	ctx := context.Background()
	o1, t1 := f.order("rider-1", fl(100000)), newID()
	o2, t2 := f.order("rider-2", fl(50000)), newID()
	o3, t3 := f.order("rider-3", fl(70000)), newID()
	f.svc.onTripStatus(ctx, status(o1, t1, "driver-1", "rider-1", events.TripStatusCompleted, ""))
	f.svc.onTripRated(ctx, rated(o1, t1, "driver-1", "rider-1", 10000))
	f.svc.onTripStatus(ctx, status(o2, t2, "driver-1", "rider-2", events.TripStatusCompleted, ""))
	f.svc.onTripStatus(ctx, status(o3, t3, "driver-1", "rider-3", events.TripStatusCancelled, events.CancelledByRider))
	// Another driver's money must not leak in.
	o4 := f.order("rider-4", fl(90000))
	f.svc.onTripStatus(ctx, status(o4, newID(), "driver-2", "rider-4", events.TripStatusCompleted, ""))

	me := auth.Identity{Subject: "driver-1", Role: auth.Driver}
	code, body := f.get("/drivers/driver-1/earnings", me, f.svc.handleEarnings, "driver-1")
	if code != 200 {
		t.Fatalf("status %d: %v", code, body)
	}
	want := map[string]float64{
		"fares": 120000, "tips": 10000, "cancellationFees": 8000, "total": 138000, "commission": 32000, "trips": 2, "payableBalance": 138000,
	}
	for k, v := range want {
		if body[k] != v {
			t.Errorf("%s = %v, want %v (body %v)", k, body[k], v, body)
		}
	}

	if code, _ := f.get("/drivers/driver-2/earnings", me, f.svc.handleEarnings, "driver-2"); code != 403 {
		t.Errorf("reading another driver's earnings: %d, want 403", code)
	}
	rider := auth.Identity{Subject: "rider-1", Role: auth.Rider}
	if code, _ := f.get("/drivers/driver-1/earnings", rider, f.svc.handleEarnings, "driver-1"); code != 403 {
		t.Errorf("a rider reading earnings: %d, want 403", code)
	}
	f.assertLedgerBalances()
}
