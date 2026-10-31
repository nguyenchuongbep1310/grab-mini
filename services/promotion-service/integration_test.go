package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/promo"
)

// These tests need Postgres: set TEST_DATABASE_URL to a throwaway database
// (they truncate the promotion tables and delete this service's outbox rows).

// jsonEncoder stands in for the Avro codec: the tests read back what was queued.
type jsonEncoder struct{}

func (jsonEncoder) Encode(_ context.Context, _ string, v any) ([]byte, error) { return json.Marshal(v) }

type fixture struct {
	t      *testing.T
	db     *pgxpool.Pool
	svc    *service
	signer *promo.Signer
	issuer *auth.Issuer
	api    http.Handler
	now    time.Time
}

var testPriv ed25519.PrivateKey

func newFixture(t *testing.T) *fixture {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	t.Setenv(auth.RateLimitScaleEnv, "0") // the cases below ask more often than a person would
	ctx := context.Background()
	db, err := connectDB(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(ctx, `
		TRUNCATE redemptions, promotion_events, promotion_cancelled_orders, promotions;
		DELETE FROM outbox WHERE source = 'promotion-service'`); err != nil {
		t.Fatal(err)
	}
	signer, err := promo.NewSigner("test-promo-secret")
	if err != nil {
		t.Fatal(err)
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	issuer, err := auth.NewIssuer(priv, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	svc := newService(db, signer, jsonEncoder{}, nil)
	f := &fixture{t: t, db: db, svc: svc, signer: signer, issuer: issuer, now: time.Now().UTC().Truncate(time.Second)}
	svc.now = func() time.Time { return f.now }
	f.api = svc.routes(auth.NewVerifier(auth.NewKeySet(pub)))
	return f
}

func newID() string { return uuid.NewString() }

// promotion inserts a promotion that is valid now, edited by fn.
func (f *fixture) promotion(code string, fn func(*Promotion)) {
	f.t.Helper()
	p := Promotion{Code: code, Kind: kindPercent, Value: 10, MaxDiscount: ptr[int64](20000),
		ValidFrom: f.now.Add(-time.Hour), ValidTo: f.now.Add(24 * time.Hour), PerRider: 1}
	if fn != nil {
		fn(&p)
	}
	_, err := f.db.Exec(context.Background(), `
		INSERT INTO promotions (code, kind, value, max_discount, min_fare, vehicle_type, valid_from, valid_to, usage_limit, per_rider_limit, disabled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		p.Code, p.Kind, p.Value, p.MaxDiscount, p.MinFare, p.VehicleType, p.ValidFrom, p.ValidTo, p.UsageLimit, p.PerRider, p.DisabledAt)
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) scalar(sql string, args ...any) int64 {
	f.t.Helper()
	var n int64
	if err := f.db.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

func (f *fixture) outboxRows() int64 {
	return f.scalar(`SELECT count(*) FROM outbox WHERE source = 'promotion-service' AND topic = 'promotion.redeemed'`)
}

func (f *fixture) statusOf(code, orderID string) string {
	f.t.Helper()
	var s string
	if err := f.db.QueryRow(context.Background(), `SELECT status FROM redemptions WHERE code = $1 AND order_id = $2`, code, orderID).Scan(&s); err != nil {
		f.t.Fatalf("redemption %s/%s: %v", code, orderID, err)
	}
	return s
}

func created(orderID, rider, code string, discount int64) events.OrderCreated {
	price := 100000.0
	return events.OrderCreated{
		Envelope: events.Envelope{EventID: newID(), OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)},
		OrderID:  orderID, RiderID: rider, Price: &price, SurgeMultiplier: 1, PromoCode: code, Discount: discount,
	}
}

func cancelled(orderID, rider string) events.OrderCancelled {
	return events.OrderCancelled{
		Envelope: events.Envelope{EventID: newID(), OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)},
		OrderID:  orderID, RiderID: rider, CancelledBy: events.CancelledByRider,
	}
}

func tripCancelled(orderID, rider string) events.TripStatusChanged {
	return events.TripStatusChanged{
		Envelope: events.Envelope{EventID: newID(), OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)},
		TripID:   newID(), OrderID: orderID, DriverID: "driver-1", RiderID: rider,
		Status: events.TripStatusCancelled, CancelledBy: events.CancelledByRider,
	}
}

func noDriver(orderID, rider string) events.OrderNoDriverFound {
	return events.OrderNoDriverFound{
		Envelope: events.Envelope{EventID: newID(), OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)},
		OrderID:  orderID, RiderID: rider,
	}
}

// ------------------------------------------------------------ redemption ----

func TestAnOrderWithACodeIsRedeemedOnceAndAnnounced(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.promotion("WELCOME10", nil)
	order := newID()
	evt := created(order, "rider-1", "WELCOME10", 10000)

	if err := f.svc.onOrderCreated(ctx, evt); err != nil {
		t.Fatal(err)
	}
	if got := f.statusOf("WELCOME10", order); got != "REDEEMED" {
		t.Errorf("status = %s, want REDEEMED", got)
	}
	if n := f.outboxRows(); n != 1 {
		t.Fatalf("%d promotion.redeemed queued, want 1", n)
	}
	var key []byte
	var payload []byte
	f.db.QueryRow(ctx, `SELECT key, payload FROM outbox WHERE source = 'promotion-service'`).Scan(&key, &payload)
	var out events.PromotionRedeemed
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatal(err)
	}
	if string(key) != order || out.OrderID != order || out.RiderID != "rider-1" || out.Code != "WELCOME10" ||
		out.Discount != 10000 || out.Status != events.PromotionStatusRedeemed || out.Currency != "VND" || out.EventID == "" {
		t.Errorf("key %q, event %+v", key, out)
	}
}

func TestOrdersWithoutACodeAreNotRecordedAtAll(t *testing.T) {
	f := newFixture(t)
	if err := f.svc.onOrderCreated(context.Background(), created(newID(), "rider-1", "", 0)); err != nil {
		t.Fatal(err)
	}
	if n := f.scalar(`SELECT count(*) FROM redemptions`) + f.scalar(`SELECT count(*) FROM promotion_events`) + f.outboxRows(); n != 0 {
		t.Errorf("%d rows written for an order with no promotion", n)
	}
}

func TestRedeliveryAndReplayDoNotRedeemTwice(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.promotion("WELCOME10", func(p *Promotion) { p.PerRider = 5 })
	order := newID()
	evt := created(order, "rider-1", "WELCOME10", 10000)

	f.svc.onOrderCreated(ctx, evt) // the event
	f.svc.onOrderCreated(ctx, evt) // redelivered: same eventId
	replay := evt
	replay.EventID = newID() // replayed with a new eventId (an upstream re-publish): (code, order) still holds
	if err := f.svc.onOrderCreated(ctx, replay); err != nil {
		t.Fatal(err)
	}

	if n := f.scalar(`SELECT count(*) FROM redemptions`); n != 1 {
		t.Errorf("%d redemptions, want 1", n)
	}
	if n := f.outboxRows(); n != 1 {
		t.Errorf("%d events queued, want 1", n)
	}
}

func TestConcurrentDeliveriesOfTheSameOrderRedeemOnce(t *testing.T) {
	f := newFixture(t)
	f.promotion("WELCOME10", func(p *Promotion) { p.PerRider = 5 })
	evt := created(newID(), "rider-1", "WELCOME10", 10000)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := f.svc.onOrderCreated(context.Background(), evt); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if n, o := f.scalar(`SELECT count(*) FROM redemptions`), f.outboxRows(); n != 1 || o != 1 {
		t.Errorf("%d redemptions, %d events; want 1 and 1", n, o)
	}
}

func TestThePerRiderLimitIsEnforcedInTheRedemptionTransaction(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.promotion("WELCOME10", nil) // one per rider
	first, second := newID(), newID()
	f.svc.onOrderCreated(ctx, created(first, "rider-1", "WELCOME10", 10000))
	// Booked before the first was recorded, so validation let it through. The order exists.
	if err := f.svc.onOrderCreated(ctx, created(second, "rider-1", "WELCOME10", 10000)); err != nil {
		t.Fatalf("an order past the limit must not fail: %v", err)
	}
	if got := f.statusOf("WELCOME10", first); got != "REDEEMED" {
		t.Errorf("first = %s", got)
	}
	if got := f.statusOf("WELCOME10", second); got != "OVER_LIMIT" {
		t.Errorf("second = %s, want OVER_LIMIT", got)
	}
	var reason string
	f.db.QueryRow(ctx, `SELECT over_limit_reason FROM redemptions WHERE order_id = $1`, second).Scan(&reason)
	if reason != "per_rider_limit" {
		t.Errorf("reason = %q", reason)
	}
	// Another rider is unaffected.
	other := newID()
	f.svc.onOrderCreated(ctx, created(other, "rider-2", "WELCOME10", 10000))
	if got := f.statusOf("WELCOME10", other); got != "REDEEMED" {
		t.Errorf("other rider = %s", got)
	}
	// Both are announced, the overflow with its status.
	if n := f.outboxRows(); n != 3 {
		t.Errorf("%d events queued, want 3", n)
	}
	var over int64
	if err := f.db.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE source = 'promotion-service' AND convert_from(payload, 'UTF8') LIKE '%OVER_LIMIT%'`).Scan(&over); err != nil {
		t.Fatal(err)
	}
	if over != 1 {
		t.Errorf("%d OVER_LIMIT events, want 1", over)
	}
}

func TestTheGlobalLimitIsSoftNotExceededByRedemptionsButRecordedAsOverLimit(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.promotion("FLAT15K", func(p *Promotion) { p.UsageLimit = ptr[int64](3); p.PerRider = 10 })

	// Ten riders' orders were all validated while the code still had room, then arrive together.
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			if err := f.svc.onOrderCreated(ctx, created(newID(), fmt.Sprintf("rider-%d", i), "FLAT15K", 15000)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	redeemed := f.scalar(`SELECT count(*) FROM redemptions WHERE status = 'REDEEMED'`)
	over := f.scalar(`SELECT count(*) FROM redemptions WHERE status = 'OVER_LIMIT' AND over_limit_reason = 'usage_limit'`)
	if redeemed != 3 || over != 7 {
		t.Errorf("redeemed %d, over limit %d; want exactly 3 and 7 (the row lock makes the count exact)", redeemed, over)
	}
}

func TestACancelledOrderGivesTheRedemptionBack(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.promotion("WELCOME10", nil)
	order := newID()
	f.svc.onOrderCreated(ctx, created(order, "rider-1", "WELCOME10", 10000))

	cancel := cancelled(order, "rider-1")
	if err := f.svc.onOrderCancelled(ctx, cancel); err != nil {
		t.Fatal(err)
	}
	f.svc.onOrderCancelled(ctx, cancel) // redelivered
	if got := f.statusOf("WELCOME10", order); got != "RELEASED" {
		t.Fatalf("status = %s, want RELEASED", got)
	}
	// The code can be used again.
	rec := f.validate("rider-1", "WELCOME10", "100000", "CAR")
	if rec.Code != http.StatusOK {
		t.Errorf("after release, validate = %d %s", rec.Code, rec.Body)
	}
	again := newID()
	f.svc.onOrderCreated(ctx, created(again, "rider-1", "WELCOME10", 10000))
	if got := f.statusOf("WELCOME10", again); got != "REDEEMED" {
		t.Errorf("a new order after a release = %s, want REDEEMED", got)
	}
}

func TestACancellationSeenBeforeTheOrderRecordsTheRedemptionAsReleased(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.promotion("WELCOME10", nil)
	order := newID()

	if err := f.svc.onOrderCancelled(ctx, cancelled(order, "rider-1")); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.onOrderCreated(ctx, created(order, "rider-1", "WELCOME10", 10000)); err != nil {
		t.Fatal(err)
	}
	if got := f.statusOf("WELCOME10", order); got != "RELEASED" {
		t.Errorf("status = %s, want RELEASED", got)
	}
	if n := f.outboxRows(); n != 0 {
		t.Errorf("%d promotion.redeemed for an order that never took effect", n)
	}
	// And it does not use up the rider's one use.
	next := newID()
	f.svc.onOrderCreated(ctx, created(next, "rider-1", "WELCOME10", 10000))
	if got := f.statusOf("WELCOME10", next); got != "REDEEMED" {
		t.Errorf("next order = %s, want REDEEMED", got)
	}
}

func TestCancellingAndRedeemingAtTheSameTimeNeverLeavesALiveRedemption(t *testing.T) {
	f := newFixture(t)
	f.promotion("WELCOME10", func(p *Promotion) { p.PerRider = 100 })
	for range 20 {
		order := newID()
		var wg sync.WaitGroup
		wg.Go(func() { f.svc.onOrderCreated(context.Background(), created(order, "rider-1", "WELCOME10", 10000)) })
		wg.Go(func() { f.svc.onOrderCancelled(context.Background(), cancelled(order, "rider-1")) })
		wg.Wait()
		if got := f.statusOf("WELCOME10", order); got != "RELEASED" {
			t.Fatalf("status = %s, want RELEASED whichever event was handled first", got)
		}
	}
}

func TestCancelledOrderWithoutAPromotionLeavesOnlyAMarker(t *testing.T) {
	f := newFixture(t)
	if err := f.svc.onOrderCancelled(context.Background(), cancelled(newID(), "rider-1")); err != nil {
		t.Fatal(err)
	}
	if n := f.scalar(`SELECT count(*) FROM promotion_cancelled_orders`); n != 1 {
		t.Errorf("%d markers, want 1", n)
	}
	if n := f.outboxRows(); n != 0 {
		t.Errorf("%d events queued", n)
	}
}

func TestEventsThatCanNeverBeAppliedArePermanentErrors(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.promotion("WELCOME10", nil)
	bad := map[string]error{}
	bad["unknown code"] = f.svc.onOrderCreated(ctx, created(newID(), "rider-1", "NOSUCHCODE", 1000))
	bad["no order id"] = f.svc.onOrderCreated(ctx, created("", "rider-1", "WELCOME10", 1000))
	bad["no rider"] = f.svc.onOrderCreated(ctx, created(newID(), "", "WELCOME10", 1000))
	bad["negative discount"] = f.svc.onOrderCreated(ctx, created(newID(), "rider-1", "WELCOME10", -1))
	badID := created(newID(), "rider-1", "WELCOME10", 1000)
	badID.EventID = "not-a-uuid"
	bad["bad event id"] = f.svc.onOrderCreated(ctx, badID)
	bad["cancel without order"] = f.svc.onOrderCancelled(ctx, cancelled("", "rider-1"))
	for name, err := range bad {
		var p interface{ Permanent() bool }
		if !errors.As(err, &p) || !p.Permanent() {
			t.Errorf("%s: error = %v, want a permanent one", name, err)
		}
	}
	// None of them left a trace: an unknown code must not be marked as seen, so a redrive after the
	// promotion exists can still apply it.
	if n := f.scalar(`SELECT count(*) FROM redemptions`) + f.scalar(`SELECT count(*) FROM promotion_events`); n != 0 {
		t.Errorf("%d rows left behind by refused events", n)
	}
}

func TestACodeTypedInLowerCaseIsRedeemedAgainstTheRealOne(t *testing.T) {
	f := newFixture(t)
	f.promotion("WELCOME10", nil)
	order := newID()
	if err := f.svc.onOrderCreated(context.Background(), created(order, "rider-1", "welcome10", 10000)); err != nil {
		t.Fatal(err)
	}
	if got := f.statusOf("WELCOME10", order); got != "REDEEMED" {
		t.Errorf("status = %s", got)
	}
}

// -------------------------------------------------------------------- API ----

func (f *fixture) do(method, path, role, subject string, body any) *httptest.ResponseRecorder {
	f.t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rd)
	if role != "" {
		tok, _, err := f.issuer.Issue(auth.Role(role), subject)
		if err != nil {
			f.t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	rec := httptest.NewRecorder()
	f.api.ServeHTTP(rec, req)
	return rec
}

func (f *fixture) validate(rider, code, price, vehicle string) *httptest.ResponseRecorder {
	return f.do(http.MethodGet, "/promotions/validate?code="+code+"&price="+price+"&vehicleType="+vehicle, "rider", rider, nil)
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("body %q: %v", rec.Body, err)
	}
	return m
}

func TestValidateReturnsTheDiscountAndATokenBoundToTheTrip(t *testing.T) {
	f := newFixture(t)
	f.promotion("WELCOME10", func(p *Promotion) { p.MinFare = 40000 })

	rec := f.validate("rider-1", "welcome10", "87000", "CAR") // lower case is fine
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	body := decode(t, rec)
	if body["code"] != "WELCOME10" || body["discount"] != float64(8700) || body["finalPrice"] != float64(78300) {
		t.Errorf("body = %v", body)
	}
	token, _ := body["promoToken"].(string)
	c, err := f.signer.Verify(token, promo.Binding{RiderID: "rider-1", Price: 87000, Vehicle: "CAR"}, f.now)
	if err != nil {
		t.Fatalf("the token does not verify for this trip: %v", err)
	}
	if c.Code != "WELCOME10" || c.Discount != 8700 {
		t.Errorf("claims = %+v", c)
	}
	// Not for another rider.
	if _, err := f.signer.Verify(token, promo.Binding{RiderID: "rider-2", Price: 87000, Vehicle: "CAR"}, f.now); !errors.Is(err, promo.ErrWrongRider) {
		t.Errorf("another rider: %v", err)
	}
	// The cap applies.
	rec = f.validate("rider-1", "WELCOME10", "500000", "CAR")
	if d := decode(t, rec)["discount"]; d != float64(20000) {
		t.Errorf("capped discount = %v, want 20000", d)
	}
}

func TestValidateRefusesWithAMachineReadableCode(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.promotion("MINFARE", func(p *Promotion) { p.MinFare = 50000 })
	f.promotion("CARONLY", func(p *Promotion) { p.VehicleType = ptr("CAR") })
	f.promotion("OLDONE", func(p *Promotion) { p.ValidFrom = f.now.Add(-48 * time.Hour); p.ValidTo = f.now.Add(-time.Hour) })
	f.promotion("SOON", func(p *Promotion) { p.ValidFrom = f.now.Add(time.Hour); p.ValidTo = f.now.Add(48 * time.Hour) })
	f.promotion("OFF", func(p *Promotion) { p.DisabledAt = &f.now })
	f.promotion("FULL", func(p *Promotion) { p.UsageLimit = ptr[int64](1); p.PerRider = 5 })
	f.promotion("ONCE", nil)
	f.svc.onOrderCreated(ctx, created(newID(), "rider-9", "FULL", 1000))
	f.svc.onOrderCreated(ctx, created(newID(), "rider-1", "ONCE", 1000))

	cases := []struct {
		name, rider, code, price, vehicle string
		status                            int
		want                              string
	}{
		{"no such code", "rider-1", "NOPE", "87000", "CAR", 404, "not_found"},
		{"not even a code", "rider-1", "x", "87000", "CAR", 404, "not_found"},
		{"disabled looks absent", "rider-1", "OFF", "87000", "CAR", 404, "not_found"},
		{"expired", "rider-1", "OLDONE", "87000", "CAR", 422, "expired"},
		{"not started", "rider-1", "SOON", "87000", "CAR", 422, "not_started"},
		{"min fare", "rider-1", "MINFARE", "49999", "CAR", 422, "min_fare"},
		{"wrong vehicle", "rider-1", "CARONLY", "87000", "BIKE", 422, "wrong_vehicle"},
		{"limit reached", "rider-1", "FULL", "87000", "CAR", 422, "limit_reached"},
		{"already used", "rider-1", "ONCE", "87000", "CAR", 422, "already_used"},
		{"another rider can still use it", "rider-2", "ONCE", "87000", "CAR", 200, ""},
		{"missing price", "rider-1", "ONCE", "", "CAR", 400, "invalid_request"},
		{"price not a number", "rider-1", "ONCE", "cheap", "CAR", 400, "invalid_request"},
		{"zero price", "rider-1", "ONCE", "0", "CAR", 400, "invalid_request"},
		{"bad vehicle", "rider-1", "ONCE", "87000", "HELICOPTER", 400, "invalid_request"},
	}
	for _, tc := range cases {
		rec := f.validate(tc.rider, tc.code, tc.price, tc.vehicle)
		if rec.Code != tc.status {
			t.Errorf("%s: status %d, want %d (%s)", tc.name, rec.Code, tc.status, rec.Body)
			continue
		}
		body := decode(t, rec)
		if tc.want != "" && body["code"] != tc.want {
			t.Errorf("%s: code %v, want %s", tc.name, body["code"], tc.want)
		}
		if tc.want != "" && (body["error"] == "" || body["promoToken"] != nil) {
			t.Errorf("%s: body %v", tc.name, body)
		}
	}
}

func TestValidateIsForRidersAndNeedsAToken(t *testing.T) {
	f := newFixture(t)
	f.promotion("WELCOME10", nil)
	if rec := f.do(http.MethodGet, "/promotions/validate?code=WELCOME10&price=87000", "", "", nil); rec.Code != 401 {
		t.Errorf("no token: %d", rec.Code)
	}
	for _, role := range []string{"driver", "admin"} {
		if rec := f.do(http.MethodGet, "/promotions/validate?code=WELCOME10&price=87000", role, "x", nil); rec.Code != 403 {
			t.Errorf("%s: %d, want 403", role, rec.Code)
		}
	}
}

func TestPromotionsAreManagedByAdminsOnly(t *testing.T) {
	f := newFixture(t)
	f.promotion("WELCOME10", nil)
	body := map[string]any{"code": "NEW10", "kind": "PERCENT", "value": 10, "validTo": f.now.Add(time.Hour)}
	routes := []struct {
		method, path string
		body         any
	}{
		{http.MethodPost, "/promotions", body},
		{http.MethodGet, "/promotions", nil},
		{http.MethodPost, "/promotions/WELCOME10/disable", nil},
	}
	for _, rt := range routes {
		if rec := f.do(rt.method, rt.path, "", "", rt.body); rec.Code != 401 {
			t.Errorf("%s %s without a token: %d", rt.method, rt.path, rec.Code)
		}
		for _, role := range []string{"rider", "driver"} {
			if rec := f.do(rt.method, rt.path, role, "someone", rt.body); rec.Code != 403 {
				t.Errorf("%s %s as %s: %d, want 403", rt.method, rt.path, role, rec.Code)
			}
		}
	}
	if n := f.scalar(`SELECT count(*) FROM promotions WHERE disabled_at IS NOT NULL OR code = 'NEW10'`); n != 0 {
		t.Errorf("a refused request changed %d promotions", n)
	}
}

func TestAdminCreatesListsAndDisablesAPromotion(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	rec := f.do(http.MethodPost, "/promotions", "admin", "admin-1", map[string]any{
		"code": " summer20 ", "kind": "percent", "value": 20, "maxDiscount": 30000, "minFare": 50000,
		"vehicleType": "CAR", "validTo": f.now.Add(72 * time.Hour), "usageLimit": 100, "perRiderLimit": 2,
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	created := decode(t, rec)
	if created["code"] != "SUMMER20" || created["kind"] != "PERCENT" || created["status"] != "active" ||
		created["maxDiscount"] != float64(30000) || created["usageLimit"] != float64(100) || created["perRiderLimit"] != float64(2) ||
		created["createdBy"] != "admin-1" || created["redeemed"] != float64(0) {
		t.Errorf("created = %v", created)
	}

	if rec := f.do(http.MethodPost, "/promotions", "admin", "admin-1", map[string]any{
		"code": "SUMMER20", "kind": "FIXED", "value": 5000, "validTo": f.now.Add(time.Hour),
	}); rec.Code != http.StatusConflict || decode(t, rec)["code"] != "code_exists" {
		t.Errorf("duplicate: %d %s", rec.Code, rec.Body)
	}

	// It works for a rider at once, and shows in the list with its use.
	if rec := f.validate("rider-1", "SUMMER20", "100000", "CAR"); rec.Code != 200 {
		t.Fatalf("validate: %d %s", rec.Code, rec.Body)
	}
	f.svc.onOrderCreated(ctx, created2("rider-1", "SUMMER20"))
	list := decode(t, f.do(http.MethodGet, "/promotions", "admin", "admin-1", nil))["promotions"].([]any)
	if len(list) != 1 || list[0].(map[string]any)["redeemed"] != float64(1) {
		t.Errorf("list = %v", list)
	}

	// Disabling stops new validations, keeps the history, and is repeatable.
	for range 2 {
		rec = f.do(http.MethodPost, "/promotions/summer20/disable", "admin", "admin-1", nil)
		if rec.Code != 200 || decode(t, rec)["status"] != "disabled" {
			t.Fatalf("disable: %d %s", rec.Code, rec.Body)
		}
	}
	if rec := f.validate("rider-2", "SUMMER20", "100000", "CAR"); rec.Code != 404 {
		t.Errorf("validate after disable: %d, want 404", rec.Code)
	}
	if n := f.scalar(`SELECT count(*) FROM redemptions`); n != 1 {
		t.Errorf("disabling changed the redemptions (%d)", n)
	}
	if rec := f.do(http.MethodPost, "/promotions/NOPE/disable", "admin", "admin-1", nil); rec.Code != 404 {
		t.Errorf("disable unknown: %d", rec.Code)
	}
}

func created2(rider, code string) events.OrderCreated { return created(newID(), rider, code, 10000) }

func TestCreateRefusesPromotionsThatCannotWork(t *testing.T) {
	f := newFixture(t)
	valid := func() map[string]any {
		return map[string]any{"code": "OKAY10", "kind": "PERCENT", "value": 10, "validTo": f.now.Add(time.Hour)}
	}
	cases := map[string]func(m map[string]any){
		"short code":            func(m map[string]any) { m["code"] = "ab" },
		"code with a space":     func(m map[string]any) { m["code"] = "TWO WORDS" },
		"unknown kind":          func(m map[string]any) { m["kind"] = "BOGO" },
		"zero value":            func(m map[string]any) { m["value"] = 0 },
		"percent above 100":     func(m map[string]any) { m["value"] = 101 },
		"cap on a fixed code":   func(m map[string]any) { m["kind"] = "FIXED"; m["maxDiscount"] = 5000 },
		"zero cap":              func(m map[string]any) { m["maxDiscount"] = 0 },
		"negative min fare":     func(m map[string]any) { m["minFare"] = -1 },
		"bad vehicle":           func(m map[string]any) { m["vehicleType"] = "TRUCK" },
		"no end date":           func(m map[string]any) { delete(m, "validTo") },
		"ends before it starts": func(m map[string]any) { m["validFrom"] = f.now.Add(2 * time.Hour) },
		"already over": func(m map[string]any) {
			m["validFrom"] = f.now.Add(-2 * time.Hour)
			m["validTo"] = f.now.Add(-time.Hour)
		},
		"zero usage limit":      func(m map[string]any) { m["usageLimit"] = 0 },
		"zero per-rider limit":  func(m map[string]any) { m["perRiderLimit"] = 0 },
		"value above the limit": func(m map[string]any) { m["kind"] = "FIXED"; m["value"] = maxAmount + 1 },
	}
	for name, mutate := range cases {
		m := valid()
		mutate(m)
		rec := f.do(http.MethodPost, "/promotions", "admin", "admin-1", m)
		if rec.Code != http.StatusBadRequest || decode(t, rec)["code"] != "invalid_promotion" {
			t.Errorf("%s: %d %s, want 400 invalid_promotion", name, rec.Code, rec.Body)
		}
	}
	if n := f.scalar(`SELECT count(*) FROM promotions`); n != 0 {
		t.Errorf("%d promotions saved from refused requests", n)
	}
	if rec := f.do(http.MethodPost, "/promotions", "admin", "admin-1", valid()); rec.Code != http.StatusCreated {
		t.Errorf("the valid baseline was refused: %d %s", rec.Code, rec.Body)
	}
}

func TestSeedDevAddsTheDemoCodesOnce(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	n, err := seedDev(ctx, f.db)
	if err != nil || n != 3 {
		t.Fatalf("first run: %d, %v", n, err)
	}
	if n, err := seedDev(ctx, f.db); err != nil || n != 0 {
		t.Fatalf("second run: %d, %v; want nothing added", n, err)
	}
	f.now = time.Now().UTC()
	// WELCOME10: 10% capped at 20,000; FLAT15K: 15,000 off a fare of 40,000 or more.
	if d := decode(t, f.validate("rider-1", "WELCOME10", "300000", "CAR"))["discount"]; d != float64(20000) {
		t.Errorf("WELCOME10 on 300,000 = %v", d)
	}
	if rec := f.validate("rider-1", "FLAT15K", "39000", "CAR"); decode(t, rec)["code"] != "min_fare" {
		t.Errorf("FLAT15K on 39,000: %s", rec.Body)
	}
	if d := decode(t, f.validate("rider-1", "FLAT15K", "87000", "BIKE"))["discount"]; d != float64(15000) {
		t.Errorf("FLAT15K on 87,000 = %v", d)
	}
}

// ----------------------------------------------- orders that end without a ride ----

// Each way an order ends without a ride releases its redemption, with no
// promotion.redeemed for the release; the code can be used again afterwards.
func TestEveryEndWithoutARideReleasesTheRedemption(t *testing.T) {
	ends := map[string]func(f *fixture, order, rider string) error{
		"order.cancelled": func(f *fixture, order, rider string) error {
			return f.svc.onOrderCancelled(context.Background(), cancelled(order, rider))
		},
		"trip cancelled by the rider": func(f *fixture, order, rider string) error {
			return f.svc.onTripStatusChanged(context.Background(), tripCancelled(order, rider))
		},
		"trip cancelled by the driver": func(f *fixture, order, rider string) error {
			evt := tripCancelled(order, rider)
			evt.CancelledBy = events.CancelledByDriver
			return f.svc.onTripStatusChanged(context.Background(), evt)
		},
		"order.no_driver_found": func(f *fixture, order, rider string) error {
			return f.svc.onNoDriverFound(context.Background(), noDriver(order, rider))
		},
	}
	for name, end := range ends {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			f.promotion("WELCOME10", nil) // once per rider
			order := newID()
			f.svc.onOrderCreated(ctx, created(order, "rider-1", "WELCOME10", 10000))
			if got := f.statusOf("WELCOME10", order); got != "REDEEMED" {
				t.Fatalf("before: %s", got)
			}
			if rec := f.validate("rider-1", "WELCOME10", "100000", "CAR"); rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("a used-up code validated with %d %s", rec.Code, rec.Body)
			}
			before := f.outboxRows()

			if err := end(f, order, "rider-1"); err != nil {
				t.Fatal(err)
			}
			if got := f.statusOf("WELCOME10", order); got != "RELEASED" {
				t.Fatalf("status = %s, want RELEASED", got)
			}
			if n := f.scalar(`SELECT count(*) FROM redemptions WHERE order_id = $1 AND released_at IS NOT NULL`, order); n != 1 {
				t.Errorf("released_at not set")
			}
			if n := f.outboxRows(); n != before {
				t.Errorf("a release queued %d promotion.redeemed", n-before)
			}
			if rec := f.validate("rider-1", "WELCOME10", "100000", "CAR"); rec.Code != http.StatusOK {
				t.Errorf("after release, validate = %d %s", rec.Code, rec.Body)
			}
		})
	}
}

func TestAReleaseIsDeduplicatedByEventIDAndAlsoWhenAnotherEventNamesTheSameOrder(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.promotion("WELCOME10", func(p *Promotion) { p.PerRider = 5 })
	order := newID()
	f.svc.onOrderCreated(ctx, created(order, "rider-1", "WELCOME10", 10000))

	trip := tripCancelled(order, "rider-1")
	if err := f.svc.onTripStatusChanged(ctx, trip); err != nil {
		t.Fatal(err)
	}
	var releasedAt time.Time
	f.db.QueryRow(ctx, `SELECT released_at FROM redemptions WHERE order_id = $1`, order).Scan(&releasedAt)
	dedupeRows := f.scalar(`SELECT count(*) FROM promotion_events`)

	// The same event again, then a different event for the same order (a rider
	// cancels just as a driver is matched: order.cancelled and the trip's CANCELLED).
	f.now = f.now.Add(time.Hour)
	for _, err := range []error{
		f.svc.onTripStatusChanged(ctx, trip),
		f.svc.onOrderCancelled(ctx, cancelled(order, "rider-1")),
		f.svc.onNoDriverFound(ctx, noDriver(order, "rider-1")),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := f.statusOf("WELCOME10", order); got != "RELEASED" {
		t.Errorf("status = %s", got)
	}
	var again time.Time
	f.db.QueryRow(ctx, `SELECT released_at FROM redemptions WHERE order_id = $1`, order).Scan(&again)
	if !again.Equal(releasedAt) {
		t.Errorf("released_at moved from %s to %s", releasedAt, again)
	}
	if n := f.scalar(`SELECT count(*) FROM promotion_events`); n != dedupeRows+2 {
		t.Errorf("%d dedupe rows, want %d (the redelivery adds none, the two new events one each)", n, dedupeRows+2)
	}
	if n := f.scalar(`SELECT count(*) FROM redemptions WHERE order_id = $1`, order); n != 1 {
		t.Errorf("%d redemptions", n)
	}
}

func TestOtherTripStatusesLeaveTheRedemptionAlone(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.promotion("WELCOME10", nil)
	order := newID()
	f.svc.onOrderCreated(ctx, created(order, "rider-1", "WELCOME10", 10000))

	for _, status := range []events.TripStatus{events.TripStatusAssigned, events.TripStatusInProgress, events.TripStatusCompleted} {
		evt := tripCancelled(order, "rider-1")
		evt.Status, evt.CancelledBy = status, ""
		if err := f.svc.onTripStatusChanged(ctx, evt); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.statusOf("WELCOME10", order); got != "REDEEMED" {
		t.Errorf("status = %s, want REDEEMED", got)
	}
	if n := f.scalar(`SELECT count(*) FROM promotion_cancelled_orders`); n != 0 {
		t.Errorf("%d cancelled-order markers for trips that were not cancelled", n)
	}
}

// An over-limit redemption is released too, so it stops counting.
func TestAnOverLimitRedemptionIsReleasedByACancelledTrip(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.promotion("WELCOME10", func(p *Promotion) { p.PerRider = 1 })
	first, second := newID(), newID()
	f.svc.onOrderCreated(ctx, created(first, "rider-1", "WELCOME10", 10000))
	f.svc.onOrderCreated(ctx, created(second, "rider-1", "WELCOME10", 10000))
	if got := f.statusOf("WELCOME10", second); got != "OVER_LIMIT" {
		t.Fatalf("second = %s", got)
	}
	if err := f.svc.onTripStatusChanged(ctx, tripCancelled(second, "rider-1")); err != nil {
		t.Fatal(err)
	}
	if got := f.statusOf("WELCOME10", second); got != "RELEASED" {
		t.Errorf("second = %s, want RELEASED", got)
	}
	if got := f.statusOf("WELCOME10", first); got != "REDEEMED" {
		t.Errorf("the other order's redemption = %s, want REDEEMED", got)
	}
}

// The release seen before order.created leaves the marker, and the redemption
// is then recorded as released, for the trip and no-driver events like for
// order.cancelled.
func TestATripCancellationOrNoDriverSeenBeforeTheOrderRecordsTheRedemptionAsReleased(t *testing.T) {
	for name, end := range map[string]func(f *fixture, order string) error{
		"trip cancelled": func(f *fixture, order string) error {
			return f.svc.onTripStatusChanged(context.Background(), tripCancelled(order, "rider-1"))
		},
		"no driver": func(f *fixture, order string) error {
			return f.svc.onNoDriverFound(context.Background(), noDriver(order, "rider-1"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.promotion("WELCOME10", nil)
			order := newID()
			if err := end(f, order); err != nil {
				t.Fatal(err)
			}
			if err := f.svc.onOrderCreated(context.Background(), created(order, "rider-1", "WELCOME10", 10000)); err != nil {
				t.Fatal(err)
			}
			if got := f.statusOf("WELCOME10", order); got != "RELEASED" {
				t.Errorf("status = %s, want RELEASED", got)
			}
			if n := f.outboxRows(); n != 0 {
				t.Errorf("%d promotion.redeemed for an order that never took effect", n)
			}
			next := newID()
			f.svc.onOrderCreated(context.Background(), created(next, "rider-1", "WELCOME10", 10000))
			if got := f.statusOf("WELCOME10", next); got != "REDEEMED" {
				t.Errorf("next order = %s, want REDEEMED", got)
			}
		})
	}
}

func TestReleasingAndRedeemingAtTheSameTimeNeverLeavesALiveRedemptionForTheNewEvents(t *testing.T) {
	f := newFixture(t)
	f.promotion("WELCOME10", func(p *Promotion) { p.PerRider = 100 })
	for i := range 20 {
		order := newID()
		var wg sync.WaitGroup
		wg.Go(func() { f.svc.onOrderCreated(context.Background(), created(order, "rider-1", "WELCOME10", 10000)) })
		wg.Go(func() {
			if i%2 == 0 {
				f.svc.onTripStatusChanged(context.Background(), tripCancelled(order, "rider-1"))
			} else {
				f.svc.onNoDriverFound(context.Background(), noDriver(order, "rider-1"))
			}
		})
		wg.Wait()
		if got := f.statusOf("WELCOME10", order); got != "RELEASED" {
			t.Fatalf("status = %s, want RELEASED whichever event was handled first", got)
		}
	}
}

func TestReleaseEventsWithoutAnOrderIdArePermanentErrors(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for name, err := range map[string]error{
		"trip":      f.svc.onTripStatusChanged(ctx, tripCancelled("", "rider-1")),
		"no driver": f.svc.onNoDriverFound(ctx, noDriver("", "rider-1")),
	} {
		var inv *invalidEvent
		if !errors.As(err, &inv) {
			t.Errorf("%s: err = %v, want an invalidEvent", name, err)
		}
	}
}
