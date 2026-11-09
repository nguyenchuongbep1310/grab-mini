package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"grab-mini/order-service/internal/domain"
)

func TestAnOrderAndItsEventAreSavedTogether(t *testing.T) {
	e := newTestEnv(t)
	req := tripRequest()

	res, err := e.book("rider-1", "", req)
	if err != nil || res.Status != 201 || res.Replayed {
		t.Fatalf("CreateOrder = %+v, %v; want a new 201", res, err)
	}
	orders := e.db.allOrders()
	if len(orders) != 1 {
		t.Fatalf("saved %d orders, want 1", len(orders))
	}
	o := orders[0]
	if o.RiderID != "rider-1" || o.Status != domain.StatusPending || o.VehicleType != domain.VehicleCar ||
		o.Pickup != *req.Pickup || o.Dropoff != *req.Dropoff || o.SurgeMultiplier != 1.2 || o.Price == nil {
		t.Errorf("saved order %+v", o)
	}

	q := e.db.queued()
	if len(q) != 1 || q[0].topic != "order.created" || q[0].orderID != o.ID {
		t.Fatalf("queued %+v, want one order.created keyed by the order id", q)
	}
	ev := e.enc.created[0]
	if ev.OrderID != o.ID || ev.RiderID != "rider-1" || ev.Pickup != *req.Pickup || ev.Dropoff != *req.Dropoff ||
		ev.VehicleType != domain.VehicleCar || ev.SurgeMultiplier != 1.2 || ev.Price == nil || *ev.Price != *o.Price {
		t.Errorf("event %+v does not describe order %+v", ev, o)
	}
	if want, _ := json.Marshal(ev); !bytes.Equal(q[0].payload, want) {
		t.Errorf("queued payload %s, want the encoded event %s", q[0].payload, want)
	}

	want := testReceipt(Receipt{OrderID: o.ID, SurgeMultiplier: 1.2, Price: *o.Price, FinalPrice: *o.Price})
	if !bytes.Equal(res.Body, want) {
		t.Errorf("body %s, want the receipt %s", res.Body, want)
	}
	if len(e.claims.released) != 0 {
		t.Errorf("released %v after a 201; a saved booking keeps its claims", e.claims.released)
	}
	if e.relay.count() != 1 || e.metrics.created["CAR"] != 1 || e.metrics.idem["new"] != 0 {
		t.Errorf("wakes=%d created=%v idem=%v; want one wake, one CAR order and no key counted",
			e.relay.count(), e.metrics.created, e.metrics.idem)
	}
}

func TestNoOrderIsLeftBehindWhenTheEventCannotBeQueued(t *testing.T) {
	e := newTestEnv(t)
	e.db.failQueue = errors.New("outbox is gone")

	if _, err := e.book("rider-1", "", tripRequest()); !errors.Is(err, ErrInternal) {
		t.Fatalf("err = %v, want ErrInternal", err)
	}
	if n := len(e.db.allOrders()); n != 0 {
		t.Errorf("%d order(s) survived although the event could not be queued", n)
	}
	if len(e.db.queued()) != 0 || e.relay.count() != 0 || len(e.metrics.created) != 0 {
		t.Errorf("queued=%v wakes=%d created=%v; want nothing", e.db.queued(), e.relay.count(), e.metrics.created)
	}
}

func TestARetriedBookingReturnsTheSameOrderAndPublishesOnce(t *testing.T) {
	e := newTestEnv(t)
	req := tripRequest()

	first, err := e.book("rider-1", "k1", req)
	if err != nil || first.Status != 201 || first.Replayed {
		t.Fatalf("first = %+v, %v", first, err)
	}
	again, err := e.book("rider-1", "k1", req)
	if err != nil || !again.Replayed || again.Status != first.Status || !bytes.Equal(again.Body, first.Body) {
		t.Fatalf("retry = %+v, %v; want the first response replayed", again, err)
	}
	if len(e.db.allOrders()) != 1 || len(e.db.queued()) != 1 || e.relay.count() != 1 {
		t.Errorf("orders=%d queued=%d wakes=%d; want one of each", len(e.db.allOrders()), len(e.db.queued()), e.relay.count())
	}
	if e.metrics.idem["new"] != 1 || e.metrics.idem["replayed"] != 1 || e.metrics.created["CAR"] != 1 {
		t.Errorf("idem=%v created=%v", e.metrics.idem, e.metrics.created)
	}
	rec := e.db.lookup("rider-1", "k1")
	if rec == nil || rec.Status != 201 || !bytes.Equal(rec.Body, first.Body) || rec.Fingerprint != req.Fingerprint() || rec.OrderID != e.db.allOrders()[0].ID {
		t.Errorf("stored key %+v", rec)
	}
}

func TestTheSameKeyWithAnotherBodyIs422(t *testing.T) {
	e := newTestEnv(t)
	if _, err := e.book("rider-1", "k1", tripRequest()); err != nil {
		t.Fatal(err)
	}
	other := tripRequest()
	other.Dropoff = &domain.LatLng{Lat: 10.9, Lng: 106.8}
	if _, err := e.book("rider-1", "k1", other); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("err = %v, want ErrKeyMismatch", err)
	}
	if len(e.db.allOrders()) != 1 || e.metrics.idem["mismatch"] != 1 {
		t.Errorf("orders=%d idem=%v", len(e.db.allOrders()), e.metrics.idem)
	}
}

func TestAKeyBelongsToItsRider(t *testing.T) {
	e := newTestEnv(t)
	if _, err := e.book("rider-1", "k1", tripRequest()); err != nil {
		t.Fatal(err)
	}
	res, err := e.book("rider-2", "k1", tripRequest())
	if err != nil || res.Replayed || res.Status != 201 {
		t.Fatalf("another rider's booking = %+v, %v; want a new order", res, err)
	}
	if len(e.db.allOrders()) != 2 || e.metrics.idem["new"] != 2 {
		t.Errorf("orders=%d idem=%v", len(e.db.allOrders()), e.metrics.idem)
	}
}

func TestWithoutAKeyEveryRequestIsABooking(t *testing.T) {
	e := newTestEnv(t)
	for i := 0; i < 2; i++ {
		if res, err := e.book("rider-1", "", tripRequest()); err != nil || res.Replayed {
			t.Fatalf("booking #%d = %+v, %v", i+1, res, err)
		}
	}
	orders := e.db.allOrders()
	if len(orders) != 2 || orders[0].ID == orders[1].ID || len(e.db.queued()) != 2 {
		t.Errorf("orders=%+v queued=%d; want two distinct orders and events", orders, len(e.db.queued()))
	}
	if len(e.metrics.idem) != 0 {
		t.Errorf("idem=%v; nothing to count without a key", e.metrics.idem)
	}
}

func TestAReplayIsAnsweredBeforeTheQuoteIsLookedAt(t *testing.T) {
	e := newTestEnv(t)
	req := tripRequest()
	req.QuoteID = quoteFor(e.pricer, "rider-1", req, 1.4, t0.Add(-10*time.Second)).QuoteID

	first, err := e.book("rider-1", "k1", req)
	if err != nil {
		t.Fatal(err)
	}
	e.rec.reset()
	// The retry carries the quote the first attempt used up: it must not be
	// told the quote was already used.
	again, err := e.book("rider-1", "k1", req)
	if err != nil || !again.Replayed || !bytes.Equal(again.Body, first.Body) {
		t.Fatalf("retry = %+v, %v; want a replay", again, err)
	}
	if got, want := e.rec.list(), []string{"begin", "LockKey", "Lookup", "rollback"}; !slices.Equal(got, want) {
		t.Errorf("calls %v, want %v (nothing priced)", got, want)
	}
	if e.metrics.pricing["quote"] != 1 || e.metrics.pricing["quote_reused"] != 0 {
		t.Errorf("pricing=%v; the replay must not touch the quote", e.metrics.pricing)
	}
}

func TestADuplicateKeyAtSaveFallsBackToReplayingTheWinner(t *testing.T) {
	e := newTestEnv(t)
	req, quoteID, promoID := quotedPromoRequest(e)
	winner := domain.KeyRecord{Key: "k1", Fingerprint: req.Fingerprint(), OrderID: "winner-order", Status: 201, Body: []byte("winner\n")}
	e.db.onSaveKey = func() error {
		e.db.putKey("rider-1", winner) // a request that raced past the lock committed first
		return fmt.Errorf("insert key: %w", ErrDuplicateKey)
	}

	res, err := e.book("rider-1", "k1", req)
	if err != nil || !res.Replayed || res.Status != 201 || !bytes.Equal(res.Body, winner.Body) {
		t.Fatalf("CreateOrder = %+v, %v; want the winner replayed", res, err)
	}
	if calls := e.rec.list(); !slices.Equal(calls[len(calls)-2:], []string{"rollback", "keys.Lookup"}) {
		t.Errorf("calls %v, want the rollback then a lookup outside the transaction", calls)
	}
	if len(e.db.allOrders()) != 0 || e.relay.count() != 0 || len(e.metrics.created) != 0 {
		t.Errorf("orders=%d wakes=%d created=%v; the loser saves and announces nothing", len(e.db.allOrders()), e.relay.count(), e.metrics.created)
	}
	if e.metrics.idem["replayed"] != 1 || e.metrics.idem["new"] != 0 {
		t.Errorf("idem=%v", e.metrics.idem)
	}
	// The loser's order was never saved: both claims go back.
	if e.claims.used[quoteID] || e.claims.used[promoID] ||
		!slices.Contains(e.claims.released, quoteID) || !slices.Contains(e.claims.released, promoID) {
		t.Errorf("used=%v released=%v; want the quote and the promo token given back", e.claims.used, e.claims.released)
	}

	// With no winner to be found it is an internal error.
	lost := newTestEnv(t)
	lost.db.onSaveKey = func() error { return ErrDuplicateKey }
	if _, err := lost.book("rider-1", "k1", req); !errors.Is(err, ErrInternal) {
		t.Errorf("err = %v, want ErrInternal when the winner cannot be found", err)
	}
}

func TestEveryFailureAfterTheClaimsGivesBothBack(t *testing.T) {
	isInternal := func(err error) bool { return errors.Is(err, ErrInternal) }
	cases := []struct {
		name     string
		key      string
		badPromo bool
		breakIt  func(*testEnv)
		wantErr  func(error) bool
		// Whether the promo token was claimed (and so must be given back).
		promoClaimed bool
	}{
		{"promo refused", "", true, func(*testEnv) {}, func(err error) bool { var pe *PromoError; return errors.As(err, &pe) }, false},
		{"encoder fails", "", false, func(e *testEnv) { e.enc.failCreated = errors.New("registry down") }, isInternal, true},
		{"InsertOrder fails", "", false, func(e *testEnv) { e.db.failInsert = errors.New("db down") }, isInternal, true},
		{"InsertOrder fails with a key", "k1", false, func(e *testEnv) { e.db.failInsert = errors.New("db down") }, isInternal, true},
		{"SaveKey fails", "k1", false, func(e *testEnv) { e.db.failSaveKey = errors.New("disk full") }, isInternal, true},
		{"commit fails", "k1", false, func(e *testEnv) { e.db.failCommit = errors.New("connection lost") }, isInternal, true},
		{"SaveKey loses to a duplicate", "k1", false, func(e *testEnv) {
			e.db.onSaveKey = func() error {
				e.db.putKey("rider-1", domain.KeyRecord{Key: "k1", Fingerprint: "another body", Status: 201})
				return ErrDuplicateKey
			}
		}, func(err error) bool { return errors.Is(err, ErrKeyMismatch) }, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := newTestEnv(t)
			req := tripRequest()
			q := quoteFor(e.pricer, "rider-1", req, 1.4, t0.Add(-10*time.Second))
			price := roundVND(q.Price)
			if c.badPromo {
				price++
			}
			tok := issue(e.promos, "rider-1", price, "CAR", 5000, t0)
			req.QuoteID, req.PromoToken = q.QuoteID, tok
			c.breakIt(e)

			if _, err := e.book("rider-1", c.key, req); !c.wantErr(err) {
				t.Fatalf("err = %v", err)
			}
			if e.claims.used[q.QuoteID] || !slices.Contains(e.claims.released, q.QuoteID) {
				t.Errorf("the quote was not given back (released %v)", e.claims.released)
			}
			if e.claims.used["promo:"+tok] {
				t.Error("the promo token is still claimed")
			}
			if got := slices.Contains(e.claims.released, "promo:"+tok); got != c.promoClaimed {
				t.Errorf("promo released = %v, want %v", got, c.promoClaimed)
			}
			if len(e.db.allOrders()) != 0 || e.relay.count() != 0 {
				t.Errorf("orders=%d wakes=%d after a failure", len(e.db.allOrders()), e.relay.count())
			}
		})
	}
}

func TestWithAKeyThePricingRunsInsideTheTransaction(t *testing.T) {
	e := newTestEnv(t)
	if _, err := e.book("rider-1", "k1", tripRequest()); err != nil {
		t.Fatal(err)
	}
	want := []string{"begin", "LockKey", "Lookup", "multiplier", "encode order.created",
		"InsertOrder", "QueueOrderCreated", "SaveKey", "commit", "wake"}
	if got := e.rec.list(); !slices.Equal(got, want) {
		t.Errorf("calls\n got %v\nwant %v", got, want)
	}
}

func TestWithoutAKeyThePricingRunsBeforeTheTransactionOpens(t *testing.T) {
	e := newTestEnv(t)
	if _, err := e.book("rider-1", "", tripRequest()); err != nil {
		t.Fatal(err)
	}
	want := []string{"multiplier", "encode order.created", "begin", "InsertOrder", "QueueOrderCreated", "commit", "wake"}
	if got := e.rec.list(); !slices.Equal(got, want) {
		t.Errorf("calls\n got %v\nwant %v", got, want)
	}
}

func TestAPromoBookingCarriesItsDiscountToTheOrderTheEventAndTheReceipt(t *testing.T) {
	e := newTestEnv(t)
	req, quoteID, promoID := quotedPromoRequest(e)
	q := quoteFor(e.pricer, "rider-1", tripRequest(), 1.4, t0.Add(-10*time.Second))

	res, err := e.book("rider-1", "k1", req)
	if err != nil {
		t.Fatal(err)
	}
	// A saved booking keeps both claims: the quote and the token cannot be spent again.
	if len(e.claims.released) != 0 || !e.claims.used[quoteID] || !e.claims.used[promoID] {
		t.Errorf("released=%v used=%v after a 201; want both claims kept", e.claims.released, e.claims.used)
	}
	o := e.db.allOrders()[0]
	ev := e.enc.created[0]
	if o.PromoCode != "WELCOME10" || o.Discount != 5000 || ev.PromoCode != "WELCOME10" || ev.Discount != 5000 {
		t.Errorf("order %+v event %+v; want WELCOME10 and 5000 off", o, ev)
	}
	want := testReceipt(Receipt{OrderID: o.ID, SurgeMultiplier: 1.4, Price: q.Price, FinalPrice: q.Price - 5000, Discount: 5000, PromoCode: "WELCOME10"})
	if !bytes.Equal(res.Body, want) {
		t.Errorf("body %s, want %s", res.Body, want)
	}
}

// quotedPromoRequest is a booking by rider-1 with a valid quote (1.4) and a
// promo token (5000 off) bound to its price, and the claim ids of both.
func quotedPromoRequest(e *testEnv) (req domain.BookingRequest, quoteID, promoID string) {
	req = tripRequest()
	q := quoteFor(e.pricer, "rider-1", req, 1.4, t0.Add(-10*time.Second))
	req.QuoteID, req.PromoToken = q.QuoteID, issue(e.promos, "rider-1", roundVND(q.Price), "CAR", 5000, t0)
	return req, q.QuoteID, "promo:" + req.PromoToken
}

// A failed keyed booking gives its claims back while the transaction, and with
// it the key's lock, is still open: after the rollback a waiting retry with the
// same key could claim the quote first and be refused as reused.
func TestWithAKeyTheClaimsAreReleasedBeforeTheRollback(t *testing.T) {
	for name, breakIt := range map[string]func(*testEnv){
		"InsertOrder fails": func(e *testEnv) { e.db.failInsert = errors.New("db down") },
		"encoder fails":     func(e *testEnv) { e.enc.failCreated = errors.New("registry down") },
	} {
		t.Run(name, func(t *testing.T) {
			e := newTestEnv(t)
			req := tripRequest()
			q := quoteFor(e.pricer, "rider-1", req, 1.4, t0.Add(-10*time.Second))
			req.QuoteID = q.QuoteID
			breakIt(e)

			if _, err := e.book("rider-1", "k1", req); err == nil {
				t.Fatal("want an error")
			}
			calls := e.rec.list()
			rel, rb := slices.Index(calls, "release"), slices.Index(calls, "rollback")
			if rel < 0 || rb < 0 || rel > rb {
				t.Errorf("release must come before rollback, calls = %v", calls)
			}
			if n := slices.Index(e.claims.released, q.QuoteID); n < 0 || len(e.claims.released) != 1 {
				t.Errorf("the quote must be released exactly once, released = %v", e.claims.released)
			}
		})
	}
}
