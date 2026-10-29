package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/auth/authtest"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/outbox"
)

// ---- unit: key validation and fingerprinting (no database) ----

func TestIdempotencyKeyValidation(t *testing.T) {
	ok := []string{"a", "550e8400-e29b-41d4-a716-446655440000", strings.Repeat("k", 128), "with space and ~!@#"}
	for _, k := range ok {
		h := http.Header{"Idempotency-Key": {k}}
		if got, err := idempotencyKey(h); err != nil || got != k {
			t.Errorf("%q: got %q, %v; want accepted", k, got, err)
		}
	}
	if got, err := idempotencyKey(http.Header{}); got != "" || err != nil {
		t.Errorf("absent header = %q, %v; want empty and no error", got, err)
	}
	bad := map[string][]string{
		"empty":         {""},
		"too long":      {strings.Repeat("k", 129)},
		"newline":       {"a\nb"},
		"tab":           {"a\tb"},
		"DEL":           {"a\x7fb"},
		"non-ASCII":     {"clé"},
		"given twice":   {"a", "b"},
		"only too long": {strings.Repeat("é", 70)},
	}
	for name, v := range bad {
		if _, err := idempotencyKey(http.Header{"Idempotency-Key": v}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestFingerprintIgnoresFormattingButNotMeaning(t *testing.T) {
	base := tripRequest()
	same := tripRequest()
	same.RiderID = "someone" // the rider is scoped by the key, not part of the body
	if fingerprint(base) != fingerprint(same) {
		t.Error("the rider changed the fingerprint")
	}
	// The same JSON written differently, with the default vehicle spelled out or not.
	a := `{"pickup":{"lat":10.776,"lng":106.72},"dropoff":{"lat":10.82,"lng":106.75}}`
	b := `{ "dropoff": {"lng":106.75, "lat":10.82}, "vehicleType":"CAR", "pickup":{"lat":1.0776e1,"lng":106.72} }`
	fa, fb := fingerprintOfJSON(t, a), fingerprintOfJSON(t, b)
	if fa != fb {
		t.Errorf("equivalent bodies fingerprint differently:\n%s\n%s", fa, fb)
	}

	for name, mutate := range map[string]func(*createOrderRequest){
		"pickup":   func(r *createOrderRequest) { r.Pickup.Lat += 0.0001 },
		"dropoff":  func(r *createOrderRequest) { r.Dropoff.Lng += 0.0001 },
		"vehicle":  func(r *createOrderRequest) { r.VehicleType = "BIKE" },
		"quote id": func(r *createOrderRequest) { r.QuoteID = "q" },
	} {
		other := tripRequest()
		mutate(&other)
		if fingerprint(other) == fingerprint(base) {
			t.Errorf("a different %s kept the fingerprint", name)
		}
	}
}

func fingerprintOfJSON(t *testing.T, body string) string {
	t.Helper()
	var req createOrderRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	if req.VehicleType == "" {
		req.VehicleType = "CAR"
	}
	return fingerprint(req)
}

func TestABadKeyIs400BeforeAnythingElse(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(
		`{"pickup":{"lat":10.776,"lng":106.7},"dropoff":{"lat":10.79,"lng":106.71}}`))
	req.Header.Set("Idempotency-Key", strings.Repeat("k", 200))
	req = req.WithContext(auth.With(req.Context(), auth.Identity{Subject: "r", Role: auth.Rider}))
	(&server{}).handleCreateOrder(rec, req) // no database: it must not get that far
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "Idempotency-Key") {
		t.Errorf("status %d body %q, want a 400 naming the header", rec.Code, rec.Body.String())
	}
}

// ---- unit: single-use quotes (miniredis) ----

func claimingPricer(t *testing.T, current float64) (*pricer, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	p := testPricer(t, current)
	p.claims = redisClaims{redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	return p, mr
}

func TestAQuoteBooksOnceAndThenGetsAFreshQuote(t *testing.T) {
	p, mr := claimingPricer(t, 1.5)
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.2, t0.Add(-10*time.Second))
	ctx := context.Background()

	first, err := p.Price(ctx, "rider-1", req, q.QuoteID)
	if err != nil || first.Multiplier != 1.2 || first.ClaimedQuote != q.QuoteID {
		t.Fatalf("first booking = %+v, %v; want the quoted 1.2x", first, err)
	}
	if ttl := mr.TTL(claimKey(q.QuoteID)); ttl <= 0 || ttl > 60*time.Second {
		t.Errorf("the mark lives %v, want the quote's remaining life (about 50s)", ttl)
	}

	_, err = p.Price(ctx, "rider-1", req, q.QuoteID)
	var again *conflict
	if !errors.As(err, &again) {
		t.Fatalf("second booking err = %v, want a *conflict", err)
	}
	if again.NewQuote.Multiplier != 1.5 || again.NewQuote.QuoteID == q.QuoteID || again.Error() != reasonQuoteUsed {
		t.Errorf("conflict = %+v (%q), want a fresh quote at the current 1.5x and the reuse reason", again.NewQuote, again.Error())
	}
}

func TestAQuoteGivenBackCanBookAgain(t *testing.T) {
	p, _ := claimingPricer(t, 1.0)
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.2, t0.Add(-10*time.Second))
	ctx := context.Background()

	settled, err := p.Price(ctx, "rider-1", req, q.QuoteID)
	if err != nil {
		t.Fatal(err)
	}
	p.release(ctx, settled) // the booking failed after pricing
	if _, err := p.Price(ctx, "rider-1", req, q.QuoteID); err != nil {
		t.Errorf("retry after a failed booking = %v, want the quote honoured again", err)
	}
}

// Fail-open policy: with Redis down a quote can't be checked, so the booking
// is not rejected; it is priced at the current multiplier, never at the old quote.
func TestWithRedisDownABookingIsRepricedNotRejected(t *testing.T) {
	p, mr := claimingPricer(t, 1.5)
	req := tripRequest()
	q := quoteFor(p, "rider-1", req, 1.2, t0.Add(-10*time.Second))
	mr.Close()

	got, err := p.Price(context.Background(), "rider-1", req, q.QuoteID)
	if err != nil {
		t.Fatalf("err = %v, want the booking to go ahead", err)
	}
	if got.ClaimedQuote != "" {
		t.Error("claimed a quote it could not mark")
	}
	// currentMultiplier here is the test's fixed 1.5, standing for "what Redis last said".
	if got.Multiplier != 1.5 {
		t.Errorf("multiplier = %v, want the current 1.5 and not the quoted 1.2", got.Multiplier)
	}
}

// ---- integration: the API over a real database ----

func fakeRegistryCodec(t *testing.T) *eventcodec.Codec {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int{"id": 1})
	}))
	t.Cleanup(srv.Close)
	return eventcodec.New(srv.URL)
}

type api struct {
	t      *testing.T
	db     *pgxpool.Pool
	pricer *pricer
	mux    *http.ServeMux
	issuer *auth.Issuer
}

func newAPI(t *testing.T) *api {
	t.Helper()
	db := testDB(t)
	p, _ := claimingPricer(t, 1.0)
	s := &server{db: db, relay: outbox.NewRelay(db, &recordingWriter{}, outboxSource), codec: fakeRegistryCodec(t), pricer: p}
	issuer, verifier := authtest.Pair(t, time.Hour)
	return &api{t: t, db: db, pricer: p, mux: s.routes(verifier), issuer: issuer}
}

const bookingBody = `{"pickup":{"lat":10.776,"lng":106.72},"dropoff":{"lat":10.82,"lng":106.75}}`

type reply struct {
	Code    int
	Body    map[string]any
	Replay  bool
	Raw     string
	OrderID string
}

func (a *api) book(rider, key, body string) reply {
	a.t.Helper()
	tok, _, _ := a.issuer.Issue(auth.Rider, rider)
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	a.mux.ServeHTTP(rec, req)
	r := reply{Code: rec.Code, Raw: rec.Body.String(), Replay: rec.Header().Get("Idempotent-Replayed") == "true"}
	json.Unmarshal(rec.Body.Bytes(), &r.Body)
	r.OrderID, _ = r.Body["orderId"].(string)
	return r
}

func (a *api) count(table string) int {
	a.t.Helper()
	var n int
	if err := a.db.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		a.t.Fatal(err)
	}
	return n
}

func TestARetriedBookingReturnsTheSameOrderAndPublishesOnce(t *testing.T) {
	a := newAPI(t)
	first := a.book("rider-1", "key-1", bookingBody)
	if first.Code != http.StatusCreated || first.OrderID == "" || first.Replay {
		t.Fatalf("first = %+v, want a fresh 201", first)
	}
	// Same request, different formatting: still the same booking.
	retry := a.book("rider-1", "key-1", `{ "vehicleType":"CAR", "dropoff":{"lat":10.82,"lng":106.75}, "pickup":{"lat":10.776,"lng":106.72} }`)
	if retry.Code != http.StatusCreated || retry.OrderID != first.OrderID || !retry.Replay {
		t.Fatalf("retry = %+v, want the original 201 and order %s", retry, first.OrderID)
	}
	if retry.Raw != first.Raw {
		t.Errorf("the replayed body differs:\n%s\n%s", first.Raw, retry.Raw)
	}
	if n := a.count("orders"); n != 1 {
		t.Errorf("%d orders, want 1", n)
	}
	if n := a.count("outbox"); n != 1 {
		t.Errorf("%d outbox rows, want exactly 1 order.created", n)
	}
	if n := a.count("idempotency_keys"); n != 1 {
		t.Errorf("%d key rows, want 1", n)
	}
}

func TestTheSameKeyWithAnotherBodyIs422(t *testing.T) {
	a := newAPI(t)
	first := a.book("rider-1", "key-1", bookingBody)
	other := a.book("rider-1", "key-1", strings.Replace(bookingBody, "10.82", "10.83", 1))
	if other.Code != http.StatusUnprocessableEntity {
		t.Fatalf("different body = %d %s, want 422", other.Code, other.Raw)
	}
	if other.OrderID != "" || a.count("orders") != 1 || a.count("outbox") != 1 {
		t.Errorf("the mismatch created something: %d orders, %d outbox rows", a.count("orders"), a.count("outbox"))
	}
	if again := a.book("rider-1", "key-1", bookingBody); again.OrderID != first.OrderID {
		t.Error("the original body no longer replays after a mismatch")
	}
}

func TestKeysBelongToTheRider(t *testing.T) {
	a := newAPI(t)
	r1 := a.book("rider-1", "shared-key", bookingBody)
	r2 := a.book("rider-2", "shared-key", bookingBody)
	if r1.Code != 201 || r2.Code != 201 || r1.OrderID == r2.OrderID || r2.Replay {
		t.Errorf("riders sharing a key string: %+v and %+v, want two separate orders", r1, r2)
	}
}

func TestWithoutAKeyEveryRequestIsABooking(t *testing.T) {
	a := newAPI(t)
	a.book("rider-1", "", bookingBody)
	a.book("rider-1", "", bookingBody)
	if n := a.count("orders"); n != 2 {
		t.Errorf("%d orders, want 2 (no key, no deduplication)", n)
	}
}

func TestConcurrentDuplicatesCreateOneOrder(t *testing.T) {
	// One rider fires these requests back to back on purpose; the per-caller rate limit
	// (tested on its own) would answer 429 and hide what this test is about.
	t.Setenv(auth.RateLimitScaleEnv, "0")
	a := newAPI(t)
	const n = 12
	replies := make([]reply, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range replies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			replies[i] = a.book("rider-1", "race", bookingBody)
		}()
	}
	close(start)
	wg.Wait()

	for i, r := range replies {
		if r.Code != http.StatusCreated || r.OrderID != replies[0].OrderID {
			t.Errorf("request %d = %d %s, want the one order %s", i, r.Code, r.Raw, replies[0].OrderID)
		}
	}
	if a.count("orders") != 1 || a.count("outbox") != 1 || a.count("idempotency_keys") != 1 {
		t.Errorf("%d orders, %d outbox rows, %d keys; want 1 each", a.count("orders"), a.count("outbox"), a.count("idempotency_keys"))
	}
}

// The primary key is the backstop when the lock is bypassed: the loser of the
// race finds the winner's row.
func TestTheUniqueKeyStopsASecondOrderEvenWithoutTheLock(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	s := &server{db: a.db, relay: outbox.NewRelay(a.db, &recordingWriter{}, outboxSource)}
	req := tripRequest()
	req.RiderID = "rider-1"
	rec := idemRecord{Key: "k", Fingerprint: fingerprint(req), Status: 201, Body: []byte("{}")}

	insert := func() error {
		tx, _ := a.db.Begin(ctx)
		defer tx.Rollback(ctx)
		id := uuid.NewString()
		r := rec
		r.OrderID = id
		if err := s.insertOrderTx(ctx, tx, id, req, []byte("x"), &r); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if err := insert(); err != nil {
		t.Fatal(err)
	}
	err := insert()
	if !isDuplicateKey(err) {
		t.Fatalf("second insert err = %v, want the idempotency_keys primary key violation", err)
	}
	if a.count("orders") != 1 || a.count("outbox") != 1 {
		t.Errorf("%d orders, %d outbox rows after the rejected insert, want 1 each", a.count("orders"), a.count("outbox"))
	}
}

func TestAKeyExpiresAfter24Hours(t *testing.T) {
	a := newAPI(t)
	first := a.book("rider-1", "old", bookingBody)
	a.db.Exec(context.Background(), `UPDATE idempotency_keys SET created_at = now() - interval '25 hours'`)

	again := a.book("rider-1", "old", bookingBody)
	if again.Code != 201 || again.Replay || again.OrderID == first.OrderID {
		t.Errorf("after 25h = %+v, want a new order", again)
	}
	if a.count("orders") != 2 || a.count("idempotency_keys") != 1 {
		t.Errorf("%d orders, %d keys; want 2 orders and the key re-recorded once", a.count("orders"), a.count("idempotency_keys"))
	}

	a.db.Exec(context.Background(), `UPDATE idempotency_keys SET created_at = now() - interval '25 hours'`)
	n, err := purgeExpiredKeys(context.Background(), a.db)
	if err != nil || n != 1 || a.count("idempotency_keys") != 0 {
		t.Errorf("purge = %d, %v; %d keys left; want it to remove the expired one", n, err, a.count("idempotency_keys"))
	}
}

// ---- integration: quote replay ----

func (a *api) quoteBody(rider string, mult float64) (string, string) {
	q := quoteFor(a.pricer, rider, tripRequest(), mult, t0.Add(-10*time.Second))
	return q.QuoteID, strings.TrimSuffix(bookingBody, "}") + `,"quoteId":"` + q.QuoteID + `"}`
}

func TestAReusedQuoteIs409WithAFreshQuote(t *testing.T) {
	a := newAPI(t)
	_, body := a.quoteBody("rider-1", 1.2)

	if first := a.book("rider-1", "", body); first.Code != http.StatusCreated || first.Body["surgeMultiplier"] != 1.2 {
		t.Fatalf("first booking = %d %s, want 201 at the quoted 1.2x", first.Code, first.Raw)
	}
	for _, key := range []string{"", "another-key"} {
		second := a.book("rider-1", key, body)
		if second.Code != http.StatusConflict {
			t.Fatalf("reused quote (key %q) = %d %s, want 409", key, second.Code, second.Raw)
		}
		nq, _ := second.Body["newQuote"].(map[string]any)
		if nq == nil || nq["quoteId"] == "" || !strings.Contains(second.Raw, "already used") {
			t.Errorf("409 body = %s, want a fresh newQuote and the reason", second.Raw)
		}
	}
	if n := a.count("orders"); n != 1 {
		t.Errorf("%d orders, want 1: the reused quote must not book", n)
	}
}

func TestAnIdempotentRetryWithTheSameQuoteIsNotRejected(t *testing.T) {
	a := newAPI(t)
	_, body := a.quoteBody("rider-1", 1.2)

	first := a.book("rider-1", "k", body)
	retry := a.book("rider-1", "k", body)
	if first.Code != 201 || retry.Code != 201 || retry.OrderID != first.OrderID || !retry.Replay {
		t.Fatalf("first %+v, retry %+v; want the stored response, not a 409", first, retry)
	}
}

func TestAQuoteSurvivesAFailedBookingForTheRetry(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	_, body := a.quoteBody("rider-1", 1.2)

	a.db.Exec(ctx, `ALTER TABLE outbox RENAME TO outbox_off`)
	failed := a.book("rider-1", "k", body)
	a.db.Exec(ctx, `ALTER TABLE outbox_off RENAME TO outbox`)
	if failed.Code != http.StatusInternalServerError {
		t.Fatalf("booking with a broken outbox = %d %s, want 500", failed.Code, failed.Raw)
	}
	retry := a.book("rider-1", "k", body)
	if retry.Code != http.StatusCreated || retry.Body["surgeMultiplier"] != 1.2 {
		t.Errorf("retry = %d %s, want 201 with the quote still honoured", retry.Code, retry.Raw)
	}
}

// Against a real Redis, when TEST_REDIS_ADDR is set: SET NX with a TTL.
func TestQuoteMarksUseSetNXWithATTL(t *testing.T) {
	addr := ""
	if addr = getEnv("TEST_REDIS_ADDR", ""); addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	c := redisClaims{rdb}
	id := "test-quote-" + uuid.NewString()
	t.Cleanup(func() { rdb.Del(ctx, claimKey(id)) })

	if first, err := c.Claim(ctx, id, 30*time.Second); err != nil || !first {
		t.Fatalf("first claim = %v, %v", first, err)
	}
	if first, err := c.Claim(ctx, id, 30*time.Second); err != nil || first {
		t.Fatalf("second claim = %v, %v; want it refused", first, err)
	}
	if ttl := rdb.TTL(ctx, claimKey(id)).Val(); ttl <= 0 || ttl > 30*time.Second {
		t.Errorf("ttl = %v, want at most 30s", ttl)
	}
	c.Release(ctx, id)
	if first, _ := c.Claim(ctx, id, 30*time.Second); !first {
		t.Error("claim after release refused")
	}
}
