package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	httpapi "grab-mini/order-service/internal/adapters/http"
	kafkaadapter "grab-mini/order-service/internal/adapters/kafka"
	ordermetrics "grab-mini/order-service/internal/adapters/metrics"
	"grab-mini/order-service/internal/adapters/postgres"
	redisadapter "grab-mini/order-service/internal/adapters/redis"
	"grab-mini/order-service/internal/app"
	"grab-mini/pkg/auth"
	"grab-mini/pkg/auth/authtest"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
	"grab-mini/pkg/promo"
	"grab-mini/pkg/surge"
)

// ---- the API over a real database ----

// fakeRegistryCodec is a codec over an in-process Schema Registry that keeps
// the schemas it is given, so a test can decode what the service queued.
func fakeRegistryCodec(t *testing.T) *eventcodec.Codec {
	t.Helper()
	var mu sync.Mutex
	var schemas []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPost:
			var body struct{ Schema string }
			json.NewDecoder(r.Body).Decode(&body)
			for i, s := range schemas {
				if s == body.Schema {
					json.NewEncoder(w).Encode(map[string]int{"id": i + 1})
					return
				}
			}
			schemas = append(schemas, body.Schema)
			json.NewEncoder(w).Encode(map[string]int{"id": len(schemas)})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/schemas/ids/"):
			id, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/schemas/ids/"))
			if id < 1 || id > len(schemas) {
				http.NotFound(w, r)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"schema": schemas[id-1]})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return eventcodec.New(srv.URL)
}

// api is the service wired as main wires it, over a private database and an
// in-process Redis, with a fixed clock for quotes and promo tokens.
type api struct {
	t       *testing.T
	db      *pgxpool.Pool
	store   *postgres.Store
	uow     *postgres.UnitOfWork
	svc     *app.Service
	pricer  *app.Pricer
	promos  *app.PromoChecker
	codec   *eventcodec.Codec
	mux     *http.ServeMux
	issuer  *auth.Issuer
	handler *kafkaadapter.Handler
}

func newAPI(t *testing.T) *api {
	t.Helper()
	db := testDB(t)
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	m := ordermetrics.New()
	claims := redisadapter.NewClaims(rdb)
	signer, err := surge.NewSigner("test-secret")
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return t0 }
	p := &app.Pricer{Signer: signer, Mult: redisadapter.NewMultipliers(rdb, m), Claims: claims,
		Tolerance: 0.2, CellDeg: 0.05, Now: clock, Metrics: m}
	promoSigner, err := promo.NewSigner("test-promo-secret")
	if err != nil {
		t.Fatal(err)
	}
	pc := &app.PromoChecker{Signer: promoSigner, Claims: claims, Now: clock, Metrics: m}
	codec := fakeRegistryCodec(t)
	store := postgres.NewStore(db)
	uow := postgres.NewUnitOfWork(db, outboxSource)
	svc := app.New(app.Deps{
		Orders: store, Keys: store, Outcomes: store, UoW: uow,
		Encoder: kafkaadapter.NewEncoder(codec), Receipt: httpapi.ReceiptEncoder(),
		Pricer: p, Promos: pc, Relay: outbox.NewRelay(db, &recordingWriter{}, outboxSource), Metrics: m,
	})
	issuer, verifier := authtest.Pair(t, time.Hour)
	return &api{t: t, db: db, store: store, uow: uow, svc: svc, pricer: p, promos: pc, codec: codec,
		mux: httpapi.NewAPI(svc, verifier).Routes(), issuer: issuer, handler: kafkaadapter.NewHandler(svc, codec)}
}

// message encodes evt as the producer on topic would.
func (a *api) message(t *testing.T, topic string, evt any) kafka.Message {
	t.Helper()
	payload, err := a.codec.Encode(context.Background(), topic, evt)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: topic, Value: payload}
}

// matched is order.matched for the order, to the driver.
func (a *api) matched(t *testing.T, orderID, driverID string) kafka.Message {
	return a.message(t, events.TopicOrderMatched, events.OrderMatched{OrderID: orderID, DriverID: driverID, VehicleType: events.VehicleCar})
}

// outcome is what matching reports for the order: MATCHED (with no driver
// named) or NO_DRIVER.
func (a *api) outcome(t *testing.T, orderID, status string) kafka.Message {
	t.Helper()
	switch status {
	case "MATCHED":
		return a.matched(t, orderID, "")
	case "NO_DRIVER":
		return a.message(t, events.TopicOrderNoDriverFound, events.OrderNoDriverFound{OrderID: orderID})
	}
	t.Fatalf("no outcome %q", status)
	return kafka.Message{}
}

// tripCancelled is trip.status.changed CANCELLED for the order's trip.
func (a *api) tripCancelled(t *testing.T, orderID, by, reason string) kafka.Message {
	return a.message(t, events.TopicTripStatusChanged, events.TripStatusChanged{
		TripID: "trip-1", OrderID: orderID, Status: events.TripStatusCancelled, CancelledBy: by, Reason: reason,
	})
}

// deliver hands one consumed message to the Kafka handler.
func (a *api) deliver(ctx context.Context, msg kafka.Message) error {
	return a.handler.Handle(ctx, msg)
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
	n, err := a.svc.PurgeExpiredKeys(context.Background())
	if err != nil || n != 1 || a.count("idempotency_keys") != 0 {
		t.Errorf("purge = %d, %v; %d keys left; want it to remove the expired one", n, err, a.count("idempotency_keys"))
	}
}

// ---- quote replay ----

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
