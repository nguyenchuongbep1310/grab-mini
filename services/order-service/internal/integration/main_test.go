// Package integration tests order-service end to end inside the process: the
// real HTTP API and Kafka handler over the use cases and the real Postgres and
// Redis adapters. Each test gets a database of its own (testdb_test.go); set
// TEST_DATABASE_URL to run them.
package integration

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"grab-mini/order-service/internal/adapters/postgres"
	"grab-mini/order-service/internal/app"
	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
	"grab-mini/pkg/surge"
)

// outboxSource names this service in the shared outbox table.
const outboxSource = "order-service"

var t0 = time.Unix(1_800_000_000, 0)

func tripRequest() domain.BookingRequest {
	return domain.BookingRequest{
		Pickup: &domain.LatLng{Lat: 10.776, Lng: 106.72}, Dropoff: &domain.LatLng{Lat: 10.82, Lng: 106.75},
		VehicleType: domain.VehicleCar,
	}
}

func quoteFor(p *app.Pricer, rider string, req domain.BookingRequest, mult float64, at time.Time) surge.Quote {
	return p.Signer.Issue(surge.QuoteRequest{
		RiderID: rider, Vehicle: string(req.VehicleType),
		PickupLat: req.Pickup.Lat, PickupLng: req.Pickup.Lng, DropoffLat: req.Dropoff.Lat, DropoffLng: req.Dropoff.Lng,
	}, mult, at)
}

func straightBase(req domain.BookingRequest) float64 {
	return surge.BaseFare(string(req.VehicleType), surge.HaversineKm(req.Pickup.Lat, req.Pickup.Lng, req.Dropoff.Lat, req.Dropoff.Lng))
}

type recordingWriter struct{ sent []kafka.Message }

func (w *recordingWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.sent = append(w.sent, msgs...)
	return nil
}

// newOrder is an unpriced CAR order for rider from (1,2) to (3,4).
func newOrder(rider string) app.NewOrder {
	return app.NewOrder{
		RiderID: rider, Pickup: domain.LatLng{Lat: 1, Lng: 2}, Dropoff: domain.LatLng{Lat: 3, Lng: 4},
		VehicleType: domain.VehicleCar,
	}
}

// insertOrder saves an order and its order.created event in one transaction,
// through the Postgres unit of work, and returns the order's id.
func insertOrder(ctx context.Context, uow *postgres.UnitOfWork, o app.NewOrder, payload []byte) (string, error) {
	if o.ID == "" {
		o.ID = uuid.NewString()
	}
	return o.ID, uow.Do(ctx, func(tx app.Tx) error {
		if err := tx.InsertOrder(ctx, o); err != nil {
			return err
		}
		return tx.QueueOrderCreated(ctx, o.ID, payload)
	})
}

func TestAnOrderAndItsEventAreSavedTogether(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	w := &recordingWriter{}
	relay := outbox.NewRelay(db, w, outboxSource)

	id, err := insertOrder(ctx, postgres.NewUnitOfWork(db, outboxSource), newOrder("rider-1"), []byte("avro-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	if o, err := postgres.NewStore(db).ByID(ctx, id); err != nil || o.Status != "PENDING" {
		t.Fatalf("order = %v, %v; want a PENDING order", o, err)
	}

	if n, err := relay.Flush(ctx); err != nil || n != 1 {
		t.Fatalf("Flush = %d, %v; want the order.created event", n, err)
	}
	m := w.sent[0]
	if m.Topic != events.TopicOrderCreated || string(m.Key) != id || string(m.Value) != "avro-bytes" {
		t.Errorf("published topic=%s key=%s value=%s", m.Topic, m.Key, m.Value)
	}
}

func TestNoOrderIsLeftBehindWhenTheEventCannotBeQueued(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	// Break the outbox so the second statement of the transaction fails.
	if _, err := db.Exec(ctx, `ALTER TABLE outbox RENAME TO outbox_off`); err != nil {
		t.Fatal(err)
	}
	defer db.Exec(ctx, `ALTER TABLE outbox_off RENAME TO outbox`)

	o := newOrder("rider-1")
	o.ID = uuid.NewString()
	if _, err := insertOrder(ctx, postgres.NewUnitOfWork(db, outboxSource), o, []byte("x")); err == nil {
		t.Fatal("insertOrder succeeded without being able to queue the event")
	}
	if _, err := postgres.NewStore(db).ByID(ctx, o.ID); err == nil {
		t.Error("the order row survived although its event could not be queued")
	}
}

// The API as a client sees it: tokens, roles and ownership, over the real mux
// and a real database.
func TestOrderAccessIsLimitedToTheOwnerAndDrivers(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	id, err := insertOrder(ctx, a.uow, newOrder("rider-1"), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}

	do := func(method, path string, role auth.Role, subject string) int {
		req := httptest.NewRequest(method, path, strings.NewReader(`{}`))
		if role != "" {
			tok, _, _ := a.issuer.Issue(role, subject)
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		rec := httptest.NewRecorder()
		a.mux.ServeHTTP(rec, req)
		return rec.Code
	}

	cases := []struct {
		name     string
		method   string
		path     string
		role     auth.Role
		subject  string
		wantCode int
	}{
		{"no token", "GET", "/orders/" + id, "", "", 401},
		{"the owner reads their order", "GET", "/orders/" + id, auth.Rider, "rider-1", 200},
		{"another rider can't see it (it doesn't exist for them)", "GET", "/orders/" + id, auth.Rider, "rider-2", 404},
		{"a driver can't read an order that isn't theirs", "GET", "/orders/" + id, auth.Driver, "driver-1", 404},
		{"the owner's latest order", "GET", "/riders/rider-1/latest-order", auth.Rider, "rider-1", 200},
		{"someone else's latest order", "GET", "/riders/rider-1/latest-order", auth.Rider, "rider-2", 403},
		{"a driver has no latest order", "GET", "/riders/rider-1/latest-order", auth.Driver, "rider-1", 403},
		{"a driver can't book", "POST", "/orders", auth.Driver, "driver-1", 403},
		{"booking without a token", "POST", "/orders", "", "", 401},
	}
	for _, c := range cases {
		if got := do(c.method, c.path, c.role, c.subject); got != c.wantCode {
			t.Errorf("%s: status %d, want %d", c.name, got, c.wantCode)
		}
	}

	// Once matched, that driver (and only that driver) can read it.
	if err := a.deliver(ctx, a.matched(t, id, "driver-1")); err != nil {
		t.Fatal(err)
	}
	if got := do("GET", "/orders/"+id, auth.Driver, "driver-1"); got != 200 {
		t.Errorf("the matched driver: status %d, want 200", got)
	}
	if got := do("GET", "/orders/"+id, auth.Driver, "driver-2"); got != 404 {
		t.Errorf("another driver after the match: status %d, want 404", got)
	}
}
