package postgres

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/order-service/internal/app"
	"grab-mini/order-service/internal/domain"
)

var (
	_ app.OrderReader = (*Store)(nil)
	_ app.KeyStore    = (*Store)(nil)
	_ app.Outcomes    = (*Store)(nil)
	_ app.UnitOfWork  = (*UnitOfWork)(nil)
	_ app.Tx          = (*tx)(nil)
)

// migrations is the service's own migration directory, read from disk: the
// embed lives in the flat main package, which this one must not import.
func migrations() fs.FS { return os.DirFS("../../..") }

func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	db, err := Connect(context.Background(), url, migrations())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(context.Background(), `DELETE FROM outbox; DELETE FROM idempotency_keys; DELETE FROM orders`); err != nil {
		t.Fatal(err)
	}
	return db
}

func count(t *testing.T, db *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func newOrder(rider string) app.NewOrder {
	return app.NewOrder{
		ID: uuid.NewString(), RiderID: rider,
		Pickup: domain.LatLng{Lat: 1, Lng: 2}, Dropoff: domain.LatLng{Lat: 3, Lng: 4},
		VehicleType: domain.VehicleCar,
	}
}

func insert(t *testing.T, u *UnitOfWork, o app.NewOrder) {
	t.Helper()
	err := u.Do(context.Background(), func(tx app.Tx) error {
		if err := tx.InsertOrder(context.Background(), o); err != nil {
			return err
		}
		return tx.QueueOrderCreated(context.Background(), o.ID, []byte("payload"))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestConnectAndMigrateAreIdempotent(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	if err := Migrate(context.Background(), url, migrations()); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(context.Background(), "postgres://nobody@localhost:1/x?sslmode=disable&connect_timeout=1", migrations()); err == nil {
		t.Error("Migrate against a dead database must fail")
	}
}

func TestAnOrderAndItsOutboxRowCommitTogether(t *testing.T) {
	db := testDB(t)
	u := NewUnitOfWork(db, "order-service")
	o := newOrder("rider-1")
	insert(t, u, o)
	if count(t, db, "orders") != 1 || count(t, db, "outbox") != 1 {
		t.Fatalf("%d orders, %d outbox rows, want 1 each", count(t, db, "orders"), count(t, db, "outbox"))
	}
	var topic, source, key string
	db.QueryRow(context.Background(), `SELECT topic, source, convert_from(key, 'UTF8') FROM outbox`).Scan(&topic, &source, &key)
	if topic != "order.created" || source != "order-service" || key != o.ID {
		t.Errorf("outbox row = %q %q %q", topic, source, key)
	}

	// An error rolls everything back and comes back unchanged.
	boom := errors.New("boom")
	o2 := newOrder("rider-1")
	err := u.Do(context.Background(), func(tx app.Tx) error {
		if err := tx.InsertOrder(context.Background(), o2); err != nil {
			return err
		}
		if err := tx.QueueOrderCreated(context.Background(), o2.ID, []byte("x")); err != nil {
			return err
		}
		return boom
	})
	if err != boom {
		t.Errorf("Do returned %v (%T), want fn's error itself", err, err)
	}
	if count(t, db, "orders") != 1 || count(t, db, "outbox") != 1 {
		t.Errorf("after the rolled back Do: %d orders, %d outbox rows, want 1 each", count(t, db, "orders"), count(t, db, "outbox"))
	}
}

func TestPriceAndMultiplierArePersistedAndOldOrdersReadAsUnpriced(t *testing.T) {
	db := testDB(t)
	u := NewUnitOfWork(db, "order-service")
	s := NewStore(db)
	ctx := context.Background()

	price := 50000.0
	priced := newOrder("rider-1")
	priced.Multiplier, priced.Price, priced.PromoCode, priced.Discount = 1.5, &price, "SAVE", 10000
	insert(t, u, priced)
	got, err := s.ByID(ctx, priced.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.SurgeMultiplier != 1.5 || got.Price == nil || *got.Price != 50000 || got.PromoCode != "SAVE" || got.Discount != 10000 {
		t.Errorf("priced order = %+v", got)
	}
	if f := got.FinalPrice(); f == nil || *f != 40000 {
		t.Errorf("final price = %v, want 40000", f)
	}
	if got.Status != domain.StatusPending || got.Pickup.Lat != 1 || got.Dropoff.Lng != 4 || got.VehicleType != domain.VehicleCar {
		t.Errorf("order = %+v", got)
	}

	// A zero multiplier is stored as 1.0.
	plain := newOrder("rider-1")
	insert(t, u, plain)
	if got, _ := s.ByID(ctx, plain.ID); got.SurgeMultiplier != 1.0 {
		t.Errorf("zero multiplier stored as %v, want 1.0", got.SurgeMultiplier)
	}

	// An order booked before surge pricing has no price.
	old := uuid.NewString()
	if _, err := db.Exec(ctx, `INSERT INTO orders (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, vehicle_type, status) VALUES ($1,'rider-9',1,2,3,4,'CAR','PENDING')`, old); err != nil {
		t.Fatal(err)
	}
	o, err := s.ByID(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	if o.Price != nil || o.FinalPrice() != nil || o.SurgeMultiplier != 1.0 {
		t.Errorf("old order price = %v, final = %v, multiplier = %v; want unpriced", o.Price, o.FinalPrice(), o.SurgeMultiplier)
	}

	if _, err := s.ByID(ctx, uuid.NewString()); !errors.Is(err, domain.ErrOrderNotFound) {
		t.Errorf("unknown id err = %v, want ErrOrderNotFound", err)
	}
	if _, err := s.LatestByRider(ctx, "nobody"); !errors.Is(err, domain.ErrOrderNotFound) {
		t.Errorf("unknown rider err = %v, want ErrOrderNotFound", err)
	}
	latest, err := s.LatestByRider(ctx, "rider-1")
	if err != nil || latest.ID != plain.ID {
		t.Errorf("latest = %v, %v; want the most recently created order", latest.ID, err)
	}
}

func TestTheGuardedUpdatesNeverOverwriteACancelledOrder(t *testing.T) {
	db := testDB(t)
	u := NewUnitOfWork(db, "order-service")
	s := NewStore(db)
	ctx := context.Background()

	o := newOrder("rider-1")
	insert(t, u, o)
	var cancelled bool
	if err := u.Do(ctx, func(tx app.Tx) error {
		var err error
		cancelled, err = tx.CancelPending(ctx, o.ID, "changed my mind")
		return err
	}); err != nil || !cancelled {
		t.Fatalf("CancelPending = %v, %v", cancelled, err)
	}
	// A MATCHED outcome after CANCELLED changes nothing.
	if changed, err := s.Set(ctx, o.ID, domain.StatusMatched, "driver-1"); err != nil || changed {
		t.Errorf("Set after cancel = %v, %v; want unchanged", changed, err)
	}
	if changed, err := s.CancelMatched(ctx, o.ID, "DRIVER", "x"); err != nil || changed {
		t.Errorf("CancelMatched on CANCELLED = %v, %v; want unchanged", changed, err)
	}
	got, _ := s.ByID(ctx, o.ID)
	if got.Status != domain.StatusCancelled || got.DriverID != "" || got.CancelledBy != "RIDER" || got.CancelReason != "changed my mind" || got.CancelledAt == nil {
		t.Errorf("order = %+v, want CANCELLED by RIDER with no driver", got)
	}

	// CancelMatched only moves a MATCHED order, once.
	m := newOrder("rider-1")
	insert(t, u, m)
	if changed, _ := s.CancelMatched(ctx, m.ID, "DRIVER", "x"); changed {
		t.Error("CancelMatched moved a PENDING order")
	}
	if changed, err := s.Set(ctx, m.ID, domain.StatusMatched, "driver-7"); err != nil || !changed {
		t.Fatalf("Set MATCHED = %v, %v", changed, err)
	}
	if changed, _ := s.Set(ctx, m.ID, domain.StatusNoDriver, ""); changed {
		t.Error("NO_DRIVER overwrote a MATCHED order")
	}
	if changed, err := s.CancelMatched(ctx, m.ID, "DRIVER", "flat tyre"); err != nil || !changed {
		t.Fatalf("CancelMatched = %v, %v", changed, err)
	}
	if changed, _ := s.CancelMatched(ctx, m.ID, "DRIVER", "again"); changed {
		t.Error("a redelivered cancel changed the order again")
	}
	got, _ = s.ByID(ctx, m.ID)
	if got.Status != domain.StatusCancelled || got.CancelledBy != "DRIVER" || got.CancelReason != "flat tyre" || got.DriverID != "driver-7" {
		t.Errorf("order = %+v", got)
	}
}

func TestCancelPendingReportsFalseWhenTheOrderIsNoLongerPending(t *testing.T) {
	db := testDB(t)
	u := NewUnitOfWork(db, "order-service")
	s := NewStore(db)
	ctx := context.Background()
	o := newOrder("rider-1")
	insert(t, u, o)
	s.Set(ctx, o.ID, domain.StatusMatched, "d")

	var cancelled bool
	err := u.Do(ctx, func(tx app.Tx) error {
		var err error
		cancelled, err = tx.CancelPending(ctx, o.ID, "late")
		return err
	})
	if err != nil || cancelled {
		t.Errorf("CancelPending on MATCHED = %v, %v; want (false, nil)", cancelled, err)
	}
	if got, _ := s.ByID(ctx, o.ID); got.Status != domain.StatusMatched {
		t.Errorf("status = %s, want MATCHED", got.Status)
	}
}

func TestAnExpiredKeyIsDeletedOnLookup(t *testing.T) {
	db := testDB(t)
	u := NewUnitOfWork(db, "order-service")
	s := NewStore(db)
	ctx := context.Background()
	o := newOrder("rider-1")
	rec := domain.KeyRecord{Key: "k", Fingerprint: "fp", OrderID: o.ID, Status: 201, Body: []byte("{}")}
	if err := u.Do(ctx, func(tx app.Tx) error {
		if err := tx.InsertOrder(ctx, o); err != nil {
			return err
		}
		return tx.SaveKey(ctx, "rider-1", rec)
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.Lookup(ctx, "rider-1", "k")
	if err != nil || got == nil {
		t.Fatal(got, err)
	}
	if got.OrderID != o.ID || got.Fingerprint != "fp" || got.Status != 201 || string(got.Body) != "{}" || got.Key != "k" {
		t.Errorf("record = %+v", got)
	}
	if none, err := s.Lookup(ctx, "rider-2", "k"); err != nil || none != nil {
		t.Errorf("another rider's lookup = %v, %v; want nil, nil", none, err)
	}

	age := func(d time.Duration) {
		db.Exec(ctx, `UPDATE idempotency_keys SET created_at = now() - make_interval(secs => $1)`, d.Seconds())
	}
	age(domain.IdempotencyTTL - time.Hour)
	if got, _ := s.Lookup(ctx, "rider-1", "k"); got == nil {
		t.Error("a key inside its 24h is expired")
	}
	age(domain.IdempotencyTTL + time.Hour)
	if got, err := s.Lookup(ctx, "rider-1", "k"); err != nil || got != nil {
		t.Errorf("expired key = %v, %v; want nil, nil", got, err)
	}
	if n := count(t, db, "idempotency_keys"); n != 0 {
		t.Errorf("%d keys left, want the expired one deleted", n)
	}

	// The same inside a transaction (with the lock statement).
	if err := u.Do(ctx, func(tx app.Tx) error { return tx.SaveKey(ctx, "rider-1", rec) }); err != nil {
		t.Fatal(err)
	}
	age(domain.IdempotencyTTL + time.Hour)
	u.Do(ctx, func(tx app.Tx) error {
		if err := tx.LockKey(ctx, "rider-1", "k"); err != nil {
			t.Error(err)
		}
		if got, err := tx.Lookup(ctx, "rider-1", "k"); err != nil || got != nil {
			t.Errorf("Tx.Lookup of an expired key = %v, %v", got, err)
		}
		return nil
	})
	if n := count(t, db, "idempotency_keys"); n != 0 {
		t.Errorf("%d keys left after Tx.Lookup, want 0", n)
	}
}

func TestPurgeDeletesExpiredKeysInBatches(t *testing.T) {
	db := testDB(t)
	u := NewUnitOfWork(db, "order-service")
	s := NewStore(db)
	ctx := context.Background()
	o := newOrder("rider-1")
	insert(t, u, o)
	if _, err := db.Exec(ctx, `
		INSERT INTO idempotency_keys (rider_id, key, order_id, fingerprint, status_code, response, created_at)
		SELECT 'r', 'k' || g, $1, 'fp', 201, '{}', now() - interval '25 hours' FROM generate_series(1, 5001) g`, o.ID); err != nil {
		t.Fatal(err)
	}
	db.Exec(ctx, `INSERT INTO idempotency_keys (rider_id, key, order_id, fingerprint, status_code, response) VALUES ('r', 'fresh', $1, 'fp', 201, '{}')`, o.ID)
	n, err := s.Purge(ctx)
	if err != nil || n != 5000 {
		t.Fatalf("first purge = %d, %v; want a batch of 5000", n, err)
	}
	n, err = s.Purge(ctx)
	if err != nil || n != 1 {
		t.Fatalf("second purge = %d, %v; want 1", n, err)
	}
	if left := count(t, db, "idempotency_keys"); left != 1 {
		t.Errorf("%d keys left, want only the fresh one", left)
	}
}

// The primary key is the backstop when the lock is bypassed: the loser of the
// race gets ErrDuplicateKey, and its order and outbox row are rolled back.
func TestTheUniqueKeyStopsASecondOrderEvenWithoutTheLock(t *testing.T) {
	db := testDB(t)
	u := NewUnitOfWork(db, "order-service")
	ctx := context.Background()

	book := func() error {
		o := newOrder("rider-1")
		return u.Do(ctx, func(tx app.Tx) error {
			if err := tx.InsertOrder(ctx, o); err != nil {
				return err
			}
			if err := tx.QueueOrderCreated(ctx, o.ID, []byte("x")); err != nil {
				return err
			}
			return tx.SaveKey(ctx, "rider-1", domain.KeyRecord{Key: "k", Fingerprint: "fp", OrderID: o.ID, Status: 201, Body: []byte("{}")})
		})
	}
	if err := book(); err != nil {
		t.Fatal(err)
	}
	err := book()
	if !errors.Is(err, app.ErrDuplicateKey) {
		t.Fatalf("second booking err = %v, want app.ErrDuplicateKey", err)
	}
	if count(t, db, "orders") != 1 || count(t, db, "outbox") != 1 {
		t.Errorf("%d orders, %d outbox rows after the rejected insert, want 1 each", count(t, db, "orders"), count(t, db, "outbox"))
	}
}

// Only the idempotency_keys primary key is a duplicate key; any other error,
// including another unique violation, passes through.
func TestSaveKeyPassesOtherErrorsThrough(t *testing.T) {
	db := testDB(t)
	u := NewUnitOfWork(db, "order-service")
	ctx := context.Background()
	o := newOrder("rider-1")
	insert(t, u, o)

	// order_id is a UUID column: a malformed id is a data exception.
	err := u.Do(ctx, func(tx app.Tx) error {
		return tx.SaveKey(ctx, "rider-1", domain.KeyRecord{Key: "k", Fingerprint: "fp", OrderID: "not-a-uuid", Status: 201, Body: []byte("{}")})
	})
	if err == nil || errors.Is(err, app.ErrDuplicateKey) {
		t.Errorf("bad order id err = %v, want it passed through", err)
	}
	// A second order with the same id is a unique violation on orders_pkey.
	err = u.Do(ctx, func(tx app.Tx) error { return tx.InsertOrder(ctx, o) })
	if err == nil || errors.Is(err, app.ErrDuplicateKey) {
		t.Errorf("InsertOrder duplicate err = %v, want it passed through unmapped", err)
	}
}
