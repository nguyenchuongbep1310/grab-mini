package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/promotion-service/internal/app"
	"grab-mini/promotion-service/internal/domain"
)

const eventA = "11111111-1111-4111-8111-111111111111"

func setup(t *testing.T, code string) (*pgxpool.Pool, *UnitOfWork) {
	t.Helper()
	db := testDB(t)
	if err := NewStore(db).Create(context.Background(), samplePromotion(code), "admin"); err != nil {
		t.Fatal(err)
	}
	return db, NewUnitOfWork(db, "promotion-service")
}

func redemption(order, code string) app.NewRedemption {
	return app.NewRedemption{OrderID: order, Code: code, RiderID: "rider", Discount: 5000, StoredStatus: "REDEEMED", SourceEventID: eventA}
}

func TestAnOrderRedemptionAndItsOutboxRowCommitTogether(t *testing.T) {
	db, uow := setup(t, "TOGETHER")
	ctx := context.Background()
	err := uow.Do(ctx, func(tx app.Tx) error {
		if ins, err := tx.InsertRedemption(ctx, redemption("o1", "TOGETHER")); err != nil || !ins {
			t.Fatalf("InsertRedemption = %v, %v", ins, err)
		}
		return tx.QueueRedeemed(ctx, "o1", []byte("payload"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, `SELECT count(*) FROM redemptions`); n != 1 {
		t.Fatalf("redemptions = %d", n)
	}
	if n := count(t, db, `SELECT count(*) FROM outbox WHERE topic = 'promotion.redeemed' AND source = 'promotion-service'`); n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}

	// And they roll back together.
	boom := errors.New("boom")
	err = uow.Do(ctx, func(tx app.Tx) error {
		if _, err := tx.InsertRedemption(ctx, redemption("o2", "TOGETHER")); err != nil {
			return err
		}
		if err := tx.QueueRedeemed(ctx, "o2", []byte("p2")); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Do = %v, want fn's error", err)
	}
	if n := count(t, db, `SELECT count(*) FROM redemptions`); n != 1 {
		t.Fatalf("redemptions after rollback = %d, want 1", n)
	}
	if n := count(t, db, `SELECT count(*) FROM outbox`); n != 1 {
		t.Fatalf("outbox after rollback = %d, want 1", n)
	}
}

func TestAFailedFnLeavesNoEventMarker(t *testing.T) {
	db, uow := setup(t, "MARK")
	ctx := context.Background()
	boom := errors.New("boom")
	_ = uow.Do(ctx, func(tx app.Tx) error {
		if first, err := tx.MarkEventProcessed(ctx, eventA); err != nil || !first {
			t.Fatalf("Mark = %v, %v", first, err)
		}
		return boom
	})
	if n := count(t, db, `SELECT count(*) FROM promotion_events`); n != 0 {
		t.Fatalf("markers after a failed fn = %d, want 0", n)
	}
	for i, want := range []bool{true, false} {
		err := uow.Do(ctx, func(tx app.Tx) error {
			first, err := tx.MarkEventProcessed(ctx, eventA)
			if first != want {
				t.Errorf("attempt %d: first = %v, want %v", i, first, want)
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTheAdvisoryLockSerialisesTwoTransactionsOnOneOrder(t *testing.T) {
	_, uow := setup(t, "LOCK")
	ctx := context.Background()
	holding, release, secondDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- uow.Do(ctx, func(tx app.Tx) error {
			if err := tx.LockOrder(ctx, "order-1"); err != nil {
				return err
			}
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	go func() {
		defer close(secondDone)
		uow.Do(ctx, func(tx app.Tx) error { return tx.LockOrder(ctx, "order-1") })
	}()
	select {
	case <-secondDone:
		t.Fatal("the second LockOrder returned while the first transaction was open")
	case <-time.After(300 * time.Millisecond):
	}
	// A different order is not blocked.
	if err := uow.Do(ctx, func(tx app.Tx) error { return tx.LockOrder(ctx, "order-2") }); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the second LockOrder never got the lock after the first committed")
	}
	if err := <-firstErr; err != nil {
		t.Fatal(err)
	}
}

func TestTheRowLockSerialisesRedemptionsOfOneCode(t *testing.T) {
	_, uow := setup(t, "ROW")
	ctx := context.Background()
	holding, release, secondDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go uow.Do(ctx, func(tx app.Tx) error {
		if _, err := tx.FindPromotionForUpdate(ctx, "ROW"); err != nil {
			t.Error(err)
		}
		close(holding)
		<-release
		return nil
	})
	<-holding
	go func() {
		defer close(secondDone)
		uow.Do(ctx, func(tx app.Tx) error {
			_, err := tx.FindPromotionForUpdate(ctx, "ROW")
			return err
		})
	}()
	select {
	case <-secondDone:
		t.Fatal("the second FOR UPDATE returned while the first transaction was open")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the second FOR UPDATE never returned")
	}
	err := uow.Do(ctx, func(tx app.Tx) error {
		_, err := tx.FindPromotionForUpdate(ctx, "NOPE")
		return err
	})
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown code = %v, want ErrNotFound", err)
	}
}

func TestReleaseOnlyMovesRedeemedAndOverLimitRows(t *testing.T) {
	db, uow := setup(t, "REL")
	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour).Truncate(time.Microsecond)
	for _, r := range []struct{ order, status string }{{"o1", "REDEEMED"}, {"o2", "OVER_LIMIT"}} {
		if _, err := db.Exec(ctx, `INSERT INTO redemptions (order_id, code, rider_id, discount, status, source_event_id) VALUES ($1, 'REL', 'r', 1, $2, $3)`, r.order, r.status, uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(ctx, `INSERT INTO redemptions (order_id, code, rider_id, discount, status, source_event_id, released_at) VALUES ('o3', 'REL', 'r', 1, 'RELEASED', $2, $1)`, old, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	for order, want := range map[string]int64{"o1": 1, "o2": 1, "o3": 0, "none": 0} {
		var n int64
		err := uow.Do(ctx, func(tx app.Tx) (err error) { n, err = tx.ReleaseRedemptions(ctx, order); return })
		if err != nil || n != want {
			t.Fatalf("release %s = %d, %v; want %d", order, n, err, want)
		}
	}
	var at time.Time
	if err := db.QueryRow(ctx, `SELECT released_at FROM redemptions WHERE order_id = 'o3'`).Scan(&at); err != nil || !at.Equal(old) {
		t.Fatalf("o3 released_at = %v, %v; want it kept at %v", at, err, old)
	}
	if n := count(t, db, `SELECT count(*) FROM redemptions WHERE status = 'RELEASED' AND released_at IS NOT NULL`); n != 3 {
		t.Fatalf("released rows with a time = %d, want 3", n)
	}
}

func TestInsertRedemptionIsIdempotentPerCodeAndOrder(t *testing.T) {
	db, uow := setup(t, "IDEM")
	ctx := context.Background()
	over := redemption("o1", "IDEM")
	over.StoredStatus, over.OverLimitReason = "OVER_LIMIT", "usage_limit"
	again := redemption("o1", "IDEM")
	again.SourceEventID = "22222222-2222-4222-8222-222222222222"
	for i, tc := range []struct {
		r    app.NewRedemption
		want bool
	}{{over, true}, {again, false}} {
		err := uow.Do(ctx, func(tx app.Tx) error {
			ins, err := tx.InsertRedemption(ctx, tc.r)
			if ins != tc.want {
				t.Errorf("insert %d = %v, want %v", i, ins, tc.want)
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := count(t, db, `SELECT count(*) FROM redemptions`); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
	// Empty reason is NULL; a given one is stored.
	if n := count(t, db, `SELECT count(*) FROM redemptions WHERE over_limit_reason = 'usage_limit'`); n != 1 {
		t.Fatal("the over-limit reason was not stored")
	}
	plain := redemption("o2", "IDEM")
	rel := time.Now().Truncate(time.Microsecond)
	plain.ReleasedAt = &rel
	plain.StoredStatus = "RELEASED"
	if err := uow.Do(ctx, func(tx app.Tx) error { _, err := tx.InsertRedemption(ctx, plain); return err }); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, `SELECT count(*) FROM redemptions WHERE order_id = 'o2' AND over_limit_reason IS NULL AND released_at IS NOT NULL`); n != 1 {
		t.Fatal("an empty reason must be NULL and released_at kept")
	}
}

func TestCancelledOrdersAreRemembered(t *testing.T) {
	_, uow := setup(t, "CAN")
	ctx := context.Background()
	for i, want := range []bool{false, true, true} {
		err := uow.Do(ctx, func(tx app.Tx) error {
			got, err := tx.OrderWasCancelled(ctx, "o9")
			if err != nil || got != want {
				t.Errorf("step %d: OrderWasCancelled = %v, %v; want %v", i, got, err, want)
			}
			if i > 0 {
				return nil
			}
			return tx.RememberCancelled(ctx, "o9") // twice is fine
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := uow.Do(ctx, func(tx app.Tx) error {
		if err := tx.RememberCancelled(ctx, "o9"); err != nil {
			return err
		}
		return tx.RememberCancelled(ctx, "o9")
	}); err != nil {
		t.Fatal(err)
	}
}
