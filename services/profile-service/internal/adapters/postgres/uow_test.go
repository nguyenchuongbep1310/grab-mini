package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/events"
	"grab-mini/profile-service/internal/app"
	"grab-mini/profile-service/internal/domain"
)

var errStop = errors.New("stop")

func outboxRows(t *testing.T, db *pgxpool.Pool) (n int, key, payload []byte) {
	t.Helper()
	rows, err := db.Query(context.Background(), `SELECT topic, key, payload FROM outbox WHERE source = 'profile-service'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var topic string
		if err := rows.Scan(&topic, &key, &payload); err != nil {
			t.Fatal(err)
		}
		if topic != events.TopicDriverApplicationReviewed {
			t.Fatalf("topic %q", topic)
		}
		n++
	}
	return n, key, payload
}

func status(t *testing.T, db *pgxpool.Pool, id string) string {
	t.Helper()
	var st string
	if err := db.QueryRow(context.Background(), `SELECT status FROM driver_profiles WHERE driver_id = $1`, id).Scan(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestADecisionAndItsOutboxRowCommitTogether(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	insertDriver(t, db, "d1", domain.StatusPending, t0)
	u := NewUnitOfWork(db, "profile-service")
	err := u.Do(ctx, func(tx app.Tx) error {
		d, err := tx.LockDriver(ctx, "d1")
		if err != nil {
			return err
		}
		d.Status, d.Reason, d.ReviewedBy, d.StatusAt = domain.StatusApproved, "ok", "admin", t0.Add(time.Minute)
		if err := tx.RecordReview(ctx, d, t0.Add(time.Minute)); err != nil {
			return err
		}
		return tx.QueueReview(ctx, "d1", []byte("payload"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if st := status(t, db, "d1"); st != "APPROVED" {
		t.Fatalf("status %s", st)
	}
	n, key, payload := outboxRows(t, db)
	if n != 1 || string(key) != "d1" || string(payload) != "payload" {
		t.Fatalf("outbox: %d %q %q", n, key, payload)
	}
}

func TestADecisionAndItsOutboxRowRollBackTogether(t *testing.T) {
	for name, failAfter := range map[string]string{"QueueReview": "queue", "RecordReview": "record"} {
		t.Run("after "+name, func(t *testing.T) {
			db := testDB(t)
			ctx := context.Background()
			t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			insertDriver(t, db, "d1", domain.StatusPending, t0)
			u := NewUnitOfWork(db, "profile-service")
			err := u.Do(ctx, func(tx app.Tx) error {
				d, err := tx.LockDriver(ctx, "d1")
				if err != nil {
					return err
				}
				d.Status, d.StatusAt = domain.StatusApproved, t0.Add(time.Minute)
				if failAfter == "queue" {
					if err := tx.QueueReview(ctx, "d1", []byte("p")); err != nil {
						return err
					}
					if err := tx.RecordReview(ctx, d, t0); err != nil {
						return err
					}
					return errStop
				}
				if err := tx.RecordReview(ctx, d, t0); err != nil {
					return err
				}
				return errStop
			})
			if !errors.Is(err, errStop) {
				t.Fatalf("got %v, want fn's error untouched", err)
			}
			if st := status(t, db, "d1"); st != "PENDING" {
				t.Fatalf("status %s", st)
			}
			if n, _, _ := outboxRows(t, db); n != 0 {
				t.Fatalf("%d outbox rows survived", n)
			}
		})
	}
}

func TestLockOrCreateDriverCreatesAnEmptyNoneRowOnce(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	u := NewUnitOfWork(db, "profile-service")
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	for _, now := range []time.Time{t1, t2} {
		var d domain.Driver
		err := u.Do(ctx, func(tx app.Tx) error {
			var err error
			d, err = tx.LockOrCreateDriver(ctx, "d1", now)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if d.ID != "d1" || d.Status != "NONE" || d.Name != "" || d.Vehicle != "" || d.ReviewedAt != nil ||
			!d.StatusAt.Equal(t1) || !d.UpdatedAt.Equal(t1) {
			t.Fatalf("after %v: %+v", now, d)
		}
	}
	var n int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM driver_profiles`).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}

func TestLockDriverOfAMissingDriverIsErrNoDriver(t *testing.T) {
	db := testDB(t)
	u := NewUnitOfWork(db, "profile-service")
	err := u.Do(context.Background(), func(tx app.Tx) error {
		_, err := tx.LockDriver(context.Background(), "nobody")
		return err
	})
	if !errors.Is(err, domain.ErrNoDriver) {
		t.Fatalf("got %v", err)
	}
}

func TestLockDriverBlocksASecondTransactionUntilTheFirstEnds(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	insertDriver(t, db, "d1", domain.StatusPending, time.Now().UTC())
	u := NewUnitOfWork(db, "profile-service")
	locked, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	go func() {
		first <- u.Do(ctx, func(tx app.Tx) error {
			if _, err := tx.LockDriver(ctx, "d1"); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked
	got := make(chan error, 1)
	go func() {
		got <- u.Do(ctx, func(tx app.Tx) error {
			_, err := tx.LockDriver(ctx, "d1")
			return err
		})
	}()
	select {
	case err := <-got:
		t.Fatalf("the second transaction did not wait (err %v)", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second transaction never got the lock")
	}
}

func TestTheThreeUpdatesWriteTheirColumns(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	u := NewUnitOfWork(db, "profile-service")
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	insertDriver(t, db, "d1", domain.StatusApproved, t0)
	read := func() domain.Driver {
		d, err := NewStore(db).Driver(ctx, "d1")
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	do := func(fn func(app.Tx) error) {
		t.Helper()
		if err := u.Do(ctx, fn); err != nil {
			t.Fatal(err)
		}
	}

	// UpdateProfile: all thirteen columns.
	rev := t0.Add(time.Second)
	full := domain.Driver{ID: "d1", Name: "New", Phone: "123", Vehicle: domain.VehicleBike, Plate: "NP", Model: "M", Color: "C",
		Status: domain.StatusPending, Reason: "why", ReviewedBy: "system", ReviewedAt: &rev,
		StatusAt: t0.Add(2 * time.Second), UpdatedAt: t0.Add(3 * time.Second)}
	do(func(tx app.Tx) error { return tx.UpdateProfile(ctx, full) })
	got := read()
	if got.Name != "New" || got.Phone != "123" || got.Vehicle != domain.VehicleBike || got.Plate != "NP" || got.Model != "M" ||
		got.Color != "C" || got.Status != "PENDING" || got.Reason != "why" || got.ReviewedBy != "system" ||
		got.ReviewedAt == nil || !got.ReviewedAt.Equal(rev) || !got.StatusAt.Equal(full.StatusAt) || !got.UpdatedAt.Equal(full.UpdatedAt) {
		t.Fatalf("UpdateProfile wrote %+v", got)
	}

	// RecordReview: status, reason, reviewer, reviewed_at = at, status_at = updated_at = d.StatusAt; the rest stays.
	at, when := t0.Add(time.Hour), t0.Add(2*time.Hour)
	d := got
	d.Status, d.Reason, d.ReviewedBy, d.StatusAt = domain.StatusRejected, "no", "admin", when
	do(func(tx app.Tx) error { return tx.RecordReview(ctx, d, at) })
	got = read()
	if got.Status != "REJECTED" || got.Reason != "no" || got.ReviewedBy != "admin" || !got.ReviewedAt.Equal(at) ||
		!got.StatusAt.Equal(when) || !got.UpdatedAt.Equal(when) {
		t.Fatalf("RecordReview wrote %+v", got)
	}
	if got.Name != "New" || got.Phone != "123" || got.Plate != "NP" || got.Vehicle != domain.VehicleBike || got.Model != "M" || got.Color != "C" {
		t.Fatalf("RecordReview touched the profile fields: %+v", got)
	}

	// MarkPending: PENDING, reason cleared, status_at = updated_at = now; reviewer and reviewed_at stay.
	now := t0.Add(3 * time.Hour)
	do(func(tx app.Tx) error { return tx.MarkPending(ctx, "d1", now) })
	got = read()
	if got.Status != "PENDING" || got.Reason != "" || !got.StatusAt.Equal(now) || !got.UpdatedAt.Equal(now) {
		t.Fatalf("MarkPending wrote %+v", got)
	}
	if got.ReviewedBy != "admin" || !got.ReviewedAt.Equal(at) || got.Name != "New" || got.Plate != "NP" {
		t.Fatalf("MarkPending touched other columns: %+v", got)
	}
}
