package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func secondPool(t *testing.T, db *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	p, err := Open(context.Background(), dbURL(db))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestTheResyncLockExcludesASecondHolderAndFreesOnUnlock(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if ResyncLockKey != 0x726573796e63 {
		t.Fatalf("key %#x", ResyncLockKey)
	}
	a, b := NewResyncLocker(db), NewResyncLocker(secondPool(t, db))
	unlock, got, err := a.TryLock(ctx)
	if err != nil || !got {
		t.Fatal(got, err)
	}
	if _, got2, err := b.TryLock(ctx); err != nil || got2 {
		t.Fatalf("a second holder got the lock: %v %v", got2, err)
	}
	// A refused attempt returned its connection: the pool is not drained by refusals.
	for range 5 {
		if _, got2, err := b.TryLock(ctx); err != nil || got2 {
			t.Fatal(got2, err)
		}
	}
	unlock()
	unlock2, got, err := b.TryLock(ctx)
	if err != nil || !got {
		t.Fatalf("the lock is not free after unlock: %v %v", got, err)
	}
	unlock2()
}

func TestTheLockIsFreedEvenWhenTheCallersContextIsCancelled(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	unlock, got, err := NewResyncLocker(db).TryLock(ctx)
	if err != nil || !got {
		t.Fatal(got, err)
	}
	cancel()
	unlock()
	other := NewResyncLocker(secondPool(t, db))
	unlock2, got, err := other.TryLock(context.Background())
	if err != nil || !got {
		t.Fatalf("the lock outlived the cancelled run: %v %v", got, err)
	}
	unlock2()
}

func TestAFailedUnlockClosesTheConnectionInsteadOfReturningIt(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	unlock, got, err := NewResyncLocker(db).TryLock(ctx)
	if err != nil || !got {
		t.Fatal(got, err)
	}
	// Kill the holder's session: the unlock query fails.
	if _, err := secondPool(t, db).Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname = current_database() AND query LIKE '%pg_try_advisory_lock%' AND pid <> pg_backend_pid()`); err != nil {
		t.Fatal(err)
	}
	unlock()
	deadline := time.Now().Add(5 * time.Second)
	for db.Stat().TotalConns() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the failed connection went back to the pool: %d connections, %d idle", db.Stat().TotalConns(), db.Stat().IdleConns())
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The pool still works and the lock is free.
	unlock2, got, err := NewResyncLocker(db).TryLock(ctx)
	if err != nil || !got {
		t.Fatal(got, err)
	}
	unlock2()
}
