package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ResyncLockKey is the Postgres advisory lock that lets one resync run at a time across
// every instance and the CLI (an arbitrary constant: "resync" in hex).
const ResyncLockKey int64 = 0x726573796e63

// ResyncLocker holds the resync lock on a pooled connection for the length of a run.
type ResyncLocker struct {
	db *pgxpool.Pool
}

// NewResyncLocker returns a ResyncLocker over db.
func NewResyncLocker(db *pgxpool.Pool) *ResyncLocker { return &ResyncLocker{db: db} }

// TryLock takes the session advisory lock without waiting. acquired is false when another
// run holds it. On a refusal or an error the connection is released before returning. The
// returned unlock releases the lock even when ctx is cancelled (see release).
func (l *ResyncLocker) TryLock(ctx context.Context) (unlock func(), acquired bool, err error) {
	conn, err := l.db.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, ResyncLockKey).Scan(&got); err != nil {
		conn.Release()
		return nil, false, err
	}
	if !got {
		conn.Release()
		return nil, false, nil
	}
	return func() {
		// Unlock on a context of its own: ctx may be what ended the run. If this fails the
		// connection is closed instead of returned to the pool, which drops the lock.
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		defer conn.Release()
		var ok bool
		if err := conn.QueryRow(uctx, `SELECT pg_advisory_unlock($1)`, ResyncLockKey).Scan(&ok); err != nil || !ok {
			conn.Conn().Close(uctx)
		}
	}, true, nil
}
