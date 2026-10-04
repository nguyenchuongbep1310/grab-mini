// Package outbox is the transactional outbox: a service saves the events it
// wants to publish in the same Postgres transaction as the state change that
// caused them, and a relay publishes them to Kafka afterwards.
//
// Without it, "write the row, then publish" can fail between the two steps
// and leave a row nobody was told about (an order nobody matches, a completed
// trip the rider never hears of). Here either both are committed or neither.
//
// The relay is at-least-once: if it publishes and then dies before deleting
// the row, the event goes out again. Consumers already dedupe on eventId.
//
// Ordering: one relay publishes at a time per source (an advisory lock), in
// insertion order, so events for one key reach Kafka in the order they were
// committed.
package outbox

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

const schema = `
	CREATE TABLE IF NOT EXISTS outbox (
		id BIGSERIAL PRIMARY KEY,
		source TEXT NOT NULL,
		topic TEXT NOT NULL,
		key BYTEA,
		payload BYTEA NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	);
	CREATE INDEX IF NOT EXISTS outbox_source_id ON outbox (source, id);
`

// Execer is satisfied by *pgxpool.Pool and pgx.Tx, so Add can join the
// caller's transaction.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Migrate creates the outbox table if it doesn't exist yet. Services don't
// call it: they create the table through their own migrations
// (order-service/migrations/00003_create_outbox.sql, and the same in
// trip-service), which must stay in step with schema below. It is for tests
// and one-off setups.
func Migrate(ctx context.Context, db Execer) error {
	_, err := db.Exec(ctx, schema)
	return err
}

// Add queues one event. Call it with the pgx.Tx that carries the state
// change, so both commit together. source names the owning service; only its
// relay publishes the row.
func Add(ctx context.Context, tx Execer, source, topic string, key, payload []byte) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO outbox (source, topic, key, payload) VALUES ($1, $2, $3, $4)`,
		source, topic, key, payload)
	return err
}

// Writer is the part of *kafka.Writer the relay needs. Messages carry their
// own Topic, so the writer must not have one set.
type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Relay struct {
	db     *pgxpool.Pool
	w      Writer
	source string
	// Interval is how often to look for rows even when nobody called Wake
	// (default 1s). BatchSize caps rows per publish (default 100).
	Interval  time.Duration
	BatchSize int

	wake chan struct{}
}

func NewRelay(db *pgxpool.Pool, w Writer, source string) *Relay {
	return &Relay{db: db, w: w, source: source, Interval: time.Second, BatchSize: 100, wake: make(chan struct{}, 1)}
}

// Wake asks the relay to publish now instead of at the next tick. Call it
// after committing a transaction that used Add; it never blocks.
func (r *Relay) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run publishes queued events until ctx is cancelled, then makes one last
// attempt to drain what is left.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	backoff := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
			drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := r.flushAll(drainCtx); err != nil {
				log.Printf("[outbox:%s] final drain failed, rows stay queued: %v", r.source, err)
			}
			return
		case <-r.wake:
		case <-ticker.C:
		}

		if backoff > 0 {
			select {
			case <-ctx.Done():
				continue
			case <-time.After(backoff):
			}
		}
		if _, err := r.flushAll(ctx); err != nil {
			if ctx.Err() != nil {
				continue
			}
			backoff = min(max(backoff*2, 500*time.Millisecond), 10*time.Second)
			log.Printf("[outbox:%s] publish failed, retrying in %s: %v", r.source, backoff, err)
			continue
		}
		backoff = 0
	}
}

// flushAll publishes batches until the outbox is empty.
func (r *Relay) flushAll(ctx context.Context) (int, error) {
	total := 0
	for {
		n, err := r.Flush(ctx)
		total += n
		if err != nil || n < r.BatchSize {
			return total, err
		}
	}
}

// Flush publishes one batch and reports how many events went out. The rows
// are deleted in the transaction that holds the lock, after Kafka has
// acknowledged them.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var got bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext($1))`, "outbox:"+r.source).Scan(&got); err != nil {
		return 0, err
	}
	if !got {
		return 0, nil // another instance is relaying this source
	}

	rows, err := tx.Query(ctx,
		`SELECT id, topic, key, payload FROM outbox WHERE source = $1 ORDER BY id LIMIT $2`,
		r.source, r.BatchSize)
	if err != nil {
		return 0, err
	}
	ids, msgs, err := collect(rows)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	if err := r.w.WriteMessages(ctx, msgs...); err != nil {
		return 0, fmt.Errorf("publish %d event(s): %w", len(msgs), err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM outbox WHERE id = ANY($1)`, ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(ids), nil
}

func collect(rows pgx.Rows) ([]int64, []kafka.Message, error) {
	defer rows.Close()
	var ids []int64
	var msgs []kafka.Message
	for rows.Next() {
		var id int64
		var m kafka.Message
		if err := rows.Scan(&id, &m.Topic, &m.Key, &m.Value); err != nil {
			return nil, nil, err
		}
		ids = append(ids, id)
		msgs = append(msgs, m)
	}
	return ids, msgs, rows.Err()
}
