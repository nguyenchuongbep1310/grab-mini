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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"

	"grab-mini/pkg/metrics"
	"grab-mini/pkg/tracing"
)

var (
	publishedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_published_total",
		Help: "Events the outbox relay published to Kafka, by source and topic.",
	}, []string{"source", "topic"})

	quarantinedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_quarantined_total",
		Help: "Events moved to outbox_dead because Kafka will never accept them, by source and topic.",
	}, []string{"source", "topic"})

	// The one number that says whether the relay is keeping up: how long the
	// oldest event still waiting has been waiting. Zero when nothing waits.
	oldestAge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "outbox_oldest_pending_age_seconds",
		Help: "Age of the oldest outbox event not yet published, by source (0 if none).",
	}, []string{"source"})
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
	-- The trace context of whatever queued the event, so the trace continues
	-- through the wait for the relay and on into Kafka.
	ALTER TABLE outbox ADD COLUMN IF NOT EXISTS headers JSONB;
	CREATE INDEX IF NOT EXISTS outbox_source_id ON outbox (source, id);

	-- Events Kafka will never accept (too large for the broker, say), moved
	-- aside so they don't block the ones behind them.
	CREATE TABLE IF NOT EXISTS outbox_dead (
		id BIGSERIAL PRIMARY KEY,
		source TEXT NOT NULL,
		topic TEXT NOT NULL,
		key BYTEA,
		payload BYTEA NOT NULL,
		headers JSONB,
		created_at TIMESTAMPTZ NOT NULL,
		failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		error TEXT NOT NULL
	);
`

// Execer is satisfied by *pgxpool.Pool and pgx.Tx, so Add can join the
// caller's transaction.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Migrate creates the outbox table if it doesn't exist yet. Services don't
// call it: they create the table through their own migrations
// (order-service/migrations/00003_create_outbox.sql, and the same in
// trip-service, plus the later outbox_headers and outbox_dead migrations),
// which must stay in step with schema below. It is for tests
// and one-off setups.
func Migrate(ctx context.Context, db Execer) error {
	_, err := db.Exec(ctx, schema)
	return err
}

// Add queues one event. Call it with the pgx.Tx that carries the state
// change, so both commit together. source names the owning service; only its
// relay publishes the row.
func Add(ctx context.Context, tx Execer, source, topic string, key, payload []byte) error {
	headers, err := json.Marshal(tracing.Map(ctx))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (source, topic, key, payload, headers) VALUES ($1, $2, $3, $4, $5)`,
		source, topic, key, payload, headers)
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
		_, err := r.flushAll(ctx)
		r.observeBacklog(ctx)
		if err != nil {
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

// Flush publishes one batch and reports how many events it finished with:
// published, or (see below) quarantined. Rows are deleted in the transaction
// that holds the lock, after Kafka has acknowledged them.
//
// If the batch fails, the relay publishes its rows one at a time, in order, to
// find out whether one row is to blame. A row Kafka will never accept (too
// large for the broker, say) is moved to outbox_dead, so one bad event can't
// hold up every event behind it, nor make the good ones in its batch be sent
// again and again. A failure that may pass (the broker is down, a permission
// is missing) stops the batch there: what was finished is committed, the rest
// stays queued, and the error is returned.
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
		`SELECT id, topic, key, payload, headers FROM outbox WHERE source = $1 ORDER BY id LIMIT $2`,
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

	done, deliverErr := r.deliver(ctx, tx, ids, msgs)
	// What was finished is published (or quarantined) and must not be sent
	// again, so commit it even when a later row failed.
	if done > 0 || deliverErr == nil {
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
	}
	return done, deliverErr
}

func (r *Relay) deliver(ctx context.Context, tx pgx.Tx, ids []int64, msgs []kafka.Message) (int, error) {
	started := time.Now()
	err := r.w.WriteMessages(ctx, msgs...)
	seen := map[string]bool{}
	for _, m := range msgs {
		if !seen[m.Topic] {
			seen[m.Topic] = true
			metrics.ObservePublish(m.Topic, started, err)
		}
	}
	if err == nil {
		if _, err := tx.Exec(ctx, `DELETE FROM outbox WHERE id = ANY($1)`, ids); err != nil {
			return 0, err
		}
		for _, m := range msgs {
			publishedTotal.WithLabelValues(r.source, m.Topic).Inc()
		}
		return len(ids), nil
	}

	// The batch failed as a whole; some of it may even have gone out, which is
	// fine: delivery is at-least-once and consumers dedupe.
	done := 0
	for i := range msgs {
		started := time.Now()
		err := r.w.WriteMessages(ctx, msgs[i])
		metrics.ObservePublish(msgs[i].Topic, started, err)
		switch {
		case err == nil:
			if _, err := tx.Exec(ctx, `DELETE FROM outbox WHERE id = $1`, ids[i]); err != nil {
				return done, err
			}
			publishedTotal.WithLabelValues(r.source, msgs[i].Topic).Inc()
			done++
		case isPermanent(err):
			if err := r.quarantine(ctx, tx, ids[i], msgs[i], err); err != nil {
				return done, err
			}
			done++
		default:
			return done, fmt.Errorf("publish event %d (%s): %w", ids[i], msgs[i].Topic, err)
		}
	}
	return done, nil
}

// quarantine moves a row Kafka will never accept to outbox_dead, with the
// reason, for a person to look at.
func (r *Relay) quarantine(ctx context.Context, tx pgx.Tx, id int64, m kafka.Message, cause error) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_dead (source, topic, key, payload, headers, created_at, error)
		SELECT source, topic, key, payload, headers, created_at, $2 FROM outbox WHERE id = $1`, id, cause.Error()); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM outbox WHERE id = $1`, id); err != nil {
		return err
	}
	quarantinedTotal.WithLabelValues(r.source, m.Topic).Inc()
	slog.ErrorContext(ctx, "outbox event quarantined: Kafka will never accept it",
		"source", r.source, "topic", m.Topic, "outboxId", id, "bytes", len(m.Value), "error", cause.Error())
	return nil
}

// isPermanent reports whether Kafka rejected a message for a reason that
// retrying can't change. Anything not listed is treated as possibly temporary,
// because quarantining an event that would have gone through is worse than
// waiting.
func isPermanent(err error) bool {
	var tooLarge kafka.MessageTooLargeError
	if errors.As(err, &tooLarge) {
		return true
	}
	if we, ok := err.(kafka.WriteErrors); ok {
		for _, e := range we {
			if e != nil && isPermanent(e) {
				return true
			}
		}
		return false
	}
	for _, k := range []kafka.Error{kafka.MessageSizeTooLarge, kafka.InvalidMessageSize, kafka.InvalidMessage, kafka.InvalidTopic} {
		if errors.Is(err, k) {
			return true
		}
	}
	return false
}

// observeBacklog sets the oldest-pending-age gauge. It reads the oldest row by
// id (an index scan of one row), so it is cheap however large the backlog is.
func (r *Relay) observeBacklog(ctx context.Context) {
	var age float64
	err := r.db.QueryRow(ctx,
		`SELECT COALESCE((SELECT EXTRACT(EPOCH FROM now() - created_at) FROM outbox WHERE source = $1 ORDER BY id LIMIT 1), 0)`,
		r.source).Scan(&age)
	if err != nil {
		return // keep the last value; a database that is down shows up elsewhere
	}
	oldestAge.WithLabelValues(r.source).Set(age)
}

func collect(rows pgx.Rows) ([]int64, []kafka.Message, error) {
	defer rows.Close()
	var ids []int64
	var msgs []kafka.Message
	for rows.Next() {
		var id int64
		var m kafka.Message
		var headers []byte
		if err := rows.Scan(&id, &m.Topic, &m.Key, &m.Value, &headers); err != nil {
			return nil, nil, err
		}
		var carried map[string]string
		if len(headers) > 0 {
			if err := json.Unmarshal(headers, &carried); err != nil {
				return nil, nil, fmt.Errorf("outbox row %d has unreadable headers: %w", id, err)
			}
		}
		for k, v := range carried {
			m.Headers = append(m.Headers, kafka.Header{Key: k, Value: []byte(v)})
		}
		ids = append(ids, id)
		msgs = append(msgs, m)
	}
	return ids, msgs, rows.Err()
}
