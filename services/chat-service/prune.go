package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Retention. Chat messages are personal data, so they are not kept for ever:
// messages older than CHAT_RETENTION_DAYS are deleted, and so are the rows of
// trips that ended that long ago (nothing can be sent on them, and a trip row
// holds two user ids). Both are deleted in batches, so a large backlog does not
// hold a long lock. The dedupe table chat_events is pruned by the retention
// CronJob like every other service's (deploy/k8s/generate.py).
const (
	pruneBatch    = 5000
	pruneInterval = time.Hour
)

// pruneOnce deletes what is past retention and returns how many messages and
// trips went.
func pruneOnce(ctx context.Context, db *pgxpool.Pool, retentionDays int) (messages, trips int64, err error) {
	age := float64(retentionDays) * 24 * 3600
	for {
		tag, err := db.Exec(ctx, `
			DELETE FROM chat_messages WHERE ctid IN (
				SELECT ctid FROM chat_messages WHERE created_at < now() - make_interval(secs => $1) LIMIT $2)`, age, pruneBatch)
		if err != nil {
			return messages, trips, err
		}
		messages += tag.RowsAffected()
		if tag.RowsAffected() < pruneBatch {
			break
		}
	}
	for {
		tag, err := db.Exec(ctx, `
			DELETE FROM chat_trips WHERE ctid IN (
				SELECT ctid FROM chat_trips
				WHERE status IN ('COMPLETED', 'CANCELLED') AND updated_at < now() - make_interval(secs => $1) LIMIT $2)`, age, pruneBatch)
		if err != nil {
			return messages, trips, err
		}
		trips += tag.RowsAffected()
		if tag.RowsAffected() < pruneBatch {
			break
		}
	}
	pruned.WithLabelValues("chat_messages").Add(float64(messages))
	pruned.WithLabelValues("chat_trips").Add(float64(trips))
	return messages, trips, nil
}

// runPrune prunes every interval until ctx ends. Replicas all run it; deleting
// the same rows twice is harmless.
func runPrune(ctx context.Context, db *pgxpool.Pool, retentionDays int, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		m, tr, err := pruneOnce(ctx, db, retentionDays)
		switch {
		case err != nil && ctx.Err() == nil:
			log.Printf("pruning chat past %d days: %v", retentionDays, err)
		case m > 0 || tr > 0:
			log.Printf("pruned %d message(s) and %d finished trip(s) older than %d days", m, tr, retentionDays)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
