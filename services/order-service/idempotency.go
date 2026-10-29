package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Idempotency-Key on POST /orders.
//
// The header is optional. With it, a retried booking (same rider, same key,
// same body) returns the response of the first attempt and creates and
// publishes nothing new. The same key with a different body is a 422: it is a
// client bug, and silently returning an unrelated order would hide it.
//
// The key row is saved with the order and its outbox event in one
// transaction, and (rider_id, key) is the primary key, so even two requests
// that race past the lookup cannot both commit. A pg advisory lock on the key
// makes the loser wait and replay instead of failing.

const (
	idempotencyKeyHeader = "Idempotency-Key"
	idempotencyKeyMaxLen = 128
	// How long a key is remembered. After this the same key books a new order.
	idempotencyTTL = 24 * time.Hour
)

var idempotentRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "orders_idempotency_total",
	Help: "POST /orders carrying an Idempotency-Key, by result: new (first use), replayed (original response returned), mismatch (422, same key with another body).",
}, []string{"result"})

var errBadIdempotencyKey = errors.New("Idempotency-Key must be 1 to 128 printable ASCII characters")

// idempotencyKey reads the header: "" when absent, an error when present but
// not 1 to 128 printable ASCII characters (space to ~) or given twice.
func idempotencyKey(h http.Header) (string, error) {
	vals := h.Values(idempotencyKeyHeader)
	switch len(vals) {
	case 0:
		return "", nil
	case 1:
	default:
		return "", errBadIdempotencyKey
	}
	k := vals[0]
	if len(k) < 1 || len(k) > idempotencyKeyMaxLen {
		return "", errBadIdempotencyKey
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x20 || k[i] > 0x7e {
			return "", errBadIdempotencyKey
		}
	}
	return k, nil
}

// fingerprint identifies a booking request: a hash of the normalized body, so
// two bodies that mean the same (field order, whitespace, an omitted
// vehicleType versus "CAR") get the same value. The rider is not part of it:
// keys are scoped to the rider already, and the body can only name the caller.
// Call it after req.VehicleType has been defaulted and validated.
func fingerprint(req createOrderRequest) string {
	type latLng struct{ Lat, Lng float64 }
	norm := struct {
		Pickup, Dropoff latLng
		VehicleType     string
		QuoteID         string
	}{
		Pickup:      latLng{req.Pickup.Lat, req.Pickup.Lng},
		Dropoff:     latLng{req.Dropoff.Lat, req.Dropoff.Lng},
		VehicleType: string(req.VehicleType),
		QuoteID:     req.QuoteID,
	}
	b, _ := json.Marshal(norm) // a fixed struct: the field order is the declaration order
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// idemRecord is what is stored under a key.
type idemRecord struct {
	Key         string
	Fingerprint string
	OrderID     string
	Status      int
	Body        []byte
}

type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// lockKey serializes requests that share a rider and key until the
// transaction ends, so a duplicate waits for the first to commit and then
// finds its row.
func lockKey(ctx context.Context, tx queryer, riderID, key string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, fmt.Sprintf("%d:%s:%s", len(riderID), riderID, key))
	return err
}

// lookupKey returns the live record for (riderID, key), or nil. A record
// older than idempotencyTTL is removed and reported as absent.
func lookupKey(ctx context.Context, q queryer, riderID, key string) (*idemRecord, error) {
	var rec idemRecord
	var expired bool
	err := q.QueryRow(ctx, `
		SELECT order_id::text, fingerprint, status_code, response, created_at < now() - make_interval(secs => $3)
		FROM idempotency_keys WHERE rider_id = $1 AND key = $2`,
		riderID, key, idempotencyTTL.Seconds(),
	).Scan(&rec.OrderID, &rec.Fingerprint, &rec.Status, &rec.Body, &expired)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if expired {
		if _, err := q.Exec(ctx, `DELETE FROM idempotency_keys WHERE rider_id = $1 AND key = $2`, riderID, key); err != nil {
			return nil, err
		}
		return nil, nil
	}
	rec.Key = key
	return &rec, nil
}

func saveKey(ctx context.Context, tx queryer, riderID string, rec idemRecord) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (rider_id, key, order_id, fingerprint, status_code, response)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		riderID, rec.Key, rec.OrderID, rec.Fingerprint, rec.Status, rec.Body)
	return err
}

// isDuplicateKey reports a primary-key violation on idempotency_keys: another
// request with this rider and key committed first.
func isDuplicateKey(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505" && pg.ConstraintName == "idempotency_keys_pkey"
}

// answerFromKey writes the response for a request whose key has a record:
// the original response for the same body, a 422 for another. It reports
// whether it answered.
func answerFromKey(w http.ResponseWriter, rec *idemRecord, fp string) bool {
	if rec == nil {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	if rec.Fingerprint != fp {
		idempotentRequests.WithLabelValues("mismatch").Inc()
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(`{"error":"this Idempotency-Key was already used with a different request body"}` + "\n"))
		return true
	}
	idempotentRequests.WithLabelValues("replayed").Inc()
	w.Header().Set("Idempotent-Replayed", "true")
	w.WriteHeader(rec.Status)
	w.Write(rec.Body)
	return true
}

// purgeExpiredKeys deletes keys past their TTL, a batch at a time, and
// returns how many went. Expired keys are ignored by lookupKey either way;
// this only keeps the table small.
func purgeExpiredKeys(ctx context.Context, db *pgxpool.Pool) (int64, error) {
	tag, err := db.Exec(ctx, `
		DELETE FROM idempotency_keys WHERE ctid IN (
			SELECT ctid FROM idempotency_keys WHERE created_at < now() - make_interval(secs => $1) LIMIT 5000)`,
		idempotencyTTL.Seconds())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// runKeyPurge purges expired keys every interval until ctx ends. Replicas all
// run it; deleting the same rows twice is harmless.
func runKeyPurge(ctx context.Context, db *pgxpool.Pool, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		for {
			n, err := purgeExpiredKeys(ctx, db)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("purging expired idempotency keys: %v", err)
				}
				break
			}
			if n > 0 {
				log.Printf("purged %d expired idempotency key(s)", n)
			}
			if n < 5000 {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
