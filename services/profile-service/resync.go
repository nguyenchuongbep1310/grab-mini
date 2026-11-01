package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/pkg/eventcodec"
)

// resyncReviewer is ReviewedBy (and the reason, when none is stored) of an event that only
// repeats a decision made earlier.
const resyncReviewer = "resync"

// resyncBatch is how many drivers one transaction handles: a transaction holds row locks
// and a batch of outbox rows, so it stays short.
const resyncBatch = 200

// resyncLockKey is the Postgres advisory lock that lets one resync run at a time across
// every instance and the CLI (an arbitrary constant: "resync" in hex).
const resyncLockKey int64 = 0x726573796e63

// resyncTimeout bounds one run, which carries on even if the caller disconnects.
const resyncTimeout = 15 * time.Minute

var (
	resyncEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_resync_events_total",
		Help: "driver.application.reviewed events queued by a resync, by the status they repeat (APPROVED, REJECTED, SUSPENDED).",
	}, []string{"status"})

	resyncRuns = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "profile_resync_runs_total",
		Help: "Resyncs (POST /driver-applications/resync or the resync command), by result: completed, failed, refused_running (another one was running).",
	}, []string{"result"})
)

// errResyncRunning: another resync holds the lock.
var errResyncRunning = errors.New("a resync is already running")

// resyncResult is what a resync queued. Counts is per status; Skipped is how many drivers it
// looked at and left alone because they were no longer APPROVED, REJECTED or SUSPENDED by
// the time their row was locked (or, for one driver, never were).
type resyncResult struct {
	Counts  map[string]int
	Total   int
	Skipped int
}

func newResyncResult() resyncResult {
	return resyncResult{Counts: map[string]int{statusApproved: 0, statusRejected: 0, statusSuspended: 0}}
}

func resyncable(status string) bool {
	return status == statusApproved || status == statusRejected || status == statusSuspended
}

// resync re-publishes the current status of drivers as driver.application.reviewed, for
// when Redis (where Matching keeps who may be offered trips) lost what these events had set.
// driverID "" means every APPROVED, REJECTED or SUSPENDED driver; PENDING and NONE drivers
// have no result to repeat.
//
// The profile rows are not changed. Each event is dated like a new decision (later than the
// driver's last one, see nextReviewTime), so Matching applies it when Redis lost its record
// and ignores nothing newer; with Redis intact it sets the state the driver already has.
// Events are queued in the outbox in batches, one transaction per batch, each driver's row
// locked while its event is queued so a review cannot slip in between and be overtaken.
//
// Only one resync runs at a time, across instances: errResyncRunning for the second.
// errNoDriver when driverID names nobody.
func (s *service) resync(ctx context.Context, driverID string) (resyncResult, error) {
	res := newResyncResult()
	conn, err := s.db.Acquire(ctx)
	if err != nil {
		resyncRuns.WithLabelValues("failed").Inc()
		return res, err
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, resyncLockKey).Scan(&got); err != nil {
		resyncRuns.WithLabelValues("failed").Inc()
		return res, err
	}
	if !got {
		resyncRuns.WithLabelValues("refused_running").Inc()
		return res, errResyncRunning
	}
	defer func() {
		// Unlock on a context of its own: ctx may be what ended the run. If this fails the
		// connection is closed instead of returned to the pool, which drops the lock.
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		var ok bool
		if err := conn.QueryRow(uctx, `SELECT pg_advisory_unlock($1)`, resyncLockKey).Scan(&ok); err != nil || !ok {
			conn.Conn().Close(uctx)
		}
	}()

	if driverID != "" {
		if _, err := loadDriver(ctx, s.db, driverID, false); err != nil {
			if errors.Is(err, errNoDriver) {
				return res, err // not a run: nothing was attempted
			}
			resyncRuns.WithLabelValues("failed").Inc()
			return res, err
		}
		err := s.resyncBatch(ctx, []string{driverID}, &res)
		return res, s.finishResync(err)
	}
	after := ""
	for {
		ids, err := s.resyncIDs(ctx, after)
		if err != nil {
			return res, s.finishResync(err)
		}
		if len(ids) == 0 {
			return res, s.finishResync(nil)
		}
		if err := s.resyncBatch(ctx, ids, &res); err != nil {
			return res, s.finishResync(err)
		}
		after = ids[len(ids)-1]
	}
}

func (s *service) finishResync(err error) error {
	if err != nil {
		resyncRuns.WithLabelValues("failed").Inc()
		return err
	}
	resyncRuns.WithLabelValues("completed").Inc()
	return nil
}

// resyncIDs is the next batch of driver ids after the given one, in id order.
func (s *service) resyncIDs(ctx context.Context, after string) ([]string, error) {
	rows, err := s.db.Query(ctx, `
		SELECT driver_id FROM driver_profiles
		WHERE status IN ('APPROVED', 'REJECTED', 'SUSPENDED') AND driver_id > $1
		ORDER BY driver_id LIMIT $2`, after, resyncBatch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// resyncBatch queues the events of ids in one transaction. The status is read again under
// the row lock: a driver who became PENDING since the id list was read is skipped. The counts
// are added only once the transaction committed.
func (s *service) resyncBatch(ctx context.Context, ids []string, res *resyncResult) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	counts := map[string]int{}
	skipped := 0
	now := s.now().UTC().Truncate(time.Microsecond)
	for _, id := range ids {
		d, err := loadDriver(ctx, tx, id, true)
		if errors.Is(err, errNoDriver) || (err == nil && !resyncable(d.Status)) {
			skipped++
			continue
		}
		if err != nil {
			return err
		}
		at := nextReviewTime(d.ReviewedAt, now)
		if d.Reason == "" {
			d.Reason = resyncReviewer
		}
		d.ReviewedBy = resyncReviewer
		if err := s.queueReview(ctx, tx, d, d.Status, at); err != nil {
			return err
		}
		counts[d.Status]++
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	for status, n := range counts {
		res.Counts[status] += n
		res.Total += n
		resyncEvents.WithLabelValues(status).Add(float64(n))
	}
	res.Skipped += skipped
	s.wake()
	return nil
}

// runResync is `profile-service resync [--driver id]`: the same resync as the admin
// endpoint, for an operator with database access and no admin token. It only queues the
// events; the running service's relay publishes them (they wait in the outbox if none runs).
// Unlike seed-dev it runs in every environment.
func runResync(env Env, args []string) {
	fs := flag.NewFlagSet("resync", flag.ExitOnError)
	driver := fs.String("driver", "", "resync only this driver (default: every APPROVED, REJECTED or SUSPENDED driver)")
	fs.Parse(args)
	if *driver != "" && !validDriverID(*driver) {
		log.Fatal("resync: --driver is not a valid driver id")
	}

	ctx, cancel := context.WithTimeout(context.Background(), resyncTimeout)
	defer cancel()
	pool, err := connectDB(ctx, env.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()
	s := newService(pool, eventcodec.New(env.SchemaRegistryURL), nil, env)
	res, err := s.resync(ctx, *driver)
	switch {
	case errors.Is(err, errResyncRunning):
		log.Fatalf("%s: a resync is already running; wait for it and run this again", serviceName)
	case errors.Is(err, errNoDriver):
		log.Fatalf("%s: no such driver", serviceName)
	case err != nil:
		log.Fatalf("%s: resync stopped after %d event(s) queued: %v", serviceName, res.Total, err)
	}
	fmt.Fprintf(os.Stdout, "%s: queued %d event(s): APPROVED %d, REJECTED %d, SUSPENDED %d (%d skipped)\n",
		serviceName, res.Total, res.Counts[statusApproved], res.Counts[statusRejected], res.Counts[statusSuspended], res.Skipped)
}

// validDriverID is an id that can name a driver: not empty, not too long, valid UTF-8.
func validDriverID(id string) bool {
	return id != "" && len(id) <= maxIDLen && utf8.ValidString(id)
}
