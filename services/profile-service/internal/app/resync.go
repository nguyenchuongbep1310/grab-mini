package app

import (
	"context"
	"errors"
	"time"

	"grab-mini/profile-service/internal/domain"
)

// ResyncBatch is how many drivers one transaction handles: a transaction holds row locks
// and a batch of outbox rows, so it stays short.
const ResyncBatch = 200

// ResyncTimeout bounds one run, which carries on even if the caller disconnects.
const ResyncTimeout = 15 * time.Minute

// ErrResyncRunning: another resync holds the lock.
var ErrResyncRunning = errors.New("a resync is already running")

// ResyncResult is what a resync queued. Counts is per status; Skipped is how many drivers it
// looked at and left alone because they were no longer APPROVED, REJECTED or SUSPENDED by
// the time their row was locked (or, for one driver, never were).
type ResyncResult struct {
	Counts  map[string]int
	Total   int
	Skipped int
}

func newResyncResult() ResyncResult {
	return ResyncResult{Counts: map[string]int{domain.StatusApproved: 0, domain.StatusRejected: 0, domain.StatusSuspended: 0}}
}

// Resync re-publishes the current status of drivers as driver.application.reviewed, for
// when Redis (where Matching keeps who may be offered trips) lost what these events had set.
// driverID "" means every APPROVED, REJECTED or SUSPENDED driver; PENDING and NONE drivers
// have no result to repeat.
//
// The profile rows are not changed. Each event is dated like a new decision (later than the
// driver's last one, see domain.ForResync), so Matching applies it when Redis lost its record
// and ignores nothing newer; with Redis intact it sets the state the driver already has.
// Events are queued in batches, one transaction per batch, each driver's row locked while its
// event is queued so a review cannot slip in between and be overtaken.
//
// Only one resync runs at a time, across instances: ErrResyncRunning for the second.
// domain.ErrNoDriver when driverID names nobody (not counted as a run). The result so far is
// returned together with any error. The lock is released on every return path, after the
// run was counted.
func (s *Service) Resync(ctx context.Context, driverID string) (ResyncResult, error) {
	res := newResyncResult()
	unlock, got, err := s.locker.TryLock(ctx)
	if err != nil {
		s.metrics.ResyncRun("failed")
		return res, err
	}
	if !got {
		s.metrics.ResyncRun("refused_running")
		return res, ErrResyncRunning
	}
	defer unlock()

	if driverID != "" {
		if _, err := s.drivers.Driver(ctx, driverID); err != nil {
			if errors.Is(err, domain.ErrNoDriver) {
				return res, err // not a run: nothing was attempted
			}
			s.metrics.ResyncRun("failed")
			return res, err
		}
		err := s.resyncBatch(ctx, []string{driverID}, &res)
		return res, s.finishResync(err)
	}
	after := ""
	for {
		ids, err := s.resync.ResyncableIDs(ctx, after, ResyncBatch)
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

func (s *Service) finishResync(err error) error {
	if err != nil {
		s.metrics.ResyncRun("failed")
		return err
	}
	s.metrics.ResyncRun("completed")
	return nil
}

// resyncBatch queues the events of ids in one transaction. The status is read again under
// the row lock: a driver who became PENDING since the id list was read is skipped. The counts
// are added only once the transaction committed; the relay is woken after every committed
// batch, even one where every driver was skipped.
func (s *Service) resyncBatch(ctx context.Context, ids []string, res *ResyncResult) error {
	var counts map[string]int
	var skipped int
	err := s.uow.Do(ctx, func(tx Tx) error {
		counts, skipped = map[string]int{}, 0
		now := s.now()
		for _, id := range ids {
			d, err := tx.LockDriver(ctx, id)
			if errors.Is(err, domain.ErrNoDriver) || (err == nil && !domain.Resyncable(d.Status)) {
				skipped++
				continue
			}
			if err != nil {
				return err
			}
			d, at := domain.ForResync(d, now)
			if err := s.queueReview(ctx, tx, d, d.Status, at); err != nil {
				return err
			}
			counts[d.Status]++
		}
		return nil
	})
	if err != nil {
		return err
	}
	for status, n := range counts {
		res.Counts[status] += n
		res.Total += n
		s.metrics.ResyncEvents(status, n)
	}
	res.Skipped += skipped
	s.wake()
	return nil
}
