package app

import (
	"context"
	"fmt"
	"log"
	"time"

	"grab-mini/profile-service/internal/domain"
)

// ReviewCommand carries the raw request values: the path id, the action name, the admin's id
// and the reason from the body.
type ReviewCommand struct{ DriverID, Action, AdminID, Reason string }

// Review applies an admin's decision and queues driver.application.reviewed in the same
// transaction (see domain.Decide). The reason is checked before the id, and the id before the
// transaction; an id that cannot exist is domain.ErrNoDriver. In the transaction: the row is
// locked and the transition checked before the clock is read; the decision is written, then
// encoded and queued. After the commit: the decision counter, the relay wake (always), then
// the log line (ids only). An unknown action name is a plain error: the adapter registers
// only domain.ActionNames().
func (s *Service) Review(ctx context.Context, c ReviewCommand) (domain.Driver, error) {
	a, ok := domain.LookupAction(c.Action)
	if !ok {
		return domain.Driver{}, fmt.Errorf("unknown review action %q", c.Action)
	}
	reason, ferr := domain.CleanReason(c.Reason, a.NeedsReason)
	if ferr != nil {
		return domain.Driver{}, ferr
	}
	if !domain.ValidPathID(c.DriverID) {
		return domain.Driver{}, domain.ErrNoDriver
	}
	var out domain.Driver
	err := s.uow.Do(ctx, func(tx Tx) error {
		d, err := tx.LockDriver(ctx, c.DriverID)
		if err != nil {
			return err // domain.ErrNoDriver: the driver has no profile
		}
		// Decide checks the transition before it uses the time; the clock is read only for
		// an allowed one, after the lock.
		if _, _, err := domain.Decide(d, a, c.AdminID, reason, time.Time{}); err != nil {
			return err
		}
		d, at, _ := domain.Decide(d, a, c.AdminID, reason, s.now())
		if err := tx.RecordReview(ctx, d, at); err != nil {
			return err
		}
		if err := s.queueReview(ctx, tx, d, a.To, at); err != nil {
			return err
		}
		out = d
		return nil
	})
	if err != nil {
		return domain.Driver{}, err
	}
	s.metrics.Review(a.Decision)
	s.wake()
	log.Printf("driver %s: %s by %s -> %s", c.DriverID, c.Action, c.AdminID, out.Status)
	return out, nil
}
