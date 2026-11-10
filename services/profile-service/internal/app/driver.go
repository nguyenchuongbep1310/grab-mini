package app

import (
	"context"
	"errors"
	"log"
	"time"

	"grab-mini/profile-service/internal/domain"
)

// SaveDriverCommand carries the raw request values; cleaning is the use case's.
type SaveDriverCommand struct{ ID, Name, Phone, VehicleType, Plate, Model, Color string }

// SavedDriver is the profile after a save. VehicleChanged: the save took an APPROVED driver
// out of service (back to PENDING) because the plate or the vehicle type changed.
type SavedDriver struct {
	Driver         domain.Driver
	VehicleChanged bool
}

// ApplyResult is the profile after an application. Submitted is false for a repeat (the
// driver already was PENDING or APPROVED; nothing changed).
type ApplyResult struct {
	Driver    domain.Driver
	Submitted bool
}

// cleanDriver cleans the request in the order of refusals: name, phone, vehicle type, plate,
// model, colour.
func cleanDriver(c SaveDriverCommand) (domain.ProfileInput, error) {
	var in domain.ProfileInput
	var ferr *domain.FieldError
	if in.Name, ferr = domain.CleanName(c.Name); ferr != nil {
		return in, ferr
	}
	if in.Phone, ferr = domain.CleanPhone(c.Phone); ferr != nil {
		return in, ferr
	}
	if in.Vehicle, ferr = domain.CleanVehicle(c.VehicleType); ferr != nil {
		return in, ferr
	}
	if in.Plate, ferr = domain.CleanPlate(c.Plate); ferr != nil {
		return in, ferr
	}
	if in.Model, ferr = domain.Clean("model", c.Model, domain.MaxModelRunes); ferr != nil {
		return in, ferr
	}
	if in.Color, ferr = domain.Clean("color", c.Color, domain.MaxColorRunes); ferr != nil {
		return in, ferr
	}
	return in, nil
}

// SaveDriver replaces name, phone, vehicle type, plate, model and colour (see
// domain.ApplyProfile for what a vehicle change does). In one transaction: the clock is read
// before the row is created and locked; a vehicle change encodes and queues the SUSPENDED
// result (reviewedBy "system") before the profile is written. After the commit: the
// vehicle_changed review counter and the relay wake (only on a vehicle change), then the
// update counter and the log line (ids only).
func (s *Service) SaveDriver(ctx context.Context, c SaveDriverCommand) (SavedDriver, error) {
	in, err := cleanDriver(c)
	if err != nil {
		return SavedDriver{}, err
	}
	var out SavedDriver
	err = s.uow.Do(ctx, func(tx Tx) error {
		now := s.now()
		d, err := tx.LockOrCreateDriver(ctx, c.ID, now)
		if err != nil {
			return err
		}
		d, change, err := domain.ApplyProfile(d, in, now)
		if err != nil {
			return err
		}
		if change.VehicleChanged {
			if err := s.queueReview(ctx, tx, d, domain.StatusSuspended, change.At); err != nil {
				return err
			}
		}
		if err := tx.UpdateProfile(ctx, d); err != nil {
			return err
		}
		out = SavedDriver{Driver: d, VehicleChanged: change.VehicleChanged}
		return nil
	})
	if err != nil {
		return SavedDriver{}, err
	}
	if out.VehicleChanged {
		s.metrics.Review("vehicle_changed")
		s.wake()
	}
	s.metrics.Updated("driver")
	if out.VehicleChanged {
		log.Printf("driver %s changed vehicle: back to PENDING, out of service until approved", c.ID)
	} else {
		log.Printf("driver %s updated their profile", c.ID)
	}
	return out, nil
}

// Apply submits the driver's stored profile for review (see domain.Submit). In one
// transaction: the row is locked, the status and the profile are checked, and only then is
// the clock read and the application written. A repeat (PENDING or APPROVED) writes nothing
// and counts replayed; an application counts submitted and logs. Apply queues no event and
// never wakes the relay.
func (s *Service) Apply(ctx context.Context, id string) (ApplyResult, error) {
	var out ApplyResult
	err := s.uow.Do(ctx, func(tx Tx) error {
		d, err := tx.LockDriver(ctx, id)
		if errors.Is(err, domain.ErrNoDriver) {
			return domain.ErrNoProfile
		}
		if err != nil {
			return err
		}
		// The clock is read after the checks and only on the path that writes, so Submit
		// (pure) decides first without a time and runs again with now.
		if _, submitted, err := domain.Submit(d, time.Time{}); err != nil || !submitted {
			out = ApplyResult{Driver: d}
			return err
		}
		now := s.now()
		d, _, _ = domain.Submit(d, now)
		if err := tx.MarkPending(ctx, id, now); err != nil {
			return err
		}
		out = ApplyResult{Driver: d, Submitted: true}
		return nil
	})
	if err != nil {
		return ApplyResult{}, err
	}
	if !out.Submitted {
		s.metrics.Application("replayed")
		return out, nil
	}
	s.metrics.Application("submitted")
	log.Printf("driver %s applied: PENDING", id)
	return out, nil
}
