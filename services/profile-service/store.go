package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"grab-mini/pkg/events"
	"grab-mini/pkg/outbox"
)

// Driver statuses.
const (
	statusNone      = "NONE"
	statusPending   = "PENDING"
	statusApproved  = "APPROVED"
	statusRejected  = "REJECTED"
	statusSuspended = "SUSPENDED"
)

// systemReviewer is ReviewedBy of a result nobody decided: an approved driver changed the
// vehicle, so the service takes them out of service itself.
const systemReviewer = "system"

// vehicleChangedReason is shown to the driver whose vehicle change put them back under review.
const vehicleChangedReason = "You changed your vehicle type or plate. An admin must approve the new vehicle before you get trips again."

var (
	errNoDriver = errors.New("no such driver")
	// errNoProfile: apply before saving a profile.
	errNoProfile = errors.New("no driver profile yet")
	errSuspended = errors.New("the driver is suspended")
)

// transitionError is a review that the driver's status does not allow.
type transitionError struct{ from, action string }

func (e *transitionError) Error() string {
	return fmt.Sprintf("a driver who is %s cannot be %s", e.from, e.action)
}

// incompleteError lists what a driver must fill in before applying.
type incompleteError struct{ missing []string }

func (e *incompleteError) Error() string { return fmt.Sprintf("profile is missing %v", e.missing) }

// driverRow is a driver_profiles row. PERSONAL DATA: never log one.
type driverRow struct {
	ID         string
	Name       string
	Phone      string
	Vehicle    events.VehicleType
	Plate      string
	Model      string
	Color      string
	Status     string
	Reason     string
	ReviewedBy string
	ReviewedAt *time.Time
	StatusAt   time.Time
	UpdatedAt  time.Time
}

const driverColumns = `driver_id, name, phone, vehicle_type, plate, model, color, status, reason, reviewed_by, reviewed_at, status_at, updated_at`

type scanner interface{ Scan(dest ...any) error }

func scanDriver(row scanner) (driverRow, error) {
	var d driverRow
	var vehicle string
	err := row.Scan(&d.ID, &d.Name, &d.Phone, &vehicle, &d.Plate, &d.Model, &d.Color, &d.Status, &d.Reason, &d.ReviewedBy, &d.ReviewedAt, &d.StatusAt, &d.UpdatedAt)
	d.Vehicle = events.VehicleType(vehicle)
	if d.ReviewedAt != nil {
		t := d.ReviewedAt.UTC()
		d.ReviewedAt = &t
	}
	d.StatusAt, d.UpdatedAt = d.StatusAt.UTC(), d.UpdatedAt.UTC()
	return d, err
}

type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// loadDriver reads a profile; forUpdate locks the row until the transaction ends, so two
// changes to one driver are applied one after the other.
func loadDriver(ctx context.Context, q queryer, id string, forUpdate bool) (driverRow, error) {
	sql := `SELECT ` + driverColumns + ` FROM driver_profiles WHERE driver_id = $1`
	if forUpdate {
		sql += ` FOR UPDATE`
	}
	d, err := scanDriver(q.QueryRow(ctx, sql, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return d, errNoDriver
	}
	return d, err
}

// lockDriver creates the (empty) profile if there is none and locks it.
func lockDriver(ctx context.Context, tx pgx.Tx, id string, now time.Time) (driverRow, error) {
	if _, err := tx.Exec(ctx, `
		INSERT INTO driver_profiles (driver_id, status_at, updated_at) VALUES ($1, $2, $2) ON CONFLICT DO NOTHING`, id, now); err != nil {
		return driverRow{}, err
	}
	return loadDriver(ctx, tx, id, true)
}

// driverInput is what PUT /drivers/{id} sets, already cleaned.
type driverInput struct {
	Name, Phone, Plate, Model, Color string
	Vehicle                          events.VehicleType
}

// saveDriver replaces the editable fields of a driver's profile.
//
// The vehicle is what Matching offers trips for, so changing it needs a new look:
//   - an APPROVED driver who changes the plate or the vehicle type goes back to PENDING and a
//     driver.application.reviewed with status SUSPENDED (reviewedBy "system") is queued in the
//     same transaction, so Matching stops offering them trips with a vehicle nobody approved.
//     They finish a trip in progress. An admin approves again (PENDING to APPROVED);
//   - model and colour are descriptions, not identity: changing them changes nothing else;
//   - for any other status the status stays: a driver cannot edit their way out of REJECTED
//     or SUSPENDED (only a new application or an admin does that).
//
// The second result says whether the vehicle change took the driver out of service.
func (s *service) saveDriver(ctx context.Context, id string, in driverInput) (driverRow, bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return driverRow{}, false, err
	}
	defer tx.Rollback(ctx)

	now := s.now().UTC().Truncate(time.Microsecond)
	d, err := lockDriver(ctx, tx, id, now)
	if err != nil {
		return d, false, err
	}
	if d.Status != statusNone {
		// Past the application the vehicle can be changed but not removed.
		var missing []string
		if in.Vehicle == "" {
			missing = append(missing, "vehicleType")
		}
		if in.Plate == "" {
			missing = append(missing, "plate")
		}
		if len(missing) > 0 {
			return d, false, &incompleteError{missing}
		}
	}

	changedVehicle := d.Status == statusApproved && (in.Plate != d.Plate || in.Vehicle != d.Vehicle)
	d.Name, d.Phone, d.Plate, d.Model, d.Color, d.Vehicle = in.Name, in.Phone, in.Plate, in.Model, in.Color, in.Vehicle
	d.UpdatedAt = now
	if changedVehicle {
		d.Status, d.Reason, d.ReviewedBy, d.StatusAt = statusPending, vehicleChangedReason, systemReviewer, now
		at := nextReviewTime(d.ReviewedAt, now)
		d.ReviewedAt = &at
		if err := s.queueReview(ctx, tx, d, events.ReviewSuspended, at); err != nil {
			return d, false, err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE driver_profiles
		SET name = $2, phone = $3, vehicle_type = $4, plate = $5, model = $6, color = $7,
		    status = $8, reason = $9, reviewed_by = $10, reviewed_at = $11, status_at = $12, updated_at = $13
		WHERE driver_id = $1`,
		d.ID, d.Name, d.Phone, string(d.Vehicle), d.Plate, d.Model, d.Color,
		d.Status, d.Reason, d.ReviewedBy, d.ReviewedAt, d.StatusAt, d.UpdatedAt); err != nil {
		return d, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return d, false, err
	}
	if changedVehicle {
		reviews.WithLabelValues("vehicle_changed").Inc()
		s.wake()
	}
	return d, changedVehicle, nil
}

func (s *service) wake() {
	if s.relay != nil {
		s.relay.Wake()
	}
}

// nextReviewTime is the time of a new review result: now, but always later than the previous
// one of the same driver, so a consumer can tell which is newer by the time alone even if
// the clock stepped back or two results fall in one microsecond.
func nextReviewTime(last *time.Time, now time.Time) time.Time {
	if last != nil && !now.After(*last) {
		return last.Add(time.Microsecond)
	}
	return now
}

// queueReview adds driver.application.reviewed to the outbox in tx. d carries the driver's
// vehicle, reason and reviewer as they will be after the change.
func (s *service) queueReview(ctx context.Context, tx pgx.Tx, d driverRow, status string, at time.Time) error {
	payload, err := s.enc.Encode(ctx, events.TopicDriverApplicationReviewed, events.DriverApplicationReviewed{
		Envelope:    events.Envelope{EventID: uuid.NewString(), OccurredAt: at.Format(time.RFC3339Nano)},
		DriverID:    d.ID,
		Status:      status,
		VehicleType: d.Vehicle,
		Reason:      d.Reason,
		ReviewedBy:  d.ReviewedBy,
	})
	if err != nil {
		return fmt.Errorf("encoding the review result: %w", err)
	}
	return outbox.Add(ctx, tx, outboxSource, events.TopicDriverApplicationReviewed, []byte(d.ID), payload)
}

// apply submits the driver's stored profile for review.
//
//   - NONE or REJECTED: becomes PENDING (a new application; the old reason is cleared). The
//     name, vehicle type and plate must be filled in (incompleteError).
//   - PENDING or APPROVED: nothing changes and it is not an error, so a retry is safe and an
//     approved driver cannot drop back to PENDING by pressing the button twice.
//   - SUSPENDED: errSuspended. Only an admin lifts a suspension.
//
// No event: PENDING is not a result, and a REJECTED driver is already out of service. The
// second result is false for a repeat.
func (s *service) apply(ctx context.Context, id string) (driverRow, bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return driverRow{}, false, err
	}
	defer tx.Rollback(ctx)

	d, err := loadDriver(ctx, tx, id, true)
	if errors.Is(err, errNoDriver) {
		return d, false, errNoProfile
	}
	if err != nil {
		return d, false, err
	}
	switch d.Status {
	case statusPending, statusApproved:
		return d, false, nil
	case statusSuspended:
		return d, false, errSuspended
	}
	var missing []string
	if d.Name == "" {
		missing = append(missing, "name")
	}
	if d.Vehicle == "" {
		missing = append(missing, "vehicleType")
	}
	if d.Plate == "" {
		missing = append(missing, "plate")
	}
	if len(missing) > 0 {
		return d, false, &incompleteError{missing}
	}
	now := s.now().UTC().Truncate(time.Microsecond)
	d.Status, d.Reason, d.StatusAt, d.UpdatedAt = statusPending, "", now, now
	if _, err := tx.Exec(ctx, `
		UPDATE driver_profiles SET status = $2, reason = '', status_at = $3, updated_at = $3 WHERE driver_id = $1`,
		id, d.Status, now); err != nil {
		return d, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return d, false, err
	}
	return d, true, nil
}

// A review action and the statuses it may start from. Anything else is a conflict: a repeat
// of an action already taken is one too (the response says which status the driver has).
//
//	approve  PENDING or SUSPENDED -> APPROVED
//	reject   PENDING              -> REJECTED   (REJECTED to PENDING is the driver's new application)
//	suspend  APPROVED             -> SUSPENDED
var reviewActions = map[string]struct {
	from   []string
	to     string
	metric string
	// reason must be given for this action.
	needsReason bool
}{
	"approve": {[]string{statusPending, statusSuspended}, statusApproved, "approved", false},
	"reject":  {[]string{statusPending}, statusRejected, "rejected", true},
	"suspend": {[]string{statusApproved}, statusSuspended, "suspended", true},
}

// review applies an admin's decision and queues driver.application.reviewed in the same
// transaction; the result's time is later than the driver's previous result.
func (s *service) review(ctx context.Context, id, action, admin, reason string) (driverRow, error) {
	spec := reviewActions[action]
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return driverRow{}, err
	}
	defer tx.Rollback(ctx)

	d, err := loadDriver(ctx, tx, id, true)
	if err != nil {
		return d, err // errNoDriver: the driver has no profile
	}
	allowed := false
	for _, from := range spec.from {
		allowed = allowed || d.Status == from
	}
	if !allowed {
		return d, &transitionError{from: d.Status, action: spec.to}
	}

	now := s.now().UTC().Truncate(time.Microsecond)
	at := nextReviewTime(d.ReviewedAt, now)
	d.Status, d.Reason, d.ReviewedBy, d.ReviewedAt, d.StatusAt, d.UpdatedAt = spec.to, reason, admin, &at, now, now
	if _, err := tx.Exec(ctx, `
		UPDATE driver_profiles
		SET status = $2, reason = $3, reviewed_by = $4, reviewed_at = $5, status_at = $6, updated_at = $6
		WHERE driver_id = $1`, d.ID, d.Status, d.Reason, d.ReviewedBy, at, now); err != nil {
		return d, err
	}
	if err := s.queueReview(ctx, tx, d, spec.to, at); err != nil {
		return d, err
	}
	if err := tx.Commit(ctx); err != nil {
		return d, err
	}
	reviews.WithLabelValues(spec.metric).Inc()
	s.wake()
	return d, nil
}

// riderRow is a rider_profiles row. PERSONAL DATA.
type riderRow struct {
	ID        string
	Name      string
	Phone     string
	UpdatedAt time.Time
}

func (s *service) saveRider(ctx context.Context, id, name, phone string) (riderRow, error) {
	r := riderRow{ID: id, Name: name, Phone: phone, UpdatedAt: s.now().UTC().Truncate(time.Microsecond)}
	_, err := s.db.Exec(ctx, `
		INSERT INTO rider_profiles (rider_id, name, phone, updated_at) VALUES ($1, $2, $3, $4)
		ON CONFLICT (rider_id) DO UPDATE SET name = EXCLUDED.name, phone = EXCLUDED.phone, updated_at = EXCLUDED.updated_at`,
		r.ID, r.Name, r.Phone, r.UpdatedAt)
	return r, err
}

var errNoRider = errors.New("no such rider")

func (s *service) loadRider(ctx context.Context, id string) (riderRow, error) {
	r := riderRow{ID: id}
	err := s.db.QueryRow(ctx, `SELECT name, phone, updated_at FROM rider_profiles WHERE rider_id = $1`, id).Scan(&r.Name, &r.Phone, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, errNoRider
	}
	r.UpdatedAt = r.UpdatedAt.UTC()
	return r, err
}

// listDrivers returns up to limit drivers with the given status, oldest status change first,
// after the cursor position (zero values: from the start).
func (s *service) listDrivers(ctx context.Context, status string, limit int, afterAt time.Time, afterID string, hasCursor bool) ([]driverRow, error) {
	args := []any{status, limit}
	where := ""
	if hasCursor {
		where = ` AND (status_at, driver_id) > ($3, $4)`
		args = append(args, afterAt, afterID)
	}
	rows, err := s.db.Query(ctx, `
		SELECT `+driverColumns+` FROM driver_profiles WHERE status = $1`+where+`
		ORDER BY status_at, driver_id LIMIT $2`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []driverRow
	for rows.Next() {
		d, err := scanDriver(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
