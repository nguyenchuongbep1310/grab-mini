package domain

import "time"

// Driver is a driver_profiles row. PERSONAL DATA: never log one.
type Driver struct {
	ID         string
	Name       string
	Phone      string
	Vehicle    VehicleType
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

// ProfileInput is what PUT /drivers/{id} sets, already cleaned.
type ProfileInput struct {
	Name, Phone, Plate, Model, Color string
	Vehicle                          VehicleType
}

// ProfileChange says what an edit did beyond replacing fields.
type ProfileChange struct {
	// VehicleChanged: an APPROVED driver changed the plate or the vehicle type and went back
	// to PENDING; At is the time of the result to publish (SUSPENDED, reviewedBy "system").
	VehicleChanged bool
	At             time.Time
}

// ApplyProfile replaces the editable fields of a driver's profile.
//
// The vehicle is what Matching offers trips for, so changing it needs a new look:
//   - an APPROVED driver who changes the plate or the vehicle type goes back to PENDING;
//   - model and colour are descriptions, not identity: changing them changes nothing else;
//   - for any other status the status stays: a driver cannot edit their way out of REJECTED
//     or SUSPENDED (only a new application or an admin does that).
func ApplyProfile(d Driver, in ProfileInput, now time.Time) (Driver, ProfileChange, error) {
	if d.Status != StatusNone {
		// Past the application the vehicle can be changed but not removed.
		var missing []string
		if in.Vehicle == "" {
			missing = append(missing, "vehicleType")
		}
		if in.Plate == "" {
			missing = append(missing, "plate")
		}
		if len(missing) > 0 {
			return d, ProfileChange{}, &IncompleteError{missing}
		}
	}

	changedVehicle := d.Status == StatusApproved && (in.Plate != d.Plate || in.Vehicle != d.Vehicle)
	d.Name, d.Phone, d.Plate, d.Model, d.Color, d.Vehicle = in.Name, in.Phone, in.Plate, in.Model, in.Color, in.Vehicle
	d.UpdatedAt = now
	var ch ProfileChange
	if changedVehicle {
		d.Status, d.Reason, d.ReviewedBy, d.StatusAt = StatusPending, VehicleChangedReason, SystemReviewer, now
		at := NextReviewTime(d.ReviewedAt, now)
		d.ReviewedAt = &at
		ch = ProfileChange{VehicleChanged: true, At: at}
	}
	return d, ch, nil
}

// Submit submits the driver's stored profile for review.
//
//   - NONE or REJECTED: becomes PENDING (a new application; the old reason is cleared). The
//     name, vehicle type and plate must be filled in (IncompleteError).
//   - PENDING or APPROVED: nothing changes and it is not an error, so a retry is safe and an
//     approved driver cannot drop back to PENDING by pressing the button twice.
//   - SUSPENDED: ErrSuspended. Only an admin lifts a suspension.
//
// The second result is false for a repeat.
func Submit(d Driver, now time.Time) (Driver, bool, error) {
	switch d.Status {
	case StatusPending, StatusApproved:
		return d, false, nil
	case StatusSuspended:
		return d, false, ErrSuspended
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
		return d, false, &IncompleteError{missing}
	}
	d.Status, d.Reason, d.StatusAt, d.UpdatedAt = StatusPending, "", now, now
	return d, true, nil
}
