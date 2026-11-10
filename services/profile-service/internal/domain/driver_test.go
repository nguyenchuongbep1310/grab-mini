package domain

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func approvedDriver() Driver {
	reviewed := mustTime("2026-01-01T00:00:00Z")
	return Driver{ID: "d1", Name: "An", Phone: "0901234567", Vehicle: VehicleCar, Plate: "51A-123.45", Model: "Vios", Color: "white",
		Status: StatusApproved, Reason: "ok", ReviewedBy: "admin-1", ReviewedAt: &reviewed,
		StatusAt: reviewed, UpdatedAt: reviewed}
}

func inputOf(d Driver) ProfileInput {
	return ProfileInput{Name: d.Name, Phone: d.Phone, Plate: d.Plate, Model: d.Model, Color: d.Color, Vehicle: d.Vehicle}
}

func TestApplyProfileTakesAnApprovedDriverOutOfServiceOnAPlateOrVehicleChange(t *testing.T) {
	now := mustTime("2026-02-03T04:05:06Z")
	for name, edit := range map[string]func(*ProfileInput){
		"plate":   func(in *ProfileInput) { in.Plate = "30E-999.99" },
		"vehicle": func(in *ProfileInput) { in.Vehicle = VehicleBike },
	} {
		d := approvedDriver()
		in := inputOf(d)
		edit(&in)
		got, ch, err := ApplyProfile(d, in, now)
		if err != nil || !ch.VehicleChanged || !ch.At.Equal(now) {
			t.Fatalf("%s: %+v %v", name, ch, err)
		}
		if got.Status != StatusPending || got.Reason != VehicleChangedReason || got.ReviewedBy != SystemReviewer ||
			!got.StatusAt.Equal(now) || !got.UpdatedAt.Equal(now) || got.ReviewedAt == nil || !got.ReviewedAt.Equal(ch.At) {
			t.Errorf("%s: %+v", name, got)
		}
		if got.Plate != in.Plate || got.Vehicle != in.Vehicle {
			t.Errorf("%s: fields not copied: %+v", name, got)
		}
	}

	// A previous result at or after now: the new one is still later.
	d := approvedDriver()
	later := now.Add(time.Minute)
	d.ReviewedAt = &later
	in := inputOf(d)
	in.Plate = "30E-999.99"
	if _, ch, _ := ApplyProfile(d, in, now); !ch.At.After(later) {
		t.Errorf("At %v not after %v", ch.At, later)
	}

	// Model and colour: nothing else moves.
	d = approvedDriver()
	in = inputOf(d)
	in.Model, in.Color, in.Name, in.Phone = "Camry", "black", "Binh", ""
	got, ch, err := ApplyProfile(d, in, now)
	if err != nil || ch.VehicleChanged {
		t.Fatalf("%+v %v", ch, err)
	}
	want := d
	want.Model, want.Color, want.Name, want.Phone, want.UpdatedAt = "Camry", "black", "Binh", "", now
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}

	// Other statuses never move by editing.
	for _, st := range []string{StatusPending, StatusRejected, StatusSuspended} {
		d := approvedDriver()
		d.Status = st
		in := inputOf(d)
		in.Plate, in.Vehicle = "30E-999.99", VehicleBike
		got, ch, err := ApplyProfile(d, in, now)
		if err != nil || ch.VehicleChanged || got.Status != st || got.Reason != d.Reason || got.ReviewedBy != d.ReviewedBy ||
			!got.StatusAt.Equal(d.StatusAt) || got.Plate != "30E-999.99" || !got.UpdatedAt.Equal(now) {
			t.Errorf("%s: %+v %+v %v", st, got, ch, err)
		}
	}
}

func TestApplyProfileRefusesAnIncompleteProfileOnlyPastNone(t *testing.T) {
	now := mustTime("2026-02-03T04:05:06Z")
	for _, st := range []string{StatusPending, StatusApproved, StatusRejected, StatusSuspended} {
		for name, c := range map[string]struct {
			in      ProfileInput
			missing []string
		}{
			"both":    {ProfileInput{Name: "X"}, []string{"vehicleType", "plate"}},
			"vehicle": {ProfileInput{Name: "X", Plate: "51A-123.45"}, []string{"vehicleType"}},
			"plate":   {ProfileInput{Name: "X", Vehicle: VehicleCar}, []string{"plate"}},
		} {
			d := approvedDriver()
			d.Status = st
			got, ch, err := ApplyProfile(d, c.in, now)
			var ie *IncompleteError
			if !errors.As(err, &ie) || !reflect.DeepEqual(ie.Missing, c.missing) {
				t.Errorf("%s/%s: %v", st, name, err)
			}
			if !reflect.DeepEqual(got, d) || ch.VehicleChanged {
				t.Errorf("%s/%s: mutated: %+v", st, name, got)
			}
		}
	}
	// NONE may save a partial profile.
	got, _, err := ApplyProfile(Driver{ID: "d1", Status: StatusNone}, ProfileInput{Name: "X"}, now)
	if err != nil || got.Name != "X" || got.Status != StatusNone || !got.UpdatedAt.Equal(now) {
		t.Errorf("%+v %v", got, err)
	}
	var ie *IncompleteError
	_, _, err = ApplyProfile(approvedDriver(), ProfileInput{}, now)
	if !errors.As(err, &ie) || err.Error() != "profile is missing [vehicleType plate]" {
		t.Errorf("message: %v", err)
	}
}

func TestSubmitTable(t *testing.T) {
	now := mustTime("2026-02-03T04:05:06Z")
	full := Driver{ID: "d1", Name: "An", Vehicle: VehicleBike, Plate: "51A-123.45", Reason: "old reason"}

	for _, st := range []string{StatusNone, StatusRejected} {
		d := full
		d.Status = st
		got, changed, err := Submit(d, now)
		if err != nil || !changed || got.Status != StatusPending || got.Reason != "" || !got.StatusAt.Equal(now) || !got.UpdatedAt.Equal(now) {
			t.Errorf("%s: %+v %v %v", st, got, changed, err)
		}
	}
	for _, st := range []string{StatusPending, StatusApproved} {
		d := Driver{ID: "d1", Status: st} // incomplete, and still fine
		got, changed, err := Submit(d, now)
		if err != nil || changed || !reflect.DeepEqual(got, d) {
			t.Errorf("%s: %+v %v %v", st, got, changed, err)
		}
	}
	if _, changed, err := Submit(Driver{Status: StatusSuspended, Name: "An"}, now); !errors.Is(err, ErrSuspended) || changed {
		t.Errorf("suspended: %v %v", changed, err)
	}

	for _, st := range []string{StatusNone, StatusRejected} {
		for i, want := range [][]string{
			{"name", "vehicleType", "plate"}, {"name", "vehicleType"}, {"name", "plate"}, {"name"},
			{"vehicleType", "plate"}, {"vehicleType"}, {"plate"},
		} {
			d := Driver{Status: st, Name: "An", Vehicle: VehicleCar, Plate: "ABC 123"}
			for _, f := range want {
				switch f {
				case "name":
					d.Name = ""
				case "vehicleType":
					d.Vehicle = ""
				case "plate":
					d.Plate = ""
				}
			}
			got, changed, err := Submit(d, now)
			var ie *IncompleteError
			if !errors.As(err, &ie) || !reflect.DeepEqual(ie.Missing, want) || changed || !reflect.DeepEqual(got, d) {
				t.Errorf("%s case %d: %v %v", st, i, err, ie)
			}
		}
	}
}
