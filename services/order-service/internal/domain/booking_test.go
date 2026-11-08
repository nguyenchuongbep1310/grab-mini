package domain

import (
	"errors"
	"testing"
)

func TestNormalizeChecksCoordinatesBeforeTheVehicleType(t *testing.T) {
	good := &LatLng{10.776, 106.72}
	if _, err := (BookingRequest{Pickup: good, Dropoff: &LatLng{91, 0}, VehicleType: "BOAT"}).Normalize(); !errors.Is(err, ErrBadCoordinates) {
		t.Errorf("bad dropoff and bad vehicle: err = %v, want ErrBadCoordinates", err)
	}
	if _, err := (BookingRequest{Pickup: nil, Dropoff: good}).Normalize(); !errors.Is(err, ErrBadCoordinates) {
		t.Errorf("missing pickup: err = %v, want ErrBadCoordinates", err)
	}
	if _, err := (BookingRequest{Pickup: good, Dropoff: good, VehicleType: "car"}).Normalize(); !errors.Is(err, ErrBadVehicleType) {
		t.Errorf("lowercase vehicle: err = %v, want ErrBadVehicleType", err)
	}
	got, err := BookingRequest{Pickup: good, Dropoff: good}.Normalize()
	if err != nil || got.VehicleType != VehicleCar {
		t.Errorf("empty vehicle = %q, %v; want CAR", got.VehicleType, err)
	}
	if got, err := (BookingRequest{Pickup: good, Dropoff: good, VehicleType: VehicleBike}).Normalize(); err != nil || got.VehicleType != VehicleBike {
		t.Errorf("BIKE = %q, %v", got.VehicleType, err)
	}
}
