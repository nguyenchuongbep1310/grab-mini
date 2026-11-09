package domain

import "errors"

// The two ways a booking can be malformed. The delivery layer words them.
var (
	ErrBadCoordinates = errors.New("pickup or dropoff coordinates are invalid")
	ErrBadVehicleType = errors.New("vehicle type is invalid")
)

// BookingRequest is what a rider asks for.
type BookingRequest struct {
	Pickup, Dropoff *LatLng
	VehicleType     VehicleType
	// QuoteID locks the price of an earlier quote; PromoToken a discount.
	QuoteID, PromoToken string
}

// Normalize validates the request and defaults the vehicle type to CAR. The
// coordinates are checked before the vehicle type.
func (r BookingRequest) Normalize() (BookingRequest, error) {
	if !ValidLatLng(r.Pickup) || !ValidLatLng(r.Dropoff) {
		return r, ErrBadCoordinates
	}
	if r.VehicleType == "" {
		r.VehicleType = VehicleCar
	}
	if !r.VehicleType.Valid() {
		return r, ErrBadVehicleType
	}
	return r, nil
}
