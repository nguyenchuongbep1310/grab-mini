// Package domain holds order-service's business rules: the order, who may
// read it, how a booking is validated and fingerprinted, and how a cancel
// reason is checked. It imports the standard library only.
package domain

import (
	"errors"
	"math"
	"time"
)

// Status is where an order is in its life.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusMatched   Status = "MATCHED"
	StatusNoDriver  Status = "NO_DRIVER"
	StatusCancelled Status = "CANCELLED"
)

// VehicleType is what the rider asks for.
type VehicleType string

const (
	VehicleCar  VehicleType = "CAR"
	VehicleBike VehicleType = "BIKE"
)

// Valid reports whether v is a vehicle type the service takes.
func (v VehicleType) Valid() bool { return v == VehicleCar || v == VehicleBike }

// LatLng is a point on the map, in degrees.
type LatLng struct{ Lat, Lng float64 }

// ValidLatLng reports whether p is present and within |lat| <= 90, |lng| <= 180.
func ValidLatLng(p *LatLng) bool {
	return p != nil && math.Abs(p.Lat) <= 90 && math.Abs(p.Lng) <= 180
}

// ErrOrderNotFound is returned when no order matches (or the caller may not see it).
var ErrOrderNotFound = errors.New("order not found")

// Order is a booked ride.
type Order struct {
	ID, RiderID     string
	Pickup, Dropoff LatLng
	VehicleType     VehicleType
	Status          Status
	CancelledAt     *time.Time
	CancelledBy     string
	CancelReason    string
	SurgeMultiplier float64
	// Price is what the ride costs (VND) before the promo; nil when none was settled.
	Price     *float64
	PromoCode string
	Discount  int64
	// DriverID is set once a driver is matched.
	DriverID string
}

// FinalPrice is what the rider pays: the price less the discount, never
// below zero. Nil when the order has no price.
func (o Order) FinalPrice() *float64 {
	if o.Price == nil {
		return nil
	}
	final := max(*o.Price-float64(o.Discount), 0)
	return &final
}
