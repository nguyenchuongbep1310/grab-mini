package app

import "grab-mini/trip-service/internal/domain"

// Plain inputs of the use cases. The Kafka adapter maps each event to one of
// these; no wire type reaches the app.

// OrderMatched is order.matched: a driver was claimed for the order.
type OrderMatched struct {
	EventID  string
	OrderID  string
	DriverID string
	RiderID  string
	// Pickup and Dropoff are nil when the event carries no route.
	Pickup, Dropoff *domain.LatLng
}

// OrderCancelled is order.cancelled: the rider cancelled the order.
type OrderCancelled struct {
	OrderID string
	// CancelledBy is RIDER or DRIVER; empty on older messages.
	CancelledBy string
	Reason      string
}

// TripEnded is a trip.status.changed event, as the driver release sees it.
type TripEnded struct {
	TripID   string
	DriverID string
	Status   domain.Status
}

// StatusChanged is the announcement queued with every status change.
type StatusChanged struct {
	TripID   string
	OrderID  string
	DriverID string
	RiderID  string
	Status   domain.Status
	// CancelledBy and Reason are set on CANCELLED.
	CancelledBy string
	Reason      string
}

// TripRated is the announcement queued with every rating.
type TripRated struct {
	TripID    string
	OrderID   string
	RiderID   string
	DriverID  string
	Stars     int
	Comment   string
	TipAmount int64
	Currency  string
}

// RateInput is a rider's rating request. RiderID is optional: when given it
// must be the caller.
type RateInput struct {
	RiderID string
	Stars   int
	Comment string
	Tip     int64
}

// Rated is the outcome of a saved rating.
type Rated struct {
	TripID string
	Stars  int
	Tip    int64
}
