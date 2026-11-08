package app

import "grab-mini/order-service/internal/domain"

// CreateOrderCommand is a booking by RiderID. Request has been normalized
// (domain.BookingRequest.Normalize); IdempotencyKey is "" when the client sent none.
type CreateOrderCommand struct {
	RiderID, IdempotencyKey string
	Request                 domain.BookingRequest
}

// CreateOrderResult is the response to a booking: 201 and the receipt for a
// new order, or the stored status and bytes when Replayed.
type CreateOrderResult struct {
	Status   int
	Body     []byte
	Replayed bool
}

// CancelResult is the outcome of a rider's cancel. Conflict means the order
// could not be cancelled here (Message says why); Order is its current state.
type CancelResult struct {
	Order    domain.Order
	Conflict bool
	Message  string
}

// NewOrder is a PENDING order to insert. A zero Multiplier is stored as 1.0.
type NewOrder struct {
	ID, RiderID     string
	Pickup, Dropoff domain.LatLng
	VehicleType     domain.VehicleType
	Multiplier      float64
	Price           *float64
	PromoCode       string
	Discount        int64
}

// OrderCreated is the order.created event; the encoder adds the envelope.
type OrderCreated struct {
	OrderID, RiderID string
	Pickup, Dropoff  domain.LatLng
	VehicleType      domain.VehicleType
	SurgeMultiplier  float64
	Price            *float64
	PromoCode        string
	Discount         int64
}

// OrderCancelled is the order.cancelled event; the encoder adds the envelope.
type OrderCancelled struct {
	OrderID, RiderID, CancelledBy, Reason string
}

// Receipt is what a new booking answers. Its status is always PENDING and its
// currency surge.Currency; the ReceiptEncoder renders it.
type Receipt struct {
	OrderID                            string
	SurgeMultiplier, Price, FinalPrice float64
	Discount                           int64
	PromoCode                          string
}
