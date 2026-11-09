package app

// RedeemOrderCommand is an order.created as the use case needs it.
type RedeemOrderCommand struct {
	EventID, OrderID, RiderID, PromoCode string
	Discount                             int64
}

// ReleaseCommand is one of the events that end an order without a ride.
// Status is read only by OnTripStatusChanged.
type ReleaseCommand struct {
	EventID, OrderID, Status string
}
