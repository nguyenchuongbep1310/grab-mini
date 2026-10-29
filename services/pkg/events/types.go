package events

// Envelope carries the fields every event on the bus has, used for
// idempotency and tracing (see requirements US-6.1).
type Envelope struct {
	EventID    string `json:"eventId" avro:"eventId"`
	OccurredAt string `json:"occurredAt" avro:"occurredAt"`
}

type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "PENDING"
	OrderStatusMatched   OrderStatus = "MATCHED"
	OrderStatusNoDriver  OrderStatus = "NO_DRIVER"
	OrderStatusCancelled OrderStatus = "CANCELLED"
)

type TripStatus string

const (
	TripStatusAssigned   TripStatus = "ASSIGNED"
	TripStatusInProgress TripStatus = "IN_PROGRESS"
	TripStatusCompleted  TripStatus = "COMPLETED"
	TripStatusCancelled  TripStatus = "CANCELLED"
)

type DriverStatus string

const (
	DriverStatusAvailable DriverStatus = "AVAILABLE"
	DriverStatusAssigned  DriverStatus = "ASSIGNED"
	DriverStatusOnTrip    DriverStatus = "ON_TRIP"
)

// VehicleType is what a rider asks for and what a driver drives.
type VehicleType string

const (
	VehicleCar  VehicleType = "CAR"
	VehicleBike VehicleType = "BIKE"
)

func (v VehicleType) Valid() bool { return v == VehicleCar || v == VehicleBike }

type LatLng struct {
	Lat float64 `json:"lat" avro:"lat"`
	Lng float64 `json:"lng" avro:"lng"`
}

type OrderCreated struct {
	Envelope
	OrderID string `json:"orderId" avro:"orderId"`
	RiderID string `json:"riderId" avro:"riderId"`
	Pickup  LatLng `json:"pickup" avro:"pickup"`
	Dropoff LatLng `json:"dropoff" avro:"dropoff"`
	// Added in schema v2 (default CAR), so older messages still decode.
	VehicleType VehicleType `json:"vehicleType" avro:"vehicleType"`
	// Added in schema v3. SurgeMultiplier is 1.0 and Price nil on older
	// messages. Price is in VND, locked when the order was booked.
	SurgeMultiplier float64  `json:"surgeMultiplier" avro:"surgeMultiplier"`
	Price           *float64 `json:"price" avro:"price"`
}

type OrderMatched struct {
	Envelope
	OrderID  string `json:"orderId" avro:"orderId"`
	DriverID string `json:"driverId" avro:"driverId"`
	// Added in schema v2 (default CAR), so older messages still decode.
	VehicleType VehicleType `json:"vehicleType" avro:"vehicleType"`
	// Who to tell: lets Notification Service route the event without
	// remembering which rider made the order. Empty on older messages.
	RiderID string `json:"riderId" avro:"riderId"`
	// Added in schema v3 (default null): where the trip starts and ends, so
	// Trip Service can check the driver is there before pickup/complete.
	// Nil on older messages.
	Pickup  *LatLng `json:"pickup" avro:"pickup"`
	Dropoff *LatLng `json:"dropoff" avro:"dropoff"`
}

type OrderNoDriverFound struct {
	Envelope
	OrderID string `json:"orderId" avro:"orderId"`
	// Empty on older messages (see OrderMatched.RiderID).
	RiderID string `json:"riderId" avro:"riderId"`
}

// Who cancelled a ride (the cancelledBy field of order.cancelled and of
// trip.status.changed). Empty on events from before cancellation was recorded.
const (
	CancelledByRider  = "RIDER"
	CancelledByDriver = "DRIVER"
)

// OrderCancelled is published once, when a rider cancels an order that has
// no driver yet. Trip Service cancels any trip already created for it and
// Matching stops looking for a driver.
type OrderCancelled struct {
	Envelope
	OrderID string `json:"orderId" avro:"orderId"`
	RiderID string `json:"riderId" avro:"riderId"`
	// RIDER or DRIVER; empty on older messages.
	CancelledBy string `json:"cancelledBy" avro:"cancelledBy"`
	Reason      string `json:"reason" avro:"reason"`
}

// DriverLocationUpdated carries the driver's device timestamp, used to
// discard out-of-order updates (US-3.1).
type DriverLocationUpdated struct {
	Envelope
	DriverID  string  `json:"driverId" avro:"driverId"`
	Lat       float64 `json:"lat" avro:"lat"`
	Lng       float64 `json:"lng" avro:"lng"`
	Timestamp string  `json:"timestamp" avro:"timestamp"`
}

type TripStatusChanged struct {
	Envelope
	TripID   string     `json:"tripId" avro:"tripId"`
	OrderID  string     `json:"orderId" avro:"orderId"`
	DriverID string     `json:"driverId" avro:"driverId"`
	Status   TripStatus `json:"status" avro:"status"`
	// Empty on older messages (see OrderMatched.RiderID).
	RiderID string `json:"riderId" avro:"riderId"`
	// Set on CANCELLED: who cancelled (RIDER or DRIVER) and an optional
	// reason. Empty on older messages and on other statuses.
	CancelledBy string `json:"cancelledBy" avro:"cancelledBy"`
	Reason      string `json:"reason" avro:"reason"`
}

// TripRated is published once, when a rider rates a completed trip.
type TripRated struct {
	Envelope
	TripID   string `json:"tripId" avro:"tripId"`
	OrderID  string `json:"orderId" avro:"orderId"`
	RiderID  string `json:"riderId" avro:"riderId"`
	DriverID string `json:"driverId" avro:"driverId"`
	Stars    int    `json:"stars" avro:"stars"`
	Comment  string `json:"comment" avro:"comment"`
	// TipAmount is a recorded amount in whole currency units (VND has no
	// minor unit); no money moves, there is no payment system.
	TipAmount int64  `json:"tipAmount" avro:"tipAmount"`
	Currency  string `json:"currency" avro:"currency"`
}
