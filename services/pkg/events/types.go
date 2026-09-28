package events

// Envelope carries the fields every event on the bus has, used for
// idempotency and tracing (see requirements US-6.1).
type Envelope struct {
	EventID    string `json:"eventId"`
	OccurredAt string `json:"occurredAt"`
}

type OrderStatus string

const (
	OrderStatusPending  OrderStatus = "PENDING"
	OrderStatusMatched  OrderStatus = "MATCHED"
	OrderStatusNoDriver OrderStatus = "NO_DRIVER"
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

type LatLng struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type OrderCreated struct {
	Envelope
	OrderID string `json:"orderId"`
	RiderID string `json:"riderId"`
	Pickup  LatLng `json:"pickup"`
	Dropoff LatLng `json:"dropoff"`
}

type OrderMatched struct {
	Envelope
	OrderID  string `json:"orderId"`
	DriverID string `json:"driverId"`
}

type OrderNoDriverFound struct {
	Envelope
	OrderID string `json:"orderId"`
}

// DriverLocationUpdated carries the driver's device timestamp, used to
// discard out-of-order updates (US-3.1).
type DriverLocationUpdated struct {
	Envelope
	DriverID  string  `json:"driverId"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	Timestamp string  `json:"timestamp"`
}

type TripStatusChanged struct {
	Envelope
	TripID   string     `json:"tripId"`
	OrderID  string     `json:"orderId"`
	DriverID string     `json:"driverId"`
	Status   TripStatus `json:"status"`
}
