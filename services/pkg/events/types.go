package events

// Envelope carries the fields every event on the bus has, used for
// idempotency and tracing (see requirements US-6.1).
type Envelope struct {
	EventID    string `json:"eventId" avro:"eventId"`
	OccurredAt string `json:"occurredAt" avro:"occurredAt"`
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
	Lat float64 `json:"lat" avro:"lat"`
	Lng float64 `json:"lng" avro:"lng"`
}

type OrderCreated struct {
	Envelope
	OrderID string `json:"orderId" avro:"orderId"`
	RiderID string `json:"riderId" avro:"riderId"`
	Pickup  LatLng `json:"pickup" avro:"pickup"`
	Dropoff LatLng `json:"dropoff" avro:"dropoff"`
}

type OrderMatched struct {
	Envelope
	OrderID  string `json:"orderId" avro:"orderId"`
	DriverID string `json:"driverId" avro:"driverId"`
}

type OrderNoDriverFound struct {
	Envelope
	OrderID string `json:"orderId" avro:"orderId"`
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
}
