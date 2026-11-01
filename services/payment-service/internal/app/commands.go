package app

// The inputs of the use cases, as plain values. They mirror the fields the
// service reads from the events it consumes, but they are this package's own
// types: the Kafka adapter maps each event to one of them, so a change to a wire
// schema stays out of the use cases.

// OrderCreated is the price locked at booking and the discount a promo code
// took off it.
type OrderCreated struct {
	OrderID  string
	RiderID  string
	Price    *float64 // nil: the order has no price
	Discount int64
}

// OrderMatched is the moment an order got its driver (OccurredAt is RFC 3339).
type OrderMatched struct {
	OrderID    string
	OccurredAt string
}

// Trip statuses the service reacts to (the others settle nothing).
const (
	TripCompleted = "COMPLETED"
	TripCancelled = "CANCELLED"
)

// TripStatusChanged is a trip reaching a new status.
type TripStatusChanged struct {
	EventID     string
	TripID      string
	OrderID     string
	DriverID    string
	RiderID     string
	Status      string
	CancelledBy string // domain.CancelledByRider or domain.CancelledByDriver
	OccurredAt  string // RFC 3339
}

// TripRated is a rating, with the tip that came with it.
type TripRated struct {
	EventID   string
	TripID    string
	OrderID   string
	DriverID  string
	RiderID   string
	TipAmount int64
	Currency  string
}

// PaymentRecorded is the announcement of one settled charge (payment.recorded).
// For a captured payment DriverAmount + PlatformAmount == Amount; for a declined
// one both are 0 and there is no ledger transaction.
type PaymentRecorded struct {
	EventID             string
	OccurredAt          string
	PaymentID           string
	TripID              string
	OrderID             string
	RiderID             string
	DriverID            string
	Kind                string
	Status              string
	Amount              int64
	DriverAmount        int64
	PlatformAmount      int64
	Currency            string
	LedgerTransactionID string
}
