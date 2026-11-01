package domain

// Payment kinds and outcomes. The strings are the values carried on the wire by
// payment.recorded and stored in the payments table; the Kafka adapter's test
// pins them to services/pkg/events so the two cannot drift apart.
const (
	KindFare            = "FARE"
	KindTip             = "TIP"
	KindCancellationFee = "CANCELLATION_FEE"

	StatusCaptured = "CAPTURED"
	StatusDeclined = "DECLINED"
)

// Who cancelled a trip, as Trip Service records it.
const (
	CancelledByRider  = "RIDER"
	CancelledByDriver = "DRIVER"
)

// Payment is one settled charge: what was asked of the rider, how it was
// shared, and what the provider answered. There is at most one per (trip, kind).
type Payment struct {
	ID             string
	TripID         string
	OrderID        string
	RiderID        string
	DriverID       string
	Kind           string
	Status         string // StatusCaptured or StatusDeclined
	Amount         int64
	DriverAmount   int64 // 0 unless captured
	PlatformAmount int64 // 0 unless captured
	Currency       string
	Provider       string
	ProviderRef    string
	DeclineReason  string
	// LedgerTxnID is the ledger transaction that booked it; nil when declined.
	LedgerTxnID   *string
	SourceEventID string
}

// OrderInfo is what payment-service keeps of an order: who booked it, the price
// locked at booking and the discount a promo code took off it.
type OrderInfo struct {
	RiderID  string
	Price    *int64 // nil: the order has no price
	Discount int64  // taken off Price (0 for none)
}

// Net is what the rider pays for the ride: the price less the discount; nil when
// the order has no price.
func (o OrderInfo) Net() *int64 {
	if o.Price == nil {
		return nil
	}
	n := *o.Price - o.Discount
	return &n
}
