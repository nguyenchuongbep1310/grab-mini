package kafka

import (
	"context"

	"grab-mini/payment-service/internal/app"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
)

// Codec turns an event into its Avro wire bytes (*eventcodec.Codec).
type Codec interface {
	Encode(ctx context.Context, topic string, v any) ([]byte, error)
}

var _ Codec = (*eventcodec.Codec)(nil)

// Encoder implements app.PaymentEncoder: it maps the announcement to the
// payment.recorded wire type and encodes it for that topic.
type Encoder struct {
	codec Codec
}

func NewEncoder(codec Codec) Encoder { return Encoder{codec: codec} }

var _ app.PaymentEncoder = Encoder{}

func (e Encoder) EncodePaymentRecorded(ctx context.Context, rec app.PaymentRecorded) ([]byte, error) {
	return e.codec.Encode(ctx, events.TopicPaymentRecorded, events.PaymentRecorded{
		Envelope:  events.Envelope{EventID: rec.EventID, OccurredAt: rec.OccurredAt},
		PaymentID: rec.PaymentID, TripID: rec.TripID, OrderID: rec.OrderID, RiderID: rec.RiderID, DriverID: rec.DriverID,
		Kind: rec.Kind, Status: rec.Status, Amount: rec.Amount, DriverAmount: rec.DriverAmount,
		PlatformAmount: rec.PlatformAmount, Currency: rec.Currency, LedgerTransactionID: rec.LedgerTransactionID,
	})
}
