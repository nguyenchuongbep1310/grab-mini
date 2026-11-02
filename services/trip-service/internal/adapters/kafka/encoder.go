package kafka

import (
	"context"
	"time"

	"github.com/google/uuid"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/trip-service/internal/app"
)

// Codec turns an event into its Avro wire bytes (*eventcodec.Codec).
type Codec interface {
	Encode(ctx context.Context, topic string, v any) ([]byte, error)
}

var _ Codec = (*eventcodec.Codec)(nil)

// Encoder implements app.Encoder: it maps each announcement to its wire type
// (with a new eventId and the time now) and encodes it for its topic.
type Encoder struct {
	codec Codec
}

func NewEncoder(codec Codec) Encoder { return Encoder{codec: codec} }

var _ app.Encoder = Encoder{}

func envelope() events.Envelope {
	return events.Envelope{EventID: uuid.NewString(), OccurredAt: time.Now().UTC().Format(time.RFC3339)}
}

func (e Encoder) EncodeStatusChanged(ctx context.Context, s app.StatusChanged) ([]byte, error) {
	return e.codec.Encode(ctx, events.TopicTripStatusChanged, events.TripStatusChanged{
		Envelope:    envelope(),
		TripID:      s.TripID,
		OrderID:     s.OrderID,
		DriverID:    s.DriverID,
		Status:      events.TripStatus(s.Status),
		RiderID:     s.RiderID,
		CancelledBy: s.CancelledBy,
		Reason:      s.Reason,
	})
}

func (e Encoder) EncodeTripRated(ctx context.Context, r app.TripRated) ([]byte, error) {
	return e.codec.Encode(ctx, events.TopicTripRated, events.TripRated{
		Envelope:  envelope(),
		TripID:    r.TripID,
		OrderID:   r.OrderID,
		RiderID:   r.RiderID,
		DriverID:  r.DriverID,
		Stars:     r.Stars,
		Comment:   r.Comment,
		TipAmount: r.TipAmount,
		Currency:  r.Currency,
	})
}
