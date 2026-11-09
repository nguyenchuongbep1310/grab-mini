package kafka

import (
	"context"
	"time"

	"github.com/google/uuid"

	"grab-mini/order-service/internal/app"
	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
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

func NewEncoder(codec Codec) *Encoder { return &Encoder{codec: codec} }

var _ app.Encoder = (*Encoder)(nil)

// envelope has nanosecond precision: Matching measures how long a rider
// waited from OccurredAt, and whole seconds would make that figure wrong by
// up to a second.
func envelope() events.Envelope {
	return events.Envelope{EventID: uuid.NewString(), OccurredAt: time.Now().UTC().Format(time.RFC3339Nano)}
}

func (e *Encoder) OrderCreated(ctx context.Context, o app.OrderCreated) ([]byte, error) {
	return e.codec.Encode(ctx, events.TopicOrderCreated, events.OrderCreated{
		Envelope:        envelope(),
		OrderID:         o.OrderID,
		RiderID:         o.RiderID,
		Pickup:          events.LatLng{Lat: o.Pickup.Lat, Lng: o.Pickup.Lng},
		Dropoff:         events.LatLng{Lat: o.Dropoff.Lat, Lng: o.Dropoff.Lng},
		VehicleType:     events.VehicleType(o.VehicleType),
		SurgeMultiplier: o.SurgeMultiplier,
		Price:           o.Price,
		PromoCode:       o.PromoCode,
		Discount:        o.Discount,
	})
}

func (e *Encoder) OrderCancelled(ctx context.Context, o app.OrderCancelled) ([]byte, error) {
	return e.codec.Encode(ctx, events.TopicOrderCancelled, events.OrderCancelled{
		Envelope:    envelope(),
		OrderID:     o.OrderID,
		RiderID:     o.RiderID,
		CancelledBy: o.CancelledBy,
		Reason:      o.Reason,
	})
}
