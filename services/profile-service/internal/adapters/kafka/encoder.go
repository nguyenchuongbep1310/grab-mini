// Package kafka holds the Avro-side adapter: the encoder of driver.application.reviewed.
// It holds no consumer because the service consumes nothing; the outbox relay publishes.
package kafka

import (
	"context"
	"time"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/profile-service/internal/app"
)

// Codec turns an event into its Avro wire bytes (*eventcodec.Codec).
type Codec interface {
	Encode(ctx context.Context, topic string, v any) ([]byte, error)
}

var _ Codec = (*eventcodec.Codec)(nil)

// Encoder implements app.Encoder: it stamps the envelope from the event and encodes
// driver.application.reviewed for its topic.
type Encoder struct {
	codec Codec
}

// NewEncoder returns an Encoder over codec.
func NewEncoder(codec Codec) *Encoder { return &Encoder{codec: codec} }

var _ app.Encoder = (*Encoder)(nil)

// Reviewed encodes e as driver.application.reviewed. OccurredAt is formatted from the time
// as given: the use case passes a UTC time, and no conversion is added here.
func (e *Encoder) Reviewed(ctx context.Context, evt app.ReviewedEvent) ([]byte, error) {
	return e.codec.Encode(ctx, events.TopicDriverApplicationReviewed, events.DriverApplicationReviewed{
		Envelope:    events.Envelope{EventID: evt.EventID, OccurredAt: evt.OccurredAt.Format(time.RFC3339Nano)},
		DriverID:    evt.DriverID,
		Status:      evt.Status,
		VehicleType: events.VehicleType(evt.Vehicle),
		Reason:      evt.Reason,
		ReviewedBy:  evt.ReviewedBy,
	})
}
