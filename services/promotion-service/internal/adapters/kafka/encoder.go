package kafka

import (
	"context"
	"time"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/promotion-service/internal/app"
)

// Codec turns an event into its Avro wire bytes (*eventcodec.Codec).
type Codec interface {
	Encode(ctx context.Context, topic string, v any) ([]byte, error)
}

var _ Codec = (*eventcodec.Codec)(nil)

// Encoder implements app.Encoder: it stamps the envelope from the event and
// encodes promotion.redeemed for its topic.
type Encoder struct {
	codec Codec
}

// NewEncoder returns an Encoder over codec.
func NewEncoder(codec Codec) *Encoder { return &Encoder{codec: codec} }

var _ app.Encoder = (*Encoder)(nil)

// Redeemed encodes evt as promotion.redeemed.
func (e *Encoder) Redeemed(ctx context.Context, evt app.RedeemedEvent) ([]byte, error) {
	return e.codec.Encode(ctx, events.TopicPromotionRedeemed, events.PromotionRedeemed{
		Envelope: events.Envelope{EventID: evt.EventID, OccurredAt: evt.OccurredAt.UTC().Format(time.RFC3339Nano)},
		OrderID:  evt.OrderID,
		RiderID:  evt.RiderID,
		Code:     evt.Code,
		Discount: evt.Discount,
		Status:   evt.Status,
		Currency: evt.Currency,
	})
}
