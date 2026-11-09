package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"grab-mini/pkg/events"
	"grab-mini/promotion-service/internal/app"
	"grab-mini/promotion-service/internal/domain"
)

// jsonCodec stands in for the Avro codec: the wire bytes are JSON.
type jsonCodec struct {
	topics []string
	vals   []any
}

func (jsonCodec) Decode(_ context.Context, _ string, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (c *jsonCodec) Encode(_ context.Context, topic string, v any) ([]byte, error) {
	c.topics = append(c.topics, topic)
	c.vals = append(c.vals, v)
	return json.Marshal(v)
}

// rec is a UnitOfWork and Tx that record what the use cases ask of the store.
type rec struct{ calls []string }

func (r *rec) Do(_ context.Context, fn func(app.Tx) error) error { return fn(r) }

func (r *rec) MarkEventProcessed(_ context.Context, id string) (bool, error) {
	r.calls = append(r.calls, "mark "+id)
	return true, nil
}
func (r *rec) LockOrder(_ context.Context, id string) error {
	r.calls = append(r.calls, "lock "+id)
	return nil
}
func (r *rec) FindPromotionForUpdate(_ context.Context, code string) (domain.Promotion, error) {
	r.calls = append(r.calls, "find "+code)
	return domain.Promotion{}, domain.ErrNotFound
}
func (r *rec) ReleaseRedemptions(_ context.Context, id string) (int64, error) {
	r.calls = append(r.calls, "release "+id)
	return 1, nil
}
func (r *rec) OrderWasCancelled(context.Context, string) (bool, error) { panic("unused") }
func (r *rec) Usage(context.Context, string, string) (domain.Usage, error) {
	panic("unused")
}
func (r *rec) InsertRedemption(context.Context, app.NewRedemption) (bool, error) { panic("unused") }
func (r *rec) QueueRedeemed(context.Context, string, []byte) error               { panic("unused") }
func (r *rec) RememberCancelled(context.Context, string) error                   { panic("unused") }

type noMetrics struct{}

func (noMetrics) Validation(string)      {}
func (noMetrics) Redemption(_, _ string) {}
func (noMetrics) Released(int64)         {}
func (noMetrics) Duplicate()             {}

const (
	eventID = "11111111-1111-4111-8111-111111111111"
	orderID = "order-1"
)

func handle(t *testing.T, topic string, evt any) (*rec, error) {
	t.Helper()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(log.Writer()) })
	r := &rec{}
	h := NewHandler(app.New(app.Deps{UoW: r, Metrics: noMetrics{}}), &jsonCodec{})
	b, _ := json.Marshal(evt)
	return r, h.Handle(context.Background(), kafkago.Message{Topic: topic, Value: b})
}

func TestEachTopicReachesItsUseCase(t *testing.T) {
	env := events.Envelope{EventID: eventID}
	cases := []struct {
		name, topic string
		evt         any
		want        []string
		wantErr     string
	}{
		{"order created", events.TopicOrderCreated,
			events.OrderCreated{Envelope: env, OrderID: orderID, RiderID: "r1", PromoCode: " gold10 ", Discount: 5},
			[]string{"mark " + eventID, "lock " + orderID, "find GOLD10"}, "unknown promotion"},
		{"order created without a code is not ours", events.TopicOrderCreated,
			events.OrderCreated{Envelope: env, OrderID: orderID, RiderID: "r1"}, nil, ""},
		{"order created without a rider", events.TopicOrderCreated,
			events.OrderCreated{Envelope: env, OrderID: orderID, PromoCode: "GOLD10"}, nil, "without orderId or riderId"},
		{"order created discount mapped", events.TopicOrderCreated,
			events.OrderCreated{Envelope: env, OrderID: orderID, RiderID: "r1", PromoCode: "GOLD10", Discount: -1}, nil, "discount -1"},
		{"order cancelled", events.TopicOrderCancelled,
			events.OrderCancelled{Envelope: env, OrderID: orderID},
			[]string{"mark " + eventID, "lock " + orderID, "release " + orderID}, ""},
		{"trip cancelled", events.TopicTripStatusChanged,
			events.TripStatusChanged{Envelope: env, OrderID: orderID, Status: events.TripStatusCancelled},
			[]string{"mark " + eventID, "lock " + orderID, "release " + orderID}, ""},
		{"no driver", events.TopicOrderNoDriverFound,
			events.OrderNoDriverFound{Envelope: env, OrderID: orderID},
			[]string{"mark " + eventID, "lock " + orderID, "release " + orderID}, ""},
	}
	for _, c := range cases {
		r, err := handle(t, c.topic, c.evt)
		if c.wantErr == "" && err != nil {
			t.Errorf("%s: %v", c.name, err)
		}
		if c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
			t.Errorf("%s: error %v, want one containing %q", c.name, err, c.wantErr)
		}
		if strings.Join(r.calls, "|") != strings.Join(c.want, "|") {
			t.Errorf("%s: calls %v, want %v", c.name, r.calls, c.want)
		}
	}
}

func TestATripStatusOtherThanCancelledIsIgnored(t *testing.T) {
	for _, s := range []events.TripStatus{events.TripStatusCompleted, events.TripStatusInProgress} {
		r, err := handle(t, events.TopicTripStatusChanged,
			events.TripStatusChanged{Envelope: events.Envelope{EventID: eventID}, OrderID: orderID, Status: s})
		if err != nil || len(r.calls) != 0 {
			t.Errorf("%s: err %v, calls %v; want neither", s, err, r.calls)
		}
	}
}

func TestAnUnexpectedTopicIsAnError(t *testing.T) {
	_, err := handle(t, "order.matched", events.OrderCreated{})
	if err == nil || err.Error() != `unexpected topic "order.matched"` {
		t.Errorf("error %v", err)
	}
}

type failDecoder struct{ err error }

func (f failDecoder) Decode(context.Context, string, []byte, any) error { return f.err }

func TestADecodeErrorIsReturnedUntouched(t *testing.T) {
	boom := errors.New("bad avro")
	h := NewHandler(app.New(app.Deps{UoW: &rec{}, Metrics: noMetrics{}}), failDecoder{boom})
	for _, topic := range ConsumedTopics {
		if err := h.Handle(context.Background(), kafkago.Message{Topic: topic}); err != boom {
			t.Errorf("%s: error %v, want the decoder's own", topic, err)
		}
	}
}

func TestTheEncoderStampsTheEnvelopeFromTheEventAndMapsTheFields(t *testing.T) {
	c := &jsonCodec{}
	at := time.Date(2026, 6, 1, 19, 0, 0, 123456789, time.FixedZone("x", 7*3600))
	b, err := NewEncoder(c).Redeemed(context.Background(), app.RedeemedEvent{
		EventID: eventID, OccurredAt: at, OrderID: orderID, RiderID: "r1", Code: "GOLD10",
		Discount: 8700, Status: "OVER_LIMIT", Currency: "VND",
	})
	if err != nil || len(b) == 0 {
		t.Fatalf("%v %q", err, b)
	}
	if len(c.topics) != 1 || c.topics[0] != events.TopicPromotionRedeemed {
		t.Fatalf("topics %v", c.topics)
	}
	want := events.PromotionRedeemed{
		Envelope: events.Envelope{EventID: eventID, OccurredAt: "2026-06-01T12:00:00.123456789Z"},
		OrderID:  orderID, RiderID: "r1", Code: "GOLD10", Discount: 8700, Status: "OVER_LIMIT", Currency: "VND",
	}
	if got := c.vals[0]; got != want {
		t.Errorf("encoded %+v, want %+v", got, want)
	}
}
