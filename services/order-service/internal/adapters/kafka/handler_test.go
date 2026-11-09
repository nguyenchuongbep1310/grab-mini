package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"grab-mini/order-service/internal/app"
	"grab-mini/order-service/internal/domain"
	"grab-mini/pkg/events"
)

// jsonCodec stands in for the Avro codec: the wire bytes are JSON.
type jsonCodec struct {
	encoded []struct {
		topic string
		v     any
	}
}

func (jsonCodec) Decode(_ context.Context, _ string, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (c *jsonCodec) Encode(_ context.Context, topic string, v any) ([]byte, error) {
	c.encoded = append(c.encoded, struct {
		topic string
		v     any
	}{topic, v})
	return json.Marshal(v)
}

// outcomes records what the use cases asked of the Outcomes port.
type outcomes struct{ calls []string }

func (o *outcomes) Set(_ context.Context, id string, s domain.Status, driver string) (bool, error) {
	o.calls = append(o.calls, "set "+id+" "+string(s)+" "+driver)
	return true, nil
}

func (o *outcomes) CancelMatched(_ context.Context, id, by, reason string) (bool, error) {
	o.calls = append(o.calls, "cancel "+id+" "+by+" "+reason)
	return true, nil
}

const orderID = "6f9619ff-8b86-d011-b42d-00c04fc964ff"

func handle(t *testing.T, topic string, evt any) (*outcomes, error) {
	t.Helper()
	out := &outcomes{}
	h := NewHandler(app.New(app.Deps{Outcomes: out}), &jsonCodec{})
	b, _ := json.Marshal(evt)
	return out, h.Handle(context.Background(), kafkago.Message{Topic: topic, Value: b})
}

func TestOutcomeMessagesReachTheUseCases(t *testing.T) {
	cases := []struct {
		name, topic string
		evt         any
		want        []string
	}{
		{"matched", events.TopicOrderMatched, events.OrderMatched{OrderID: orderID, DriverID: "d1"},
			[]string{"set " + orderID + " MATCHED d1"}},
		{"no driver", events.TopicOrderNoDriverFound, events.OrderNoDriverFound{OrderID: orderID},
			[]string{"set " + orderID + " NO_DRIVER "}},
		{"trip cancelled", events.TopicTripStatusChanged,
			events.TripStatusChanged{OrderID: orderID, Status: events.TripStatusCancelled, CancelledBy: "DRIVER", Reason: "flat tyre"},
			[]string{"cancel " + orderID + " DRIVER flat tyre"}},
		{"trip completed is ignored", events.TopicTripStatusChanged,
			events.TripStatusChanged{OrderID: orderID, Status: events.TripStatusCompleted}, nil},
		{"trip in progress is ignored", events.TopicTripStatusChanged,
			events.TripStatusChanged{OrderID: orderID, Status: events.TripStatusInProgress}, nil},
	}
	for _, c := range cases {
		out, err := handle(t, c.topic, c.evt)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
		}
		if len(out.calls) != len(c.want) || (len(c.want) == 1 && out.calls[0] != c.want[0]) {
			t.Errorf("%s: calls = %q, want %q", c.name, out.calls, c.want)
		}
	}
}

func TestAnUnexpectedTopicIsAnError(t *testing.T) {
	if _, err := handle(t, "order.created", events.OrderMatched{OrderID: orderID}); err == nil {
		t.Fatal("want an error")
	}
}

func TestABadOrderIdIsPermanent(t *testing.T) {
	_, err := handle(t, events.TopicOrderMatched, events.OrderMatched{OrderID: "nope", DriverID: "d"})
	var bad *app.BadOutcomeError
	if !errors.As(err, &bad) {
		t.Fatalf("err = %v, want a *BadOutcomeError", err)
	}
}

func TestTheEncoderMapsEverythingAndStampsTheEnvelope(t *testing.T) {
	codec := &jsonCodec{}
	price := 50000.0
	enc := NewEncoder(codec)
	if _, err := enc.OrderCreated(context.Background(), app.OrderCreated{
		OrderID: "o", RiderID: "r", Pickup: domain.LatLng{Lat: 1, Lng: 2}, Dropoff: domain.LatLng{Lat: 3, Lng: 4},
		VehicleType: domain.VehicleBike, SurgeMultiplier: 1.5, Price: &price, PromoCode: "P", Discount: 7,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := enc.OrderCancelled(context.Background(), app.OrderCancelled{OrderID: "o", RiderID: "r", CancelledBy: "RIDER", Reason: "x"}); err != nil {
		t.Fatal(err)
	}
	created := codec.encoded[0].v.(events.OrderCreated)
	if codec.encoded[0].topic != events.TopicOrderCreated {
		t.Errorf("topic = %q", codec.encoded[0].topic)
	}
	if created.OrderID != "o" || created.RiderID != "r" || created.Pickup != (events.LatLng{Lat: 1, Lng: 2}) ||
		created.Dropoff != (events.LatLng{Lat: 3, Lng: 4}) || created.VehicleType != events.VehicleBike ||
		created.SurgeMultiplier != 1.5 || created.Price != &price || created.PromoCode != "P" || created.Discount != 7 {
		t.Errorf("order.created = %+v", created)
	}
	if created.EventID == "" || len(created.OccurredAt) <= len("2006-01-02T15:04:05Z") {
		t.Errorf("envelope = %q %q: want an id and a nanosecond time", created.EventID, created.OccurredAt)
	}
	cancelled := codec.encoded[1].v.(events.OrderCancelled)
	if codec.encoded[1].topic != events.TopicOrderCancelled || cancelled.CancelledBy != "RIDER" || cancelled.Reason != "x" ||
		cancelled.OrderID != "o" || cancelled.RiderID != "r" || cancelled.EventID == "" || cancelled.EventID == created.EventID {
		t.Errorf("order.cancelled = %+v", cancelled)
	}
}
