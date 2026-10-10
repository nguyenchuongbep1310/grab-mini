package eventcodec

import (
	"context"
	"testing"

	"grab-mini/pkg/events"
)

// trip.status.changed as it was before cancelledBy and reason existed.
const tripStatusChangedV1 = `{"type":"record","name":"TripStatusChanged","namespace":"grabmini.events","fields":[
 {"name":"eventId","type":"string"},{"name":"occurredAt","type":"string"},{"name":"tripId","type":"string"},{"name":"orderId","type":"string"},
 {"name":"driverId","type":"string"},{"name":"status","type":"string"},{"name":"riderId","type":"string","default":""}]}`

func TestCurrentTripStatusChangedReadsMessagesWrittenBeforeCancelledBy(t *testing.T) {
	_, url := newRegistry(t)
	ctx := context.Background()

	oldProducer := New(url, WithSchemaSource(func(string) (string, error) { return tripStatusChangedV1, nil }))
	type v1 struct {
		events.Envelope
		TripID   string `avro:"tripId"`
		OrderID  string `avro:"orderId"`
		DriverID string `avro:"driverId"`
		Status   string `avro:"status"`
		RiderID  string `avro:"riderId"`
	}
	data, err := oldProducer.Encode(ctx, events.TopicTripStatusChanged, v1{events.Envelope{EventID: "e", OccurredAt: "t"}, "t1", "o1", "d1", "CANCELLED", "r1"})
	if err != nil {
		t.Fatal(err)
	}

	var got events.TripStatusChanged
	if err := New(url).Decode(ctx, events.TopicTripStatusChanged, data, &got); err != nil {
		t.Fatalf("the current schema can't read an old message: %v", err)
	}
	if got.TripID != "t1" || got.Status != events.TripStatusCancelled || got.CancelledBy != "" || got.Reason != "" {
		t.Errorf("got %+v, want the old fields with empty cancelledBy and reason", got)
	}
}

func TestOrderCancelledRoundTripsAndDefaultsItsOptionalFields(t *testing.T) {
	_, url := newRegistry(t)
	c := New(url)
	ctx := context.Background()

	in := events.OrderCancelled{Envelope: events.Envelope{EventID: "e", OccurredAt: "t"}, OrderID: "o1", RiderID: "r1", CancelledBy: events.CancelledByRider, Reason: "changed my mind"}
	data, err := c.Encode(ctx, events.TopicOrderCancelled, in)
	if err != nil {
		t.Fatal(err)
	}
	var out events.OrderCancelled
	if err := c.Decode(ctx, events.TopicOrderCancelled, data, &out); err != nil || out != in {
		t.Errorf("round trip = %+v, %v; want %+v", out, err, in)
	}

	// A producer that only wrote the first three fields still decodes, with defaults.
	const minimal = `{"type":"record","name":"OrderCancelled","namespace":"grabmini.events","fields":[
	 {"name":"eventId","type":"string"},{"name":"occurredAt","type":"string"},{"name":"orderId","type":"string"}]}`
	old := New(url, WithSchemaSource(func(string) (string, error) { return minimal, nil }))
	data, err = old.Encode(ctx, events.TopicOrderCancelled, struct {
		events.Envelope
		OrderID string `avro:"orderId"`
	}{events.Envelope{EventID: "e", OccurredAt: "t"}, "o2"})
	if err != nil {
		t.Fatal(err)
	}
	out = events.OrderCancelled{}
	if err := c.Decode(ctx, events.TopicOrderCancelled, data, &out); err != nil || out.OrderID != "o2" || out.RiderID != "" || out.CancelledBy != "" || out.Reason != "" {
		t.Errorf("old payload = %+v, %v; want defaults", out, err)
	}
}
