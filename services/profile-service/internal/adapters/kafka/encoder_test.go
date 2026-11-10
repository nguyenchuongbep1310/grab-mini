package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"grab-mini/pkg/events"
	"grab-mini/profile-service/internal/app"
	"grab-mini/profile-service/internal/domain"
)

type codecCall struct {
	topic string
	v     any
}

type fakeCodec struct {
	calls []codecCall
	out   []byte
	err   error
}

func (c *fakeCodec) Encode(_ context.Context, topic string, v any) ([]byte, error) {
	c.calls = append(c.calls, codecCall{topic, v})
	return c.out, c.err
}

var _ app.Encoder = (*Encoder)(nil)

func TestTheEncoderMapsEveryFieldAndStampsTheEnvelope(t *testing.T) {
	c := &fakeCodec{out: []byte("avro")}
	zone := time.FixedZone("UTC+7", 7*3600)
	at := time.Date(2026, 3, 4, 12, 30, 15, 123456000, zone)
	got, err := NewEncoder(c).Reviewed(context.Background(), app.ReviewedEvent{
		EventID: "evt-1", OccurredAt: at, DriverID: "d1", Status: domain.StatusSuspended,
		Vehicle: domain.VehicleBike, Reason: "why", ReviewedBy: "admin-1",
	})
	if err != nil || string(got) != "avro" {
		t.Fatalf("%q %v", got, err)
	}
	if len(c.calls) != 1 {
		t.Fatalf("%d calls", len(c.calls))
	}
	want := events.DriverApplicationReviewed{
		Envelope:    events.Envelope{EventID: "evt-1", OccurredAt: "2026-03-04T12:30:15.123456+07:00"},
		DriverID:    "d1",
		Status:      "SUSPENDED",
		VehicleType: events.VehicleBike,
		Reason:      "why",
		ReviewedBy:  "admin-1",
	}
	if !reflect.DeepEqual(c.calls[0].v, want) {
		t.Fatalf("encoded %+v, want %+v", c.calls[0].v, want)
	}
}

func TestTheEncoderUsesTheReviewedTopic(t *testing.T) {
	c := &fakeCodec{}
	NewEncoder(c).Reviewed(context.Background(), app.ReviewedEvent{OccurredAt: time.Now()})
	if len(c.calls) != 1 || c.calls[0].topic != "driver.application.reviewed" {
		t.Fatalf("%+v", c.calls)
	}
}

func TestAnEncoderErrorIsReturnedUntouched(t *testing.T) {
	boom := errors.New("boom")
	got, err := NewEncoder(&fakeCodec{err: boom}).Reviewed(context.Background(), app.ReviewedEvent{OccurredAt: time.Now()})
	if err != boom || got != nil {
		t.Fatalf("%q %v", got, err)
	}
}
