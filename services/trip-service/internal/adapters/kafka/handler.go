// Package kafka is the event side of trip-service: it decodes the three topics
// it consumes, maps each event to an app command, and encodes the two events it
// announces. The wire types (services/pkg/events) stop here; the use cases
// never see them.
package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"

	"grab-mini/pkg/eventcodec"
	"grab-mini/pkg/events"
	"grab-mini/pkg/kafkaconsumer"
	"grab-mini/trip-service/internal/app"
	"grab-mini/trip-service/internal/domain"
)

// Decoder reads an event's Avro wire bytes (*eventcodec.Codec).
type Decoder interface {
	Decode(ctx context.Context, topic string, data []byte, v any) error
}

var _ Decoder = (*eventcodec.Codec)(nil)

// Handler maps events to use cases.
type Handler struct {
	svc *app.Service
}

func NewHandler(svc *app.Service) *Handler { return &Handler{svc: svc} }

func latLng(p *events.LatLng) *domain.LatLng {
	if p == nil {
		return nil
	}
	return &domain.LatLng{Lat: p.Lat, Lng: p.Lng}
}

func (h *Handler) OnOrderMatched(ctx context.Context, evt events.OrderMatched) error {
	return h.svc.OrderMatched(ctx, app.OrderMatched{
		EventID: evt.EventID, OrderID: evt.OrderID, DriverID: evt.DriverID, RiderID: evt.RiderID,
		Pickup: latLng(evt.Pickup), Dropoff: latLng(evt.Dropoff),
	})
}

func (h *Handler) OnOrderCancelled(ctx context.Context, evt events.OrderCancelled) error {
	return h.svc.OrderCancelled(ctx, app.OrderCancelled{OrderID: evt.OrderID, CancelledBy: evt.CancelledBy, Reason: evt.Reason})
}

func (h *Handler) OnTripStatusChanged(ctx context.Context, evt events.TripStatusChanged) error {
	return h.svc.ReleaseDriver(ctx, app.TripEnded{TripID: evt.TripID, DriverID: evt.DriverID, Status: domain.Status(evt.Status)})
}

// RunOrderMatched implements task 2.1: for every order.matched event, create a
// trip in ASSIGNED status. A redelivered event is a safe no-op rather than a
// second trip (US-6.1).
func (h *Handler) RunOrderMatched(ctx context.Context, brokers []string, dec Decoder) {
	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:    brokers,
		GroupID:    "trip-service-group", // the literal is read by deploy/k8s/check.py
		Topic:      events.TopicOrderMatched,
		LogLabel:   "[trip-service]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}, func(ctx context.Context, msg kafkago.Message) error {
		var evt events.OrderMatched
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.OnOrderMatched(ctx, evt)
	})
}

// RunOrderCancelled cancels the trip of an order the rider cancelled
// (order.cancelled), for the case where a driver was matched just before the
// rider tapped cancel.
func (h *Handler) RunOrderCancelled(ctx context.Context, brokers []string, dec Decoder) {
	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:    brokers,
		GroupID:    "trip-order-cancelled-group", // the literal is read by deploy/k8s/check.py
		Topic:      events.TopicOrderCancelled,
		LogLabel:   "[trip-order-cancelled]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}, func(ctx context.Context, msg kafkago.Message) error {
		var evt events.OrderCancelled
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.OnOrderCancelled(ctx, evt)
	})
}

// RunDriverRelease frees the driver when a trip ends (trip.status.changed).
func (h *Handler) RunDriverRelease(ctx context.Context, brokers []string, dec Decoder) {
	kafkaconsumer.Run(ctx, kafkaconsumer.Config{
		Brokers:    brokers,
		GroupID:    "trip-driver-release-group", // the literal is read by deploy/k8s/check.py
		Topic:      events.TopicTripStatusChanged,
		LogLabel:   "[trip-driver-release]",
		RetryTiers: kafkaconsumer.DefaultRetryTiers,
	}, func(ctx context.Context, msg kafkago.Message) error {
		var evt events.TripStatusChanged
		if err := dec.Decode(ctx, msg.Topic, msg.Value, &evt); err != nil {
			return err
		}
		return h.OnTripStatusChanged(ctx, evt)
	})
}
