package main

import (
	"testing"

	"grab-mini/pkg/events"
)

// Needs Redis like the other notification tests (see setup).

func TestACancelledOrderIsPushedToTheRiderWithWhoCancelled(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	riderSocket := a.connect(t, kindRider, "rider-1")
	other := a.connect(t, kindRider, "rider-2")

	evt := events.OrderCancelled{OrderID: "o1", RiderID: "rider-1", CancelledBy: events.CancelledByRider, Reason: "changed my mind"}
	if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicOrderCancelled, evt)); err != nil {
		t.Fatal(err)
	}

	m := receive(t, riderSocket)
	p, _ := m.Payload.(map[string]any)
	if m.Type != events.TopicOrderCancelled || p["orderId"] != "o1" || p["cancelledBy"] != "RIDER" || p["reason"] != "changed my mind" {
		t.Errorf("rider got %+v, want order.cancelled naming who cancelled and why", m)
	}
	expectNothing(t, other)
}

func TestACancelledOrderAlsoReachesItsMatchedDriver(t *testing.T) {
	ctx, addr := setup(t)
	a, b := newInstance(t, ctx, addr), newInstance(t, ctx, addr)
	riderSocket := a.connect(t, kindRider, "rider-1")
	driverSocket := b.connect(t, kindDriver, "driver-1")

	matched := events.OrderMatched{OrderID: "o1", DriverID: "driver-1", RiderID: "rider-1", VehicleType: events.VehicleCar}
	if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicOrderMatched, matched)); err != nil {
		t.Fatal(err)
	}
	receive(t, riderSocket)
	receive(t, driverSocket)

	// Handled by another instance than the one that saw the match.
	evt := events.OrderCancelled{OrderID: "o1", RiderID: "rider-1", CancelledBy: events.CancelledByRider}
	if err := handleMessage(ctx, jsonDecoder{}, b.h, kafkaMsg(t, events.TopicOrderCancelled, evt)); err != nil {
		t.Fatal(err)
	}
	if m := receive(t, riderSocket); m.Type != events.TopicOrderCancelled {
		t.Errorf("rider got %+v", m)
	}
	if m := receive(t, driverSocket); m.Type != events.TopicOrderCancelled {
		t.Errorf("driver got %+v, want order.cancelled", m)
	}
}

func TestACancelledTripCarriesWhoCancelledToBothSides(t *testing.T) {
	ctx, addr := setup(t)
	a := newInstance(t, ctx, addr)
	riderSocket := a.connect(t, kindRider, "rider-1")
	driverSocket := a.connect(t, kindDriver, "driver-1")

	evt := events.TripStatusChanged{TripID: "t1", OrderID: "o1", DriverID: "driver-1", RiderID: "rider-1",
		Status: events.TripStatusCancelled, CancelledBy: events.CancelledByDriver, Reason: "flat tyre"}
	if err := handleMessage(ctx, jsonDecoder{}, a.h, kafkaMsg(t, events.TopicTripStatusChanged, evt)); err != nil {
		t.Fatal(err)
	}
	if m := receive(t, riderSocket); m.Payload.(map[string]any)["cancelledBy"] != "DRIVER" {
		t.Errorf("rider got %+v", m)
	}
	if m := receive(t, driverSocket); m.Payload.(map[string]any)["reason"] != "flat tyre" {
		t.Errorf("driver got %+v", m)
	}
}
