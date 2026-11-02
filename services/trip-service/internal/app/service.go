package app

import (
	"time"

	"grab-mini/trip-service/internal/domain"
)

// Service is the set of use cases. It holds the ports and the proximity
// configuration, and no state of its own.
type Service struct {
	trips     TripReader
	routes    RouteReader
	orders    CancelledOrders
	receipts  Receipts
	uow       UnitOfWork
	drivers   Drivers
	locations Locations
	enc       Encoder
	metrics   Metrics
	prox      domain.Proximity
	now       func() time.Time
}

// Deps are what NewService needs. A zero Proximity means the defaults and a
// nil Now means time.Now (the clock is a plain func so tests can move it).
type Deps struct {
	Trips      TripReader
	Routes     RouteReader
	Orders     CancelledOrders
	Receipts   Receipts
	UnitOfWork UnitOfWork
	Drivers    Drivers
	Locations  Locations
	Encoder    Encoder
	Metrics    Metrics
	Proximity  domain.Proximity
	Now        func() time.Time
}

func NewService(d Deps) *Service {
	now := d.Now
	if now == nil {
		now = time.Now
	}
	prox := d.Proximity
	if prox == (domain.Proximity{}) {
		prox = domain.DefaultProximity()
	}
	return &Service{
		trips: d.Trips, routes: d.Routes, orders: d.Orders, receipts: d.Receipts, uow: d.UnitOfWork,
		drivers: d.Drivers, locations: d.Locations, enc: d.Encoder, metrics: d.Metrics, prox: prox, now: now,
	}
}
