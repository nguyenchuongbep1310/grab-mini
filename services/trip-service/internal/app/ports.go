// Package app holds trip-service's use cases and the ports they need. It
// depends on internal/domain and the standard library; Postgres, Redis, Kafka
// and Prometheus are reached only through the interfaces declared here, which
// adapters implement (internal/arch_test.go enforces that).
package app

import (
	"context"
	"time"

	"grab-mini/trip-service/internal/domain"
)

// TripReader reads trips. Every method answers domain.ErrTripNotFound when
// there is none.
type TripReader interface {
	ByID(ctx context.Context, id string) (domain.Trip, error)
	// ByOrderID lets a client that only knows the orderId (from an
	// order.matched push) find the trip created for it.
	ByOrderID(ctx context.Context, orderID string) (domain.Trip, error)
	// ActiveByDriver is the driver's trip still in flight (ASSIGNED or
	// IN_PROGRESS). A driver holds one at a time; if data ever says otherwise,
	// the newest wins.
	ActiveByDriver(ctx context.Context, driverID string) (domain.Trip, error)
}

// RouteReader returns where a trip starts and ends (domain.ErrTripNotFound
// when there is no such trip; a Route with nil ends for trips made before the
// route was recorded).
type RouteReader interface {
	Route(ctx context.Context, tripID string) (domain.Route, error)
}

// CancelledOrders remembers the orders the rider cancelled (order.cancelled).
type CancelledOrders interface {
	IsCancelled(ctx context.Context, orderID string) (bool, error)
}

// Receipts closes the receipt of a finished ride.
type Receipts interface {
	// CloseReceipt marks the receipt of a COMPLETED trip closed and returns the
	// time it was closed: the first call sets it, later calls keep it. ok is
	// false if the trip is not COMPLETED.
	CloseReceipt(ctx context.Context, tripID string) (at time.Time, ok bool, err error)
}

// UnitOfWork runs fn in one database transaction: it commits when fn returns
// nil and rolls back when it returns an error. Everything a use case writes
// goes through the Tx, so a state change and the event announcing it are
// committed together or not at all (ADR 0003, 0004); the relay is woken after
// the commit if fn queued an event.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(Tx) error) error
}

// Tx is what a use case may do inside the unit of work.
type Tx interface {
	// LockOrder serializes work on one order for the rest of the transaction.
	// Creating a trip and cancelling its order both take it, so neither can
	// pass the other unnoticed.
	LockOrder(ctx context.Context, orderID string) error
	IsOrderCancelled(ctx context.Context, orderID string) (bool, error)
	// MarkOrderCancelled remembers the order is cancelled; repeating it changes nothing.
	MarkOrderCancelled(ctx context.Context, orderID string) error
	// TripIDForOrder finds the trip created for the order, if any.
	TripIDForOrder(ctx context.Context, orderID string) (tripID string, found bool, err error)
	// MarkEventProcessed records that the event was applied and reports whether
	// this is the first time.
	MarkEventProcessed(ctx context.Context, eventID string) (first bool, err error)
	// CreateTrip stores an ASSIGNED trip with its route; false means the order
	// already had a trip.
	CreateTrip(ctx context.Context, t domain.Trip, route domain.Route) (created bool, err error)

	// Transition moves the trip along the action only if it is currently in one
	// of the statuses the action may start from, and reports whether this call
	// did it. That compare-and-set is what makes repeated requests idempotent.
	// cancel is recorded for domain.Cancel and ignored for the other actions.
	Transition(ctx context.Context, tripID string, a domain.Action, cancel domain.CancelInfo) (moved bool, err error)
	// SaveRating records the rating on a COMPLETED, not-yet-rated trip (and
	// closes its receipt if still open) and reports whether this call did it.
	SaveRating(ctx context.Context, tripID string, r domain.Rating) (saved bool, err error)

	// EnqueueStatusChanged queues trip.status.changed (encoded by Encoder),
	// keyed by the trip, for the outbox relay.
	EnqueueStatusChanged(ctx context.Context, tripID string, payload []byte) error
	// EnqueueTripRated queues trip.rated, keyed by the driver.
	EnqueueTripRated(ctx context.Context, driverID string, payload []byte) error
}

// Drivers is the shared driver status record Matching also uses.
type Drivers interface {
	Set(ctx context.Context, driverID string, s domain.DriverStatus) error
	// Transition is a compare-and-set: it changes the status only if it is
	// still from, and reports whether it did.
	Transition(ctx context.Context, driverID string, from, to domain.DriverStatus) (bool, error)
}

// Locations reads where a driver last reported being.
type Locations interface {
	// Last reports found=false when no position is recorded.
	Last(ctx context.Context, driverID string) (pos domain.Position, found bool, err error)
}

// Encoder turns an announcement into the bytes to publish. Encoding can call
// Schema Registry, so it runs before the transaction opens, and a failure
// refuses the request rather than change state without telling anyone.
type Encoder interface {
	EncodeStatusChanged(ctx context.Context, e StatusChanged) ([]byte, error)
	EncodeTripRated(ctx context.Context, e TripRated) ([]byte, error)
}

// Metrics receives the facts worth counting. The Prometheus implementation keeps
// the exported metric names and labels.
type Metrics interface {
	// Transitioned counts a committed status change, by the status moved to.
	Transitioned(status string)
	Rated()
	// ProximityRefused counts a pickup/complete the proximity check refused.
	ProximityRefused(step, reason string)
}
