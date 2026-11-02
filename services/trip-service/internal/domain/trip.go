// Package domain holds trip-service's rules: the trip state machine and who may
// drive it, the cancellation record, the rating validation and the driver
// proximity decision. It imports the standard library only (no I/O): the
// database, Redis, Kafka and HTTP are reached through the ports of internal/app
// (internal/arch_test.go enforces that).
package domain

import (
	"errors"
	"time"
)

// Status is where a trip is in its life. The strings are the published ones
// (events.TripStatus); internal/adapters/kafka/contract_test.go pins them.
type Status string

const (
	Assigned   Status = "ASSIGNED"
	InProgress Status = "IN_PROGRESS"
	Completed  Status = "COMPLETED"
	Cancelled  Status = "CANCELLED"
)

// Ended reports whether the trip is over, so the driver is free again.
func (s Status) Ended() bool { return s == Completed || s == Cancelled }

// ErrTripNotFound is the answer for a trip that does not exist.
var ErrTripNotFound = errors.New("trip not found")

// Trip is the stored trip.
type Trip struct {
	ID       string
	OrderID  string
	DriverID string
	// RiderID is empty on trips made before it was recorded.
	RiderID string
	Status  Status
	// Rating is set once the rider has rated the trip.
	Rating *int
	// CancelledBy (RIDER or DRIVER) and CancelReason are set once CANCELLED.
	CancelledBy  string
	CancelReason string
	// CompletedAt is set by the first completion, and only COMPLETED trips have it.
	CompletedAt *time.Time
	// ReceiptClosedAt is set when the rider closed the finished ride's receipt or rated it.
	ReceiptClosedAt *time.Time
}

// Role is the side of a trip a caller acts as.
type Role string

const (
	RoleRider  Role = "RIDER"
	RoleDriver Role = "DRIVER"
)

// Actor is whoever is calling: an id and the side they act as. Any other
// caller is an Actor with an empty Role, which is a party to nothing.
type Actor struct {
	ID   string
	Role Role
}

// IsDriverOf says whether a is the driver of t.
func (t Trip) IsDriverOf(a Actor) bool { return a.Role == RoleDriver && a.ID == t.DriverID }

// IsRiderOf says whether a is the rider of t. Trips made before rider_id was
// recorded have none, so no rider matches them (not even an empty id).
func (t Trip) IsRiderOf(a Actor) bool {
	return a.Role == RoleRider && t.RiderID != "" && a.ID == t.RiderID
}

// IsParty says whether a is either side of t.
func (t Trip) IsParty(a Actor) bool { return t.IsDriverOf(a) || t.IsRiderOf(a) }

// Action is one of the three moves a caller can make on a trip. The state
// machine: pickup ASSIGNED -> IN_PROGRESS, complete IN_PROGRESS -> COMPLETED,
// cancel ASSIGNED or IN_PROGRESS -> CANCELLED (never once COMPLETED).
//
// Doing a move to the state the trip is already in is a no-op success, which
// is what makes a retried request safe (US-6.1). Anything else outside From is
// refused.
type Action struct {
	name string
	from []Status
	to   Status
	step Step // the proximity step the driver must be at; "" for none
	any  bool // either party may do it (else only the trip's driver)
}

var (
	Pickup   = Action{name: "pickup", from: []Status{Assigned}, to: InProgress, step: StepPickup}
	Complete = Action{name: "complete", from: []Status{InProgress}, to: Completed, step: StepDropoff}
	Cancel   = Action{name: "cancel", from: []Status{Assigned, InProgress}, to: Cancelled, any: true}
)

func (a Action) Name() string { return a.name }

// To is the status the action moves a trip to.
func (a Action) To() Status { return a.to }

// From lists the statuses the action may start from (a copy).
func (a Action) From() []Status { return append([]Status(nil), a.from...) }

// CanStartFrom says whether a trip in status s may take the action.
func (a Action) CanStartFrom(s Status) bool {
	for _, f := range a.from {
		if f == s {
			return true
		}
	}
	return false
}

// Reached says whether t is already where the action leads.
func (a Action) Reached(t Trip) bool { return t.Status == a.to }

// MayBeDoneBy says who may take the action: the trip's driver for pickup and
// complete, either party for cancel.
func (a Action) MayBeDoneBy(who Actor, t Trip) bool {
	if a.any {
		return t.IsParty(who)
	}
	return t.IsDriverOf(who)
}

// ProximityStep is the place the driver has to be near to take the action.
func (a Action) ProximityStep() (Step, bool) { return a.step, a.step != "" }

// ReleasesDriver says whether the action ends the trip, so the driver goes back to AVAILABLE.
func (a Action) ReleasesDriver() bool { return a.to.Ended() }

// DriverStatus is what Matching and Trip Service know of a driver's availability.
// The strings are those of the shared driver-status record in Redis.
type DriverStatus string

const (
	DriverAvailable DriverStatus = "AVAILABLE"
	DriverAssigned  DriverStatus = "ASSIGNED"
	DriverOnTrip    DriverStatus = "ON_TRIP"
)
