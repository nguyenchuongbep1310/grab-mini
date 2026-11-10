package domain

// VehicleType is what a driver drives.
type VehicleType string

const (
	VehicleCar  VehicleType = "CAR"
	VehicleBike VehicleType = "BIKE"
)

func (v VehicleType) Valid() bool { return v == VehicleCar || v == VehicleBike }

// Driver statuses.
const (
	StatusNone      = "NONE"
	StatusPending   = "PENDING"
	StatusApproved  = "APPROVED"
	StatusRejected  = "REJECTED"
	StatusSuspended = "SUSPENDED"
)

// SystemReviewer is ReviewedBy of a result nobody decided: an approved driver changed the
// vehicle, so the service takes them out of service itself.
const SystemReviewer = "system"

// ResyncReviewer is ReviewedBy (and the reason, when none is stored) of an event that only
// repeats a decision made earlier.
const ResyncReviewer = "resync"

// VehicleChangedReason is shown to the driver whose vehicle change put them back under review.
const VehicleChangedReason = "You changed your vehicle type or plate. An admin must approve the new vehicle before you get trips again."

// Limits of the id and of a page of the review queue.
const (
	MaxIDLen        = 128
	DefaultPageSize = 50
	MaxPageSize     = 100
)
