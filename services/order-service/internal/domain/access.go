package domain

// Role is the side of the marketplace a caller acts as.
type Role string

const (
	RoleRider  Role = "rider"
	RoleDriver Role = "driver"
)

// Actor is the authenticated caller.
type Actor struct {
	Subject string
	Role    Role
}

// MayBeReadBy reports whether a may see the order: the rider who owns it, or
// the driver it was matched with (never any driver before a match).
func (o Order) MayBeReadBy(a Actor) bool {
	switch a.Role {
	case RoleRider:
		return o.RiderID == a.Subject
	case RoleDriver:
		return o.DriverID != "" && o.DriverID == a.Subject
	}
	return false
}
