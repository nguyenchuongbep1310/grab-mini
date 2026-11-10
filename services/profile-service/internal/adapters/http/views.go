package http

import (
	"time"

	"grab-mini/profile-service/internal/domain"
)

// The JSON shapes are part of the contract with the apps: tags, field order and the nulls
// do not change.

type riderView struct {
	RiderID   string     `json:"riderId"`
	Name      string     `json:"name"`
	Phone     string     `json:"phone"`
	UpdatedAt *time.Time `json:"updatedAt"`
}

// newRiderView shows updatedAt as null for a rider who never saved a profile.
func newRiderView(r domain.Rider) riderView {
	v := riderView{RiderID: r.ID, Name: r.Name, Phone: r.Phone}
	if !r.UpdatedAt.IsZero() {
		v.UpdatedAt = &r.UpdatedAt
	}
	return v
}

// driverView is a driver's own profile. ReviewedBy (an admin's id) is not shown to the driver.
type driverView struct {
	DriverID    string     `json:"driverId"`
	Name        string     `json:"name"`
	Phone       string     `json:"phone"`
	VehicleType string     `json:"vehicleType"`
	Plate       string     `json:"plate"`
	Model       string     `json:"model"`
	Color       string     `json:"color"`
	Status      string     `json:"status"`
	Reason      string     `json:"reason"`
	ReviewedAt  *time.Time `json:"reviewedAt"`
	StatusAt    *time.Time `json:"statusAt"`
	UpdatedAt   *time.Time `json:"updatedAt"`
}

// newDriverView shows a driver with no status as NONE, and statusAt and updatedAt as null
// for one who never saved a profile.
func newDriverView(d domain.Driver) driverView {
	v := driverView{
		DriverID: d.ID, Name: d.Name, Phone: d.Phone, VehicleType: string(d.Vehicle), Plate: d.Plate,
		Model: d.Model, Color: d.Color, Status: d.Status, Reason: d.Reason, ReviewedAt: d.ReviewedAt,
	}
	if d.Status == "" {
		v.Status = domain.StatusNone
	}
	if !d.UpdatedAt.IsZero() {
		v.StatusAt, v.UpdatedAt = &d.StatusAt, &d.UpdatedAt
	}
	return v
}

// adminDriverView is what a reviewer sees: the driver's profile and who decided.
type adminDriverView struct {
	driverView
	ReviewedBy string `json:"reviewedBy"`
}

func newAdminDriverView(d domain.Driver) adminDriverView {
	return adminDriverView{driverView: newDriverView(d), ReviewedBy: d.ReviewedBy}
}

// publicDriverView is all anyone but the driver and an admin may see: who is driving and
// what they drive. No phone, no status, no history.
type publicDriverView struct {
	DriverID    string `json:"driverId"`
	Name        string `json:"name"`
	VehicleType string `json:"vehicleType"`
	Plate       string `json:"plate"`
	Model       string `json:"model"`
	Color       string `json:"color"`
}

func newPublicDriverView(d domain.Driver) publicDriverView {
	return publicDriverView{
		DriverID: d.ID, Name: d.Name, VehicleType: string(d.Vehicle), Plate: d.Plate, Model: d.Model, Color: d.Color,
	}
}
