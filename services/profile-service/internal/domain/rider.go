package domain

import "time"

// Rider is a rider_profiles row. PERSONAL DATA.
type Rider struct {
	ID        string
	Name      string
	Phone     string
	UpdatedAt time.Time
}
