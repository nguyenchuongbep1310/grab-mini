package domain

import (
	"regexp"
	"strings"
)

// VehicleCar is the vehicle assumed when a request names none.
const VehicleCar = "CAR"

const vehicleBike = "BIKE"

var codePattern = regexp.MustCompile(`^[A-Z0-9_-]{3,32}$`)

// NormalizeCode upper-cases and trims what a rider typed.
func NormalizeCode(v string) string { return strings.ToUpper(strings.TrimSpace(v)) }

// ValidCode says whether a normalised code has the shape of a promotion code.
func ValidCode(code string) bool { return codePattern.MatchString(code) }

// ValidVehicle says whether v is a vehicle type (CAR or BIKE, case-sensitive).
func ValidVehicle(v string) bool { return v == VehicleCar || v == vehicleBike }
