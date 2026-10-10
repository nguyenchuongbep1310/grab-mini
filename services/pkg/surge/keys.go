package surge

import (
	"fmt"
	"time"
)

// Redis keys shared by Pricing Service (writer) and Order Service (reader).

// MultTTL is how long a stored multiplier lives. Pricing Service refreshes it
// every few seconds; if it stops, prices fall back to 1.0 within this time.
const MultTTL = 30 * time.Second

// DemandTTL is how long a per-minute demand counter lives.
const DemandTTL = 5 * time.Minute

// MultKey holds the current multiplier for a vehicle type in a cell. A
// missing key means 1.0: pricing fails open.
func MultKey(vehicle, cell string) string { return fmt.Sprintf("surge:mult:%s:%s", vehicle, cell) }

// DemandKey counts orders created in a cell during one minute.
func DemandKey(vehicle, cell string, minute int64) string {
	return fmt.Sprintf("surge:demand:%s:%s:%d", vehicle, cell, minute)
}

// CellsKey is the set of "vehicle|cell" members that have recent demand: the
// cells the sampler must look at.
const CellsKey = "surge:cells"

// CellMember is the CellsKey member for a vehicle type and cell.
func CellMember(vehicle, cell string) string { return vehicle + "|" + cell }

// Minute is the demand bucket a time falls in.
func Minute(t time.Time) int64 { return t.Unix() / 60 }
