// Package redis is trip-service's Redis side: the driver status record and the
// last reported driver position, both shared with Matching (services/pkg/driverstatus
// and services/pkg/driverlocation own the keys and scripts).
package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
	"grab-mini/trip-service/internal/app"
	"grab-mini/trip-service/internal/domain"
)

// Drivers implements app.Drivers on the shared driver status record.
type Drivers struct{ rdb *goredis.Client }

func NewDrivers(rdb *goredis.Client) Drivers { return Drivers{rdb: rdb} }

var _ app.Drivers = Drivers{}

func (d Drivers) Set(ctx context.Context, driverID string, s domain.DriverStatus) error {
	return driverstatus.Set(ctx, d.rdb, driverID, driverstatus.Status(s))
}

func (d Drivers) Transition(ctx context.Context, driverID string, from, to domain.DriverStatus) (bool, error) {
	return driverstatus.Transition(ctx, d.rdb, driverID, driverstatus.Status(from), driverstatus.Status(to))
}

// Locations implements app.Locations on the GEO data Driver Location Service writes.
type Locations struct{ rdb *goredis.Client }

func NewLocations(rdb *goredis.Client) Locations { return Locations{rdb: rdb} }

var _ app.Locations = Locations{}

func (l Locations) Last(ctx context.Context, driverID string) (domain.Position, bool, error) {
	pos, found, err := driverlocation.Get(ctx, l.rdb, driverID)
	if err != nil || !found {
		return domain.Position{}, false, err
	}
	return domain.Position{Lat: pos.Lat, Lng: pos.Lng, At: pos.At}, true, nil
}
