package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/events"
)

// seedDriver is a development driver. The ids and vehicle types are the ones
// `matching-service seed-drivers` puts into Redis, which is what makes them eligible for
// trips: the profiles below only describe them. The names, numbers and plates are invented.
type seedDriver struct {
	ID, Name, Phone, Plate, Model, Color string
	Vehicle                              events.VehicleType
}

var seedDrivers = []seedDriver{
	{"driver-1", "Nguyễn Văn An", "0900 000 001", "51A-123.45", "Toyota Vios", "Trắng", events.VehicleCar},
	{"driver-2", "Trần Thị Bích", "0900 000 002", "51H-678.90", "Honda City", "Đen", events.VehicleCar},
	{"driver-3", "Lê Minh Châu", "0900 000 003", "30G-246.80", "Kia Morning", "Đỏ", events.VehicleCar},
	{"driver-4", "Phạm Quốc Dũng", "0900 000 004", "59-X1 123.45", "Honda Wave Alpha", "Xanh", events.VehicleBike},
	{"driver-5", "Võ Thị Em", "0900 000 005", "59-P2 678.90", "Yamaha Sirius", "Đen", events.VehicleBike},
}

var seedRiders = []struct{ ID, Name, Phone string }{
	{"rider-1", "Hoàng Gia Bảo", "0900 100 001"},
	{"rider-2", "Đặng Thu Hà", "0900 100 002"},
	{"rider-3", "Bùi Khánh Linh", "0900 100 003"},
}

// SeedDev adds the development profiles, once: a profile that exists is left alone (an admin's
// decision or a person's edit is not undone by running the seed again), and it returns how many
// were added. The five drivers are APPROVED, reviewed by "seed-dev".
//
// It publishes no driver.application.reviewed. `matching-service seed-drivers` already makes
// these drivers AVAILABLE in Redis, and events published when this service starts would
// make Matching apply the approval again: harmless, but it would put a driver back to
// AVAILABLE after a demo script set them ON_TRIP or OFFLINE by hand. The cost is that these
// five drivers have an approved profile and no review event in Kafka.
func SeedDev(ctx context.Context, db *pgxpool.Pool) (drivers, riders int, err error) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, d := range seedDrivers {
		tag, err := db.Exec(ctx, `
			INSERT INTO driver_profiles (driver_id, name, phone, vehicle_type, plate, model, color, status, reviewed_by, reviewed_at, status_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, 'APPROVED', 'seed-dev', $8, $8, $8)
			ON CONFLICT (driver_id) DO NOTHING`,
			d.ID, d.Name, d.Phone, string(d.Vehicle), d.Plate, d.Model, d.Color, now)
		if err != nil {
			return drivers, riders, err
		}
		drivers += int(tag.RowsAffected())
	}
	for _, r := range seedRiders {
		tag, err := db.Exec(ctx, `
			INSERT INTO rider_profiles (rider_id, name, phone, updated_at) VALUES ($1, $2, $3, $4)
			ON CONFLICT (rider_id) DO NOTHING`, r.ID, r.Name, r.Phone, now)
		if err != nil {
			return drivers, riders, err
		}
		riders += int(tag.RowsAffected())
	}
	return drivers, riders, nil
}
