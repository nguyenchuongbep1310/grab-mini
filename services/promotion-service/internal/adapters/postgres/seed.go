package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/promotion-service/internal/domain"
)

// SeedDev adds the demo promotions, once: the codes are there already on a second
// run, and an admin's edits (a disabled code stays disabled) are left alone. They
// are valid from a day ago for a year.
//
//	WELCOME10  10% off, at most 20,000 VND, for a fare of 40,000 VND or more
//	FLAT15K    15,000 VND off a fare of 40,000 VND or more, up to 5 times per rider
//	BIKE5K     5,000 VND off a BIKE trip (a code for one vehicle type)
func SeedDev(ctx context.Context, db *pgxpool.Pool) (int, error) {
	from := time.Now().Add(-24 * time.Hour)
	to := from.AddDate(1, 0, 1)
	added := 0
	maxWelcome, bike := int64(20000), "BIKE"
	for _, p := range []struct {
		code, kind  string
		value       int64
		maxDiscount *int64
		minFare     int64
		vehicle     *string
		perRider    int64
	}{
		{"WELCOME10", domain.KindPercent, 10, &maxWelcome, 40000, nil, 1},
		{"FLAT15K", domain.KindFixed, 15000, nil, 40000, nil, 5},
		{"BIKE5K", domain.KindFixed, 5000, nil, 0, &bike, 5},
	} {
		tag, err := db.Exec(ctx, `
			INSERT INTO promotions (code, kind, value, max_discount, min_fare, vehicle_type, valid_from, valid_to, per_rider_limit, created_by)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'seed-dev')
			ON CONFLICT (code) DO NOTHING`,
			p.code, p.kind, p.value, p.maxDiscount, p.minFare, p.vehicle, from, to, p.perRider)
		if err != nil {
			return added, err
		}
		added += int(tag.RowsAffected())
	}
	return added, nil
}
