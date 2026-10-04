package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/outbox"
)

func connectDB(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS orders (
			id UUID PRIMARY KEY,
			rider_id TEXT NOT NULL,
			pickup_lat DOUBLE PRECISION NOT NULL,
			pickup_lng DOUBLE PRECISION NOT NULL,
			dropoff_lat DOUBLE PRECISION NOT NULL,
			dropoff_lng DOUBLE PRECISION NOT NULL,
			status TEXT NOT NULL DEFAULT 'PENDING',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		-- Added after the first release; existing rows were all cars.
		ALTER TABLE orders ADD COLUMN IF NOT EXISTS vehicle_type TEXT NOT NULL DEFAULT 'CAR';
	`)
	if err != nil {
		return nil, err
	}

	if err := outbox.Migrate(ctx, pool); err != nil {
		return nil, err
	}

	return pool, nil
}
