-- +goose Up
-- Added after the first release; existing rows were all cars.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS vehicle_type TEXT NOT NULL DEFAULT 'CAR';
