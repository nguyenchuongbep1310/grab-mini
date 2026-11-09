// Package postgres holds the Postgres adapters: the connection and migrations,
// the order and idempotency-key store, and the unit of work.
package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/migrate"
)

const serviceName = "order-service"

// openPool connects without touching the schema.
func openPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}

// Connect connects and brings the schema up to date, unless
// RUN_MIGRATIONS=false (then a deploy step runs `order-service migrate`).
// migrations holds the SQL under "migrations/"; an embed.FS fits.
func Connect(ctx context.Context, databaseURL string, migrations fs.FS) (*pgxpool.Pool, error) {
	pool, err := openPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := migrate.AtStartup(ctx, pool, serviceName, migrations); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// Migrate is the `order-service migrate` command: apply pending migrations and
// return. The caller decides what a failure means (the old code exited).
func Migrate(ctx context.Context, databaseURL string, migrations fs.FS) error {
	pool, err := openPool(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()
	n, err := migrate.Up(ctx, pool, serviceName, migrations)
	if err != nil {
		return err
	}
	log.Printf("%s: %d migration(s) applied", serviceName, n)
	return nil
}
