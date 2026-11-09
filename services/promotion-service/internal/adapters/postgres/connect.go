// Package postgres holds the Postgres adapters: the connection and migrations,
// the promotion store, the unit of work and the demo seed.
package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/migrate"
)

// Open connects without touching the schema.
func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}

// Connect connects and brings the schema up to date, unless
// RUN_MIGRATIONS=false (then a deploy step runs `promotion-service migrate`).
// migrations holds the SQL under "migrations/"; an embed.FS fits.
func Connect(ctx context.Context, databaseURL, service string, migrations fs.FS) (*pgxpool.Pool, error) {
	pool, err := Open(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := migrate.AtStartup(ctx, pool, service, migrations); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// Migrate is the `promotion-service migrate` command: apply pending migrations
// and return. A connection failure is returned wrapped with "failed to connect
// to database"; the caller decides what a failure means (the old code exited).
func Migrate(ctx context.Context, databaseURL, service string, migrations fs.FS) error {
	pool, err := Open(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()
	n, err := migrate.Up(ctx, pool, service, migrations)
	if err != nil {
		return err
	}
	log.Printf("%s: %d migration(s) applied", service, n)
	return nil
}
