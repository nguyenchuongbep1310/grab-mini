package postgres

import (
	"context"
	"io/fs"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/migrate"
)

// ServiceName names the migration history table (schema_migrations_payment_service).
const ServiceName = "payment-service"

// Open connects without touching the schema.
func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}

// Connect connects and brings the schema up to date, unless
// RUN_MIGRATIONS=false (then a deploy step runs `payment-service migrate`).
// migrations is a filesystem holding a "migrations" directory of SQL files.
func Connect(ctx context.Context, databaseURL string, migrations fs.FS) (*pgxpool.Pool, error) {
	pool, err := Open(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := migrate.AtStartup(ctx, pool, ServiceName, migrations); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// Migrate is the `payment-service migrate` command: apply pending migrations and exit.
func Migrate(ctx context.Context, databaseURL string, migrations fs.FS) {
	pool, err := Open(ctx, databaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()
	n, err := migrate.Up(ctx, pool, ServiceName, migrations)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s: %d migration(s) applied", ServiceName, n)
}
