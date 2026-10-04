// Package migrate applies a service's versioned SQL migrations to Postgres.
//
// Each service owns a directory of numbered .sql files (embedded in its
// binary) and a version table of its own, so services that share a database
// today can be split into separate databases later without untangling their
// history. Applying is safe to run from several instances at once: a
// Postgres advisory lock lets one run while the others wait, then find
// nothing left to do.
//
// Migrations are forward-only. Write them so they are safe to run against a
// database that already has the objects (IF NOT EXISTS): that is how a
// database created by the service's old startup DDL adopts them.
package migrate

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// Dir is where a service's migrations sit inside the fs it passes in.
const Dir = "migrations"

// Up applies every migration in fsys (under Dir) that service hasn't applied
// yet, and returns how many it applied.
func Up(ctx context.Context, pool *pgxpool.Pool, service string, fsys fs.FS) (int, error) {
	migrations, err := fs.Sub(fsys, Dir)
	if err != nil {
		return 0, err
	}

	// A closed *sql.DB from OpenDBFromPool doesn't close the pool.
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return 0, err
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations,
		goose.WithTableName("schema_migrations_"+strings.ReplaceAll(service, "-", "_")),
		goose.WithSessionLocker(locker),
	)
	if err != nil {
		return 0, fmt.Errorf("%s migrations: %w", service, err)
	}

	results, err := provider.Up(ctx)
	for _, r := range results {
		log.Printf("[%s] applied migration %s in %s", service, r.Source.Path, r.Duration)
	}
	if err != nil {
		return len(results), fmt.Errorf("%s migrations: %w", service, err)
	}
	return len(results), nil
}

// AtStartup runs Up unless RUN_MIGRATIONS=false. Locally the default (run)
// keeps `go run .` working from an empty database; where a deploy pipeline
// runs the migrations first (`<service> migrate`) set it to false, so a
// service never needs permission to change the schema and a bad migration
// stops the deploy rather than a rolling restart.
func AtStartup(ctx context.Context, pool *pgxpool.Pool, service string, fsys fs.FS) error {
	if strings.EqualFold(os.Getenv("RUN_MIGRATIONS"), "false") {
		log.Printf("[%s] RUN_MIGRATIONS=false, not migrating at startup", service)
		return nil
	}
	_, err := Up(ctx, pool, service, fsys)
	return err
}
