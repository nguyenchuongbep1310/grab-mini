package main

import (
	"context"
	"embed"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/migrate"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const serviceName = "order-service"

// openPool connects without touching the schema.
func openPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseURL)
}

// connectDB connects and brings the schema up to date, unless
// RUN_MIGRATIONS=false (then a deploy step runs `order-service migrate`).
func connectDB(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := openPool(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := migrate.AtStartup(ctx, pool, serviceName, migrationFiles); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// runMigrate is the `order-service migrate` command: apply pending migrations and exit.
func runMigrate(env Env) {
	ctx := context.Background()
	pool, err := openPool(ctx, env.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()
	n, err := migrate.Up(ctx, pool, serviceName, migrationFiles)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s: %d migration(s) applied", serviceName, n)
}
