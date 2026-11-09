package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io/fs"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/order-service/internal/adapters/postgres"
)

// testDB returns a pool on a database created for this one test and dropped
// when it ends, with the service's real migrations applied. It skips the test
// when TEST_DATABASE_URL is not set.
//
// Why a database of its own: go test runs the packages of a module in
// parallel against the same TEST_DATABASE_URL, and these tests clear tables
// and assert table-wide counts. Two packages clearing the same tables wipe
// each other's rows. So EVERY package that clears tables must use its own
// database; copy this file as it is (change only the package name and
// migrations()) into any other package that does.
// migrations is the service's own migration directory, read from disk: the
// embed lives in package main, which this one cannot import.
func migrations() fs.FS { return os.DirFS("../..") }

func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)

	b := make([]byte, 6)
	rand.Read(b)
	name := "ordertest_integration_" + hex.EncodeToString(b)
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), base)
		if err != nil {
			t.Errorf("dropping %s: %v", name, err)
			return
		}
		defer c.Close(context.Background())
		if _, err := c.Exec(context.Background(), `DROP DATABASE `+name+` WITH (FORCE)`); err != nil {
			t.Errorf("dropping %s: %v", name, err)
		}
	})

	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	db, err := postgres.Connect(ctx, u.String(), migrations())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close) // runs before the drop: cleanups run last-in first-out
	return db
}
