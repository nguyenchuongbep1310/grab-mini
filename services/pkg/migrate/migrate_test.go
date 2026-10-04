package migrate

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Needs Postgres: set TEST_DATABASE_URL to a throwaway database.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// uniq gives a service name and table that no other test run shares.
func uniq() (service, table string) {
	id := strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	return "svc-" + id, "t_" + id
}

func migration(sql string) *fstest.MapFile {
	return &fstest.MapFile{Data: []byte("-- +goose Up\n" + sql + ";\n")}
}

func tableExists(t *testing.T, pool *pgxpool.Pool, table string) bool {
	t.Helper()
	var ok bool
	if err := pool.QueryRow(context.Background(), `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

func TestAppliesPendingMigrationsOnceAndInOrder(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc, tbl := uniq()
	fsys := fstest.MapFS{
		"migrations/00001_create.sql": migration(fmt.Sprintf(`CREATE TABLE %s (id INT)`, tbl)),
		"migrations/00002_add.sql":    migration(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN name TEXT`, tbl)),
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "DROP TABLE IF EXISTS "+tbl+", schema_migrations_"+strings.ReplaceAll(svc, "-", "_"))
	})

	if n, err := Up(ctx, pool, svc, fsys); err != nil || n != 2 {
		t.Fatalf("Up = %d, %v; want 2 applied", n, err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (1, 'x')`, tbl)); err != nil {
		t.Fatalf("the second migration's column is missing: %v", err)
	}
	if n, err := Up(ctx, pool, svc, fsys); err != nil || n != 0 {
		t.Fatalf("second Up = %d, %v; want nothing left to apply", n, err)
	}

	// A migration added later is the only one that runs.
	fsys["migrations/00003_more.sql"] = migration(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN extra INT`, tbl))
	if n, err := Up(ctx, pool, svc, fsys); err != nil || n != 1 {
		t.Fatalf("Up after adding one = %d, %v; want 1", n, err)
	}
}

func TestServicesKeepSeparateHistories(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svcA, tblA := uniq()
	svcB, tblB := uniq()
	t.Cleanup(func() {
		pool.Exec(ctx, "DROP TABLE IF EXISTS "+tblA+", "+tblB+", schema_migrations_"+strings.ReplaceAll(svcA, "-", "_")+", schema_migrations_"+strings.ReplaceAll(svcB, "-", "_"))
	})
	// Both use version 1: with one shared version table the second would be skipped.
	a := fstest.MapFS{"migrations/00001_a.sql": migration(fmt.Sprintf(`CREATE TABLE %s (id INT)`, tblA))}
	b := fstest.MapFS{"migrations/00001_b.sql": migration(fmt.Sprintf(`CREATE TABLE %s (id INT)`, tblB))}

	if _, err := Up(ctx, pool, svcA, a); err != nil {
		t.Fatal(err)
	}
	if n, err := Up(ctx, pool, svcB, b); err != nil || n != 1 {
		t.Fatalf("service B Up = %d, %v; want its own version 1 applied", n, err)
	}
	if !tableExists(t, pool, tblA) || !tableExists(t, pool, tblB) {
		t.Error("a service's migration was skipped")
	}
}

// Several instances starting together must not both run the migration (a
// plain CREATE TABLE would fail the second).
func TestInstancesStartingTogetherRunEachMigrationOnce(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc, tbl := uniq()
	t.Cleanup(func() {
		pool.Exec(ctx, "DROP TABLE IF EXISTS "+tbl+", schema_migrations_"+strings.ReplaceAll(svc, "-", "_"))
	})
	fsys := fstest.MapFS{"migrations/00001_create.sql": migration(fmt.Sprintf(`CREATE TABLE %s (id INT)`, tbl))}

	var wg sync.WaitGroup
	applied := make([]int, 6)
	errs := make([]error, 6)
	for i := range applied {
		wg.Add(1)
		go func() {
			defer wg.Done()
			applied[i], errs[i] = Up(ctx, pool, svc, fsys)
		}()
	}
	wg.Wait()

	total := 0
	for i := range applied {
		if errs[i] != nil {
			t.Fatalf("instance %d: %v", i, errs[i])
		}
		total += applied[i]
	}
	if total != 1 {
		t.Errorf("the migration was applied %d times in total, want once", total)
	}
}

// A database made by the service's old startup DDL has the objects already.
func TestAdoptsADatabaseThatAlreadyHasTheObjects(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc, tbl := uniq()
	t.Cleanup(func() {
		pool.Exec(ctx, "DROP TABLE IF EXISTS "+tbl+", schema_migrations_"+strings.ReplaceAll(svc, "-", "_"))
	})
	pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (id INT)`, tbl))
	pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s VALUES (7)`, tbl))
	fsys := fstest.MapFS{"migrations/00001_create.sql": migration(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT)`, tbl))}

	if _, err := Up(ctx, pool, svc, fsys); err != nil {
		t.Fatal(err)
	}
	var id int
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT id FROM %s`, tbl)).Scan(&id); err != nil || id != 7 {
		t.Errorf("existing data = %d, %v; want it left alone", id, err)
	}
}

func TestAFailingMigrationReportsAnErrorAndLeavesNothingHalfApplied(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc, tbl := uniq()
	t.Cleanup(func() {
		pool.Exec(ctx, "DROP TABLE IF EXISTS "+tbl+", schema_migrations_"+strings.ReplaceAll(svc, "-", "_"))
	})
	fsys := fstest.MapFS{"migrations/00001_bad.sql": migration(fmt.Sprintf(`CREATE TABLE %s (id INT); SELECT * FROM does_not_exist`, tbl))}

	_, err := Up(ctx, pool, svc, fsys)
	if err == nil || !strings.Contains(err.Error(), "does_not_exist") {
		t.Fatalf("Up = %v; want the SQL error from the migration", err)
	}
	if tableExists(t, pool, tbl) {
		t.Error("the failed migration's first statement survived; it should run in one transaction")
	}
}

func TestAtStartupHonoursRunMigrationsFalse(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	svc, tbl := uniq()
	t.Cleanup(func() {
		pool.Exec(ctx, "DROP TABLE IF EXISTS "+tbl+", schema_migrations_"+strings.ReplaceAll(svc, "-", "_"))
	})
	fsys := fstest.MapFS{"migrations/00001_create.sql": migration(fmt.Sprintf(`CREATE TABLE %s (id INT)`, tbl))}

	t.Setenv("RUN_MIGRATIONS", "false")
	if err := AtStartup(ctx, pool, svc, fsys); err != nil || tableExists(t, pool, tbl) {
		t.Fatalf("AtStartup with RUN_MIGRATIONS=false migrated (err %v)", err)
	}
	t.Setenv("RUN_MIGRATIONS", "")
	if err := AtStartup(ctx, pool, svc, fsys); err != nil || !tableExists(t, pool, tbl) {
		t.Fatalf("AtStartup by default did not migrate (err %v)", err)
	}
}
