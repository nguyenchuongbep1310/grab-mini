package postgres

import (
	"context"
	"testing"
)

func TestSeedDevAddsTheDemoProfilesOnceAndQueuesNothing(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	d, r, err := SeedDev(ctx, db)
	if err != nil || d != 5 || r != 3 {
		t.Fatalf("first run: %d drivers %d riders %v", d, r, err)
	}
	// Run again after an admin's decision: nothing is added and nothing is undone.
	if _, err := db.Exec(ctx, `UPDATE driver_profiles SET status = 'SUSPENDED' WHERE driver_id = 'driver-1'`); err != nil {
		t.Fatal(err)
	}
	d, r, err = SeedDev(ctx, db)
	if err != nil || d != 0 || r != 0 {
		t.Fatalf("second run: %d drivers %d riders %v", d, r, err)
	}
	var st string
	if err := db.QueryRow(ctx, `SELECT status FROM driver_profiles WHERE driver_id = 'driver-1'`).Scan(&st); err != nil || st != "SUSPENDED" {
		t.Fatal(st, err)
	}
	var approved, bikes int
	if err := db.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status = 'APPROVED' AND reviewed_by = 'seed-dev'), count(*) FILTER (WHERE vehicle_type = 'BIKE') FROM driver_profiles`).Scan(&approved, &bikes); err != nil || approved != 4 || bikes != 2 {
		t.Fatal(approved, bikes, err)
	}
	var queued int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&queued); err != nil || queued != 0 {
		t.Fatalf("queued %d: %v", queued, err)
	}
}

func TestTheMigrationsCreateTheTablesTheStoreUses(t *testing.T) {
	db := testDB(t)
	for _, tbl := range []string{"rider_profiles", "driver_profiles", "outbox"} {
		var ok bool
		if err := db.QueryRow(context.Background(), `SELECT to_regclass($1) IS NOT NULL`, tbl).Scan(&ok); err != nil || !ok {
			t.Errorf("table %s: %v %v", tbl, ok, err)
		}
	}
}
