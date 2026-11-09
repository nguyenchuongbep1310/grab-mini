package postgres

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/promotion-service/internal/domain"
)

// migrations is the service's own migrations directory.
func migrations() fs.FS { return os.DirFS("../../..") }

func count(t *testing.T, db *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func samplePromotion(code string) domain.Promotion {
	return domain.Promotion{
		Code: code, Kind: domain.KindFixed, Value: 10000, MinFare: 20000, PerRider: 1,
		ValidFrom: time.Now().Add(-time.Hour).Truncate(time.Microsecond),
		ValidTo:   time.Now().Add(time.Hour).Truncate(time.Microsecond),
	}
}

func TestTheMigrationsCreateTheTablesTheStoreUses(t *testing.T) {
	db := testDB(t)
	for _, table := range []string{"promotions", "redemptions", "promotion_events", "promotion_cancelled_orders", "outbox"} {
		if n := count(t, db, `SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, table); n != 1 {
			t.Errorf("table %s: found %d times, want once", table, n)
		}
	}
}

func TestCreateMapsAUniqueViolationToErrCodeExists(t *testing.T) {
	db := testDB(t)
	s := NewStore(db)
	ctx := context.Background()
	if err := s.Create(ctx, samplePromotion("TWICE"), "admin-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, samplePromotion("TWICE"), "admin-2"); !errors.Is(err, domain.ErrCodeExists) {
		t.Fatalf("second create = %v, want ErrCodeExists", err)
	}
	got, err := s.Find(ctx, "TWICE")
	if err != nil || got.Value != 10000 {
		t.Fatalf("Find = %+v, %v", got, err)
	}
	if _, err := s.Find(ctx, "NOPE"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Find of an unknown code = %v, want ErrNotFound", err)
	}
	if _, err := s.View(ctx, "NOPE"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("View of an unknown code = %v, want ErrNotFound", err)
	}
}

func TestDisableKeepsTheFirstTime(t *testing.T) {
	db := testDB(t)
	s := NewStore(db)
	ctx := context.Background()
	if err := s.Create(ctx, samplePromotion("OFF"), "a"); err != nil {
		t.Fatal(err)
	}
	first := time.Now().Add(-time.Minute).Truncate(time.Microsecond)
	if found, err := s.Disable(ctx, "OFF", first); err != nil || !found {
		t.Fatalf("Disable = %v, %v", found, err)
	}
	if found, err := s.Disable(ctx, "OFF", first.Add(time.Hour)); err != nil || !found {
		t.Fatalf("second Disable = %v, %v", found, err)
	}
	p, err := s.Find(ctx, "OFF")
	if err != nil || p.DisabledAt == nil || !p.DisabledAt.Equal(first) {
		t.Fatalf("disabled_at = %v, %v; want the first time %v", p.DisabledAt, err, first)
	}
	if found, err := s.Disable(ctx, "MISSING", first); err != nil || found {
		t.Fatalf("Disable of an unknown code = %v, %v; want found=false", found, err)
	}
}

func TestListIsNewestFirstAndCountsRedemptionsByStatus(t *testing.T) {
	db := testDB(t)
	s := NewStore(db)
	ctx := context.Background()
	for _, c := range []string{"AAA", "BBB"} {
		if err := s.Create(ctx, samplePromotion(c), "admin-1"); err != nil {
			t.Fatal(err)
		}
	}
	// Make BBB the older one.
	if _, err := db.Exec(ctx, `UPDATE promotions SET created_at = now() - interval '1 day' WHERE code = 'BBB'`); err != nil {
		t.Fatal(err)
	}
	for i, st := range []string{"REDEEMED", "REDEEMED", "OVER_LIMIT", "RELEASED"} {
		if _, err := db.Exec(ctx, `INSERT INTO redemptions (order_id, code, rider_id, discount, status, source_event_id) VALUES ($1, 'AAA', 'r', 1, $2, $3)`,
			string(rune('a'+i)), st, uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	list, err := s.List(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("List = %d rows, %v", len(list), err)
	}
	if list[0].Code != "AAA" || list[1].Code != "BBB" {
		t.Fatalf("order = %s, %s; want newest first", list[0].Code, list[1].Code)
	}
	if list[0].Redeemed != 2 || list[0].OverLimit != 1 || list[0].CreatedBy != "admin-1" {
		t.Fatalf("AAA = %+v", list[0])
	}
	v, err := s.View(ctx, "AAA")
	if err != nil || v.Redeemed != 2 || v.OverLimit != 1 {
		t.Fatalf("View = %+v, %v", v, err)
	}
	if list[1].Redeemed != 0 {
		t.Fatalf("BBB redeemed = %d, want 0", list[1].Redeemed)
	}
	u, err := s.Usage(ctx, "AAA", "r")
	if err != nil || u.Total != 2 || u.ByRider != 2 {
		t.Fatalf("Usage = %+v, %v", u, err)
	}
}

func TestSeedDevAddsTheDemoCodesOnce(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	n, err := SeedDev(ctx, db)
	if err != nil || n != 3 {
		t.Fatalf("first SeedDev = %d, %v; want 3", n, err)
	}
	if _, err := db.Exec(ctx, `UPDATE promotions SET disabled_at = now() WHERE code = 'FLAT15K'`); err != nil {
		t.Fatal(err)
	}
	n, err = SeedDev(ctx, db)
	if err != nil || n != 0 {
		t.Fatalf("second SeedDev = %d, %v; want 0", n, err)
	}
	if c := count(t, db, `SELECT count(*) FROM promotions WHERE code = 'FLAT15K' AND disabled_at IS NOT NULL`); c != 1 {
		t.Fatal("a re-seed re-enabled a disabled code")
	}
	p, err := NewStore(db).Find(ctx, "BIKE5K")
	if err != nil || p.VehicleType == nil || *p.VehicleType != "BIKE" {
		t.Fatalf("BIKE5K = %+v, %v", p, err)
	}
}
