package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/profile-service/internal/app"
	"grab-mini/profile-service/internal/domain"
)

var (
	_ app.RiderStore       = (*Store)(nil)
	_ app.DriverReader     = (*Store)(nil)
	_ app.ApplicationQueue = (*Store)(nil)
	_ app.ResyncSource     = (*Store)(nil)
	_ app.UnitOfWork       = (*UnitOfWork)(nil)
	_ app.Tx               = (*tx)(nil)
	_ app.ResyncLocker     = (*ResyncLocker)(nil)
)

// insertDriver writes a row directly, in the zone of the test's choosing.
func insertDriver(t *testing.T, db *pgxpool.Pool, id, status string, statusAt time.Time) {
	t.Helper()
	name, vehicle, plate := "", "", ""
	if status != domain.StatusNone {
		name, vehicle, plate = "Name "+id, "CAR", "PL-"+id
	}
	if _, err := db.Exec(context.Background(), `
		INSERT INTO driver_profiles (driver_id, name, vehicle_type, plate, status, status_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $6)`, id, name, vehicle, plate, status, statusAt); err != nil {
		t.Fatal(err)
	}
}

func TestTimestampsComeBackInUTC(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	s := NewStore(db)
	zone := time.FixedZone("UTC+7", 7*3600)
	at := time.Date(2026, 3, 4, 12, 0, 0, 123456000, zone)
	if _, err := db.Exec(ctx, `
		INSERT INTO driver_profiles (driver_id, name, vehicle_type, plate, status, reviewed_at, status_at, updated_at)
		VALUES ('d1', 'N', 'CAR', 'P', 'APPROVED', $1, $1, $1)`, at); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveRider(ctx, domain.Rider{ID: "r1", Name: "R", UpdatedAt: at}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `SET TIME ZONE 'Asia/Ho_Chi_Minh'`); err != nil {
		t.Fatal(err)
	}
	d, err := s.Driver(ctx, "d1")
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]time.Time{"reviewed_at": *d.ReviewedAt, "status_at": d.StatusAt, "updated_at": d.UpdatedAt} {
		if got.Location() != time.UTC || !got.Equal(at) {
			t.Errorf("%s = %v (%v)", name, got, got.Location())
		}
	}
	r, ok, err := s.Rider(ctx, "r1")
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if r.UpdatedAt.Location() != time.UTC || !r.UpdatedAt.Equal(at) {
		t.Errorf("rider updated_at = %v (%v)", r.UpdatedAt, r.UpdatedAt.Location())
	}
}

func TestADriverWithNoRowIsErrNoDriver(t *testing.T) {
	s := NewStore(testDB(t))
	if _, err := s.Driver(context.Background(), "nobody"); !errors.Is(err, domain.ErrNoDriver) {
		t.Fatalf("got %v", err)
	}
}

func TestTheQueueIsOldestStatusFirstTiedByDriverIDAndResumesAfterACursor(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	s := NewStore(db)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	insertDriver(t, db, "b", domain.StatusPending, t0)
	insertDriver(t, db, "a", domain.StatusPending, t0)
	insertDriver(t, db, "c", domain.StatusPending, t0.Add(-time.Hour))
	insertDriver(t, db, "z", domain.StatusPending, t0.Add(time.Hour))
	insertDriver(t, db, "x", domain.StatusApproved, t0.Add(-2*time.Hour))
	ids := func(ds []domain.Driver) string {
		var out string
		for _, d := range ds {
			out += d.ID
		}
		return out
	}
	all, err := s.ByStatus(ctx, domain.StatusPending, 10, nil)
	if err != nil || ids(all) != "cabz" {
		t.Fatalf("%q %v", ids(all), err)
	}
	page, err := s.ByStatus(ctx, domain.StatusPending, 2, nil)
	if err != nil || ids(page) != "ca" {
		t.Fatalf("%q %v", ids(page), err)
	}
	rest, err := s.ByStatus(ctx, domain.StatusPending, 10, &domain.Cursor{At: page[1].StatusAt, ID: page[1].ID})
	if err != nil || ids(rest) != "bz" {
		t.Fatalf("%q %v", ids(rest), err)
	}
	none, err := s.ByStatus(ctx, domain.StatusSuspended, 10, nil)
	if err != nil || len(none) != 0 {
		t.Fatalf("%v %v", none, err)
	}
}

func TestResyncableIDsListsOnlyApprovedRejectedSuspendedAfterTheGivenID(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	s := NewStore(db)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, st := range []string{"NONE", "PENDING", "APPROVED", "REJECTED", "SUSPENDED", "APPROVED", "PENDING"} {
		insertDriver(t, db, fmt.Sprintf("d%d", i), st, t0)
	}
	got, err := s.ResyncableIDs(ctx, "", 10)
	if err != nil || fmt.Sprint(got) != "[d2 d3 d4 d5]" {
		t.Fatalf("%v %v", got, err)
	}
	got, err = s.ResyncableIDs(ctx, "d2", 2)
	if err != nil || fmt.Sprint(got) != "[d3 d4]" {
		t.Fatalf("%v %v", got, err)
	}
	got, err = s.ResyncableIDs(ctx, "d5", 2)
	if err != nil || len(got) != 0 {
		t.Fatalf("%v %v", got, err)
	}
}

func TestRiderUpsertReplacesNameAndPhone(t *testing.T) {
	s := NewStore(testDB(t))
	ctx := context.Background()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Minute)
	for _, r := range []domain.Rider{{ID: "r1", Name: "First", Phone: "1", UpdatedAt: t1}, {ID: "r1", Name: "Second", Phone: "", UpdatedAt: t2}} {
		if err := s.SaveRider(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := s.Rider(ctx, "r1")
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if got.Name != "Second" || got.Phone != "" || !got.UpdatedAt.Equal(t2) || got.ID != "r1" {
		t.Fatalf("%+v", got)
	}
}

func TestAMissingRiderIsReportedNotFound(t *testing.T) {
	s := NewStore(testDB(t))
	r, ok, err := s.Rider(context.Background(), "ghost")
	if err != nil || ok {
		t.Fatalf("ok %v err %v", ok, err)
	}
	if r.ID != "ghost" || r.Name != "" {
		t.Fatalf("%+v", r)
	}
}
