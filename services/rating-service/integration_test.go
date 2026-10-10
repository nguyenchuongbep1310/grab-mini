package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"grab-mini/pkg/events"
)

// These tests need Postgres: set TEST_DATABASE_URL to a throwaway database
// (they delete rows).
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	db, err := connectDB(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(ctx, `DELETE FROM driver_reviews; DELETE FROM rating_events; DELETE FROM driver_ratings`); err != nil {
		t.Fatal(err)
	}
	return db
}

func rated(driver, trip string, stars int, tip int64) events.TripRated {
	return events.TripRated{
		Envelope: events.Envelope{EventID: newUUID()},
		TripID:   trip, DriverID: driver, RiderID: "r1", Stars: stars, Comment: "nice", TipAmount: tip, Currency: "VND",
	}
}

func TestRedeliveryDoesNotDoubleCountRatingsOrTips(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	evt := rated("d1", newUUID(), 5, 20000)

	for i, want := range []bool{true, false, false} {
		got, err := record(ctx, db, evt)
		if err != nil || got != want {
			t.Fatalf("delivery %d = %v, %v; want %v", i+1, got, err, want)
		}
	}
	if _, err := record(ctx, db, rated("d1", newUUID(), 3, 0)); err != nil {
		t.Fatal(err)
	}

	r, err := getRating(ctx, db, "d1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Count != 2 || r.Average != 4 || r.TotalTips != 20000 || r.TipCount != 1 {
		t.Errorf("rating = %+v, want count 2, average 4, totalTips 20000, tipCount 1", r)
	}
	var n int
	db.QueryRow(ctx, `SELECT count(*) FROM driver_reviews WHERE driver_id = 'd1'`).Scan(&n)
	if n != 2 {
		t.Errorf("%d review rows, want 2", n)
	}
}

func TestADifferentEventForTheSameTripIsNotCountedTwice(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	trip := newUUID()
	record(ctx, db, rated("d1", trip, 5, 10000))
	if counted, err := record(ctx, db, rated("d1", trip, 1, 50000)); err != nil || counted {
		t.Fatalf("second event for the trip = %v, %v; want false, nil", counted, err)
	}
	if r, _ := getRating(ctx, db, "d1"); r.Count != 1 || r.TotalTips != 10000 {
		t.Errorf("rating = %+v, want only the first", r)
	}
}

func TestReviewsComeNewestFirstAndPage(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		if _, err := record(ctx, db, rated("d1", newUUID(), i, int64(i)*1000)); err != nil {
			t.Fatal(err)
		}
	}
	record(ctx, db, rated("d2", newUUID(), 1, 0))

	page1, err := getReviews(ctx, db, "d1", 2, time.Time{})
	if err != nil || len(page1) != 2 || page1[0].Stars != 3 {
		t.Fatalf("page 1 = %+v, %v; want 2 newest first", page1, err)
	}
	page2, _ := getReviews(ctx, db, "d1", 2, page1[1].CreatedAt)
	if len(page2) != 1 || page2[0].Stars != 1 {
		t.Errorf("page 2 = %+v, want the oldest review", page2)
	}
}

func newUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6], b[8] = b[6]&0x0f|0x40, b[8]&0x3f|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
