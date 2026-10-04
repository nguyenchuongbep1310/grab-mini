package driverlocation

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

// Need a real Redis (they FLUSH it): set TEST_REDIS_ADDR.
func newClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.FlushAll(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return rdb
}

func ids(c []Candidate) []string {
	out := make([]string, len(c))
	for i := range c {
		out[i] = c[i].ID
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSearchKeys(t *testing.T) {
	if got := searchKeys(0, 10.77, 106.70, 5); len(got) != 1 || got[0] != GeoKey {
		t.Errorf("unsharded keys = %v, want just %s", got, GeoKey)
	}
	// 0.05 degree cells (~5.5 km): a 1 km circle in the middle of a cell touches one.
	if got := searchKeys(0.05, 10.775, 106.725, 1); len(got) != 1 {
		t.Errorf("small circle mid-cell touches %v, want 1 cell", got)
	}
	// A circle on a cell corner touches four.
	if got := searchKeys(0.05, 10.75, 106.70, 1); len(got) != 4 {
		t.Errorf("circle on a corner touches %v, want 4 cells", got)
	}
}

func TestASearchSpanningTooManyCellsFailsInsteadOfSkippingDrivers(t *testing.T) {
	t.Setenv(shardEnv, "0.01")
	rdb := newClient(t)
	if _, err := Search(context.Background(), rdb, 10.75, 106.70, 5, 50); err == nil {
		t.Fatal("search over ~100 cells succeeded; it must fail rather than skip cells")
	}
}

// Sharded and unsharded layouts must answer identically, including for
// drivers on either side of a cell boundary and ones that cross it.
func TestSearchIsTheSameShardedOrNot(t *testing.T) {
	ctx := context.Background()
	drivers := []struct {
		id       string
		lat, lng float64
	}{
		{"a", 10.7500, 106.7000}, {"b", 10.7490, 106.7010}, {"c", 10.7515, 106.6985},
		{"d", 10.7800, 106.7300}, {"e", 10.7525, 106.7032}, {"far", 10.9000, 106.9000},
	}
	var want []string
	for _, deg := range []string{"", "0.05", "0.02"} {
		t.Setenv(shardEnv, deg)
		rdb := newClient(t)
		for i, d := range drivers {
			if ok, err := Update(ctx, rdb, d.id, d.lat, d.lng, int64(i+1)); err != nil || !ok {
				t.Fatalf("Update(%s) = %v, %v", d.id, ok, err)
			}
		}
		got, err := Search(ctx, rdb, 10.7500, 106.7000, 5, 50)
		if err != nil {
			t.Fatal(err)
		}
		if deg == "" {
			want = ids(got)
			if len(want) != 5 || want[0] != "a" {
				t.Fatalf("baseline = %v, want 5 drivers nearest-first starting with a", want)
			}
			continue
		}
		if !equal(ids(got), want) {
			t.Errorf("GEO_SHARD_DEGREES=%s: %v, want %v", deg, ids(got), want)
		}
		if top, _ := Search(ctx, rdb, 10.7500, 106.7000, 5, 2); len(top) != 2 || top[0].ID != "a" {
			t.Errorf("count=2 returned %v", ids(top))
		}
	}
}

func TestADriverWhoCrossesACellLeavesTheOldOne(t *testing.T) {
	ctx := context.Background()
	t.Setenv(shardEnv, "0.05")
	rdb := newClient(t)
	Update(ctx, rdb, "d1", 10.70, 106.70, 1)
	Update(ctx, rdb, "d1", 10.90, 106.90, 2) // ~30 km away: another cell

	if got, _ := Search(ctx, rdb, 10.70, 106.70, 3, 50); len(got) != 0 {
		t.Errorf("driver still found at the old position: %v", ids(got))
	}
	if got, _ := Search(ctx, rdb, 10.90, 106.90, 3, 50); len(got) != 1 {
		t.Errorf("driver not found at the new position: %v", ids(got))
	}
	// A late event for the old position must not move the driver back.
	if ok, _ := Update(ctx, rdb, "d1", 10.70, 106.70, 1); ok {
		t.Error("stale update applied")
	}
	if got, _ := Search(ctx, rdb, 10.70, 106.70, 3, 50); len(got) != 0 {
		t.Errorf("stale update moved the driver back: %v", ids(got))
	}
}

func TestClearRemovesEveryCell(t *testing.T) {
	ctx := context.Background()
	t.Setenv(shardEnv, "0.05")
	rdb := newClient(t)
	Update(ctx, rdb, "d1", 10.70, 106.70, 1)
	Update(ctx, rdb, "d2", 10.90, 106.90, 1)
	if err := Clear(ctx, rdb); err != nil {
		t.Fatal(err)
	}
	if n, _ := rdb.Keys(ctx, GeoKey+"*").Result(); len(n) != 0 {
		t.Errorf("keys left after Clear: %v", n)
	}
}
