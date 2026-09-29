package driverstatus

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func seed(t *testing.T, rdb *redis.Client, id, vehicle string, status Status) {
	t.Helper()
	ctx := context.Background()
	if err := SetVehicle(ctx, rdb, id, vehicle); err != nil {
		t.Fatal(err)
	}
	if err := Set(ctx, rdb, id, status); err != nil {
		t.Fatal(err)
	}
}

func TestClaimMovesAnAvailableDriverOfTheRightVehicle(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", Available)

	ok, err := Claim(ctx, rdb, "d1", "CAR", Available, Assigned)
	if err != nil || !ok {
		t.Fatalf("Claim = %v, %v; want true, nil", ok, err)
	}
	if got, _ := Get(ctx, rdb, "d1"); got != Assigned {
		t.Errorf("status = %q, want ASSIGNED", got)
	}
}

func TestClaimRefusesTheWrongVehicle(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "bike-1", "BIKE", Available)

	ok, err := Claim(ctx, rdb, "bike-1", "CAR", Available, Assigned)
	if err != nil || ok {
		t.Fatalf("Claim = %v, %v; want false, nil", ok, err)
	}
	if got, _ := Get(ctx, rdb, "bike-1"); got != Available {
		t.Errorf("a refused claim must leave the driver AVAILABLE, got %q", got)
	}
}

func TestClaimRefusesADriverWhoIsNotAvailable(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", OnTrip)

	if ok, _ := Claim(ctx, rdb, "d1", "CAR", Available, Assigned); ok {
		t.Error("claimed a driver who is ON_TRIP")
	}
}

func TestClaimRefusesADriverWithNoVehicleRecorded(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	if err := Set(ctx, rdb, "ghost", Available); err != nil {
		t.Fatal(err)
	}
	if ok, _ := Claim(ctx, rdb, "ghost", "CAR", Available, Assigned); ok {
		t.Error("claimed a driver whose vehicle was never recorded")
	}
}

// Two matching instances racing for the same driver: exactly one may win.
func TestConcurrentClaimsHaveExactlyOneWinner(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", Available)

	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, err := Claim(ctx, rdb, "d1", "CAR", Available, Assigned); err == nil && ok {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()

	if wins.Load() != 1 {
		t.Errorf("%d claims succeeded, want exactly 1", wins.Load())
	}
}
