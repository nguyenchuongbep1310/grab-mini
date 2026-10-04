package driverstatus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

const orderA = "order-a"

func TestClaimForOrderRecordsTheMatchAndLeasesTheDriver(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", Available)

	got, err := ClaimForOrder(ctx, rdb, orderA, "d1", "CAR", time.Minute)
	if err != nil || got != Claimed {
		t.Fatalf("ClaimForOrder = %v, %v; want Claimed", got, err)
	}
	if s, _ := Get(ctx, rdb, "d1"); s != Assigned {
		t.Errorf("status = %q, want ASSIGNED", s)
	}
	if d, _ := MatchedDriver(ctx, rdb, orderA); d != "d1" {
		t.Errorf("MatchedDriver = %q, want d1", d)
	}
}

func TestClaimForOrderOnAnAlreadyMatchedOrderClaimsNobodyElse(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", Available)
	seed(t, rdb, "d2", "CAR", Available)

	if got, _ := ClaimForOrder(ctx, rdb, orderA, "d1", "CAR", time.Minute); got != Claimed {
		t.Fatal("first claim failed")
	}
	// A redelivered order.created tries the next driver for the same order.
	got, err := ClaimForOrder(ctx, rdb, orderA, "d2", "CAR", time.Minute)
	if err != nil || got != AlreadyMatched {
		t.Fatalf("second claim = %v, %v; want AlreadyMatched", got, err)
	}
	if s, _ := Get(ctx, rdb, "d2"); s != Available {
		t.Errorf("d2 status = %q; a duplicate must not take a second driver", s)
	}
}

func TestClaimForOrderRefusesTheWrongVehicleAndBusyDrivers(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "bike", "BIKE", Available)
	seed(t, rdb, "busy", "CAR", OnTrip)

	for _, id := range []string{"bike", "busy"} {
		got, err := ClaimForOrder(ctx, rdb, orderA, id, "CAR", time.Minute)
		if err != nil || got != NotClaimed {
			t.Errorf("%s: ClaimForOrder = %v, %v; want NotClaimed", id, got, err)
		}
	}
	if d, _ := MatchedDriver(ctx, rdb, orderA); d != "" {
		t.Errorf("a refused claim recorded a match with %q", d)
	}
}

func TestAnAssignedDriverBecomesClaimableOnceTheLeaseRunsOut(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	ctx := context.Background()
	mr.SetTime(time.Now())
	seed(t, rdb, "d1", "CAR", Available)

	if got, _ := ClaimForOrder(ctx, rdb, "order-1", "d1", "CAR", time.Minute); got != Claimed {
		t.Fatal("first claim failed")
	}

	mr.SetTime(time.Now().Add(30 * time.Second))
	if got, _ := ClaimForOrder(ctx, rdb, "order-2", "d1", "CAR", time.Minute); got != NotClaimed {
		t.Errorf("claim inside the lease = %v, want NotClaimed", got)
	}

	mr.SetTime(time.Now().Add(2 * time.Minute))
	if got, _ := ClaimForOrder(ctx, rdb, "order-2", "d1", "CAR", time.Minute); got != Claimed {
		t.Errorf("claim after the lease = %v, want Claimed", got)
	}
}

func TestSettingOnTripEndsTheLease(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	ctx := context.Background()
	mr.SetTime(time.Now())
	seed(t, rdb, "d1", "CAR", Available)

	ClaimForOrder(ctx, rdb, "order-1", "d1", "CAR", time.Minute)
	if err := Set(ctx, rdb, "d1", OnTrip); err != nil {
		t.Fatal(err)
	}

	mr.SetTime(time.Now().Add(time.Hour))
	if got, _ := ClaimForOrder(ctx, rdb, "order-2", "d1", "CAR", time.Minute); got != NotClaimed {
		t.Errorf("a driver on a trip was claimed after the old lease expired (%v)", got)
	}
}

func TestReleaseMatchLetsTheOrderBeMatchedAgain(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", Available)

	ClaimForOrder(ctx, rdb, orderA, "d1", "CAR", time.Minute)
	if err := ReleaseMatch(ctx, rdb, orderA, "d1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := ClaimForOrder(ctx, rdb, orderA, "d1", "CAR", time.Minute); got != Claimed {
		t.Errorf("claim after release = %v, want Claimed", got)
	}
}

func TestOnlyOneConcurrentClaimForAnOrderWins(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		seed(t, rdb, fmt.Sprintf("d%d", i), "CAR", Available)
	}

	var claimed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if got, _ := ClaimForOrder(ctx, rdb, orderA, fmt.Sprintf("d%d", i), "CAR", time.Minute); got == Claimed {
				claimed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if claimed.Load() != 1 {
		t.Errorf("%d drivers claimed for one order, want 1", claimed.Load())
	}
}

func TestTransitionOnlyMovesADriverWhoIsInTheExpectedStatus(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", OnTrip)
	seed(t, rdb, "d2", "CAR", Assigned)

	if ok, err := Transition(ctx, rdb, "d1", OnTrip, Available); err != nil || !ok {
		t.Fatalf("Transition d1 = %v, %v; want true", ok, err)
	}
	if ok, _ := Transition(ctx, rdb, "d1", OnTrip, Available); ok {
		t.Error("a second Transition reported a change")
	}
	if ok, _ := Transition(ctx, rdb, "d2", OnTrip, Available); ok {
		t.Error("moved a driver who was ASSIGNED (a new claim) to AVAILABLE")
	}
	if s, _ := Get(ctx, rdb, "d2"); s != Assigned {
		t.Errorf("d2 = %s, want ASSIGNED untouched", s)
	}
}
