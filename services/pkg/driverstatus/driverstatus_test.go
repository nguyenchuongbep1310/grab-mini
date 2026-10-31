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

func TestClaimFirstForOrderSkipsBusyAndWrongVehicleDrivers(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "busy", "CAR", OnTrip)
	seed(t, rdb, "bike", "BIKE", Available)
	seed(t, rdb, "free", "CAR", Available)
	seed(t, rdb, "later", "CAR", Available)

	got, res, err := ClaimFirstForOrder(ctx, rdb, orderA, []string{"busy", "bike", "free", "later"}, "CAR", time.Minute)
	if err != nil || res != Claimed || got != "free" {
		t.Fatalf("ClaimFirstForOrder = %q, %v, %v; want free, Claimed", got, res, err)
	}
	if s, _ := Get(ctx, rdb, "free"); s != Assigned {
		t.Errorf("free status = %q, want ASSIGNED", s)
	}
	if s, _ := Get(ctx, rdb, "later"); s != Available {
		t.Errorf("later status = %q, want AVAILABLE (only one driver is claimed)", s)
	}
	if d, _ := MatchedDriver(ctx, rdb, orderA); d != "free" {
		t.Errorf("MatchedDriver = %q, want free", d)
	}
	// Redelivery claims nobody else.
	if _, res, _ := ClaimFirstForOrder(ctx, rdb, orderA, []string{"later"}, "CAR", time.Minute); res != AlreadyMatched {
		t.Errorf("redelivery = %v, want AlreadyMatched", res)
	}
}

func TestClaimFirstForOrderWithNoClaimableDriver(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "busy", "CAR", OnTrip)
	got, res, err := ClaimFirstForOrder(ctx, rdb, orderA, []string{"busy"}, "CAR", time.Minute)
	if err != nil || res != NotClaimed || got != "" {
		t.Fatalf("= %q, %v, %v; want empty, NotClaimed", got, res, err)
	}
	if d, _ := MatchedDriver(ctx, rdb, orderA); d != "" {
		t.Errorf("no match should be recorded, got %q", d)
	}
}

func TestAvailableVehiclesListsOnlyAvailableDrivers(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "car-free", "CAR", Available)
	seed(t, rdb, "bike-free", "BIKE", Available)
	seed(t, rdb, "car-busy", "CAR", OnTrip)
	seed(t, rdb, "car-assigned", "CAR", Assigned)

	got, err := AvailableVehicles(ctx, rdb, []string{"car-free", "bike-free", "car-busy", "car-assigned", "ghost"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["car-free"] != "CAR" || got["bike-free"] != "BIKE" {
		t.Errorf("got %v, want the two AVAILABLE drivers with their vehicles", got)
	}
	if got, err := AvailableVehicles(ctx, rdb, nil); err != nil || len(got) != 0 {
		t.Errorf("no ids: got %v, %v", got, err)
	}
}

// ---------------------------------------------------------------------------
// Profile Service's review results: ApplyReview and the suspended key.

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func apply(t *testing.T, rdb *redis.Client, id string, approved bool, vehicle string, at time.Time) ReviewResult {
	t.Helper()
	got, err := ApplyReview(context.Background(), rdb, id, approved, vehicle, at)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func status(t *testing.T, rdb *redis.Client, id string) Status {
	t.Helper()
	s, err := Get(context.Background(), rdb, id)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestApprovingAnUnknownDriverMakesThemAvailableWithTheirVehicle(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()

	if got := apply(t, rdb, "new", true, "BIKE", t0); got != ReviewAvailable {
		t.Fatalf("ApplyReview = %v, want available", got)
	}
	if s := status(t, rdb, "new"); s != Available {
		t.Errorf("status = %q, want AVAILABLE", s)
	}
	if v, _ := rdb.Get(ctx, vehicleKey("new")).Result(); v != "BIKE" {
		t.Errorf("vehicle = %q, want BIKE", v)
	}
	// ...and a claim for that vehicle works, as for a seeded driver.
	if ok, _ := Claim(ctx, rdb, "new", "BIKE", Available, Assigned); !ok {
		t.Error("an approved driver could not be claimed")
	}
}

func TestApprovingADriverOnATripNeverMakesThemAvailable(t *testing.T) {
	for _, busy := range []Status{Assigned, OnTrip} {
		t.Run(string(busy), func(t *testing.T) {
			rdb := newClient(t)
			ctx := context.Background()
			seed(t, rdb, "d1", "CAR", busy)
			if busy == Assigned {
				// A live lease, as ClaimForOrder leaves it.
				rdb.Set(ctx, leaseKey("d1"), time.Now().Add(time.Hour).UnixMilli(), 0)
			}
			if got := apply(t, rdb, "d1", true, "CAR", t0); got != ReviewApprovedBusy {
				t.Fatalf("ApplyReview = %v, want approved_busy", got)
			}
			if s := status(t, rdb, "d1"); s != busy {
				t.Errorf("status = %q, want %s untouched", s, busy)
			}
		})
	}
}

func TestApprovingADriverWhoseAssignmentLeaseRanOutMakesThemAvailable(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", Assigned)
	rdb.Set(ctx, leaseKey("d1"), time.Now().Add(-time.Minute).UnixMilli(), 0)
	if got := apply(t, rdb, "d1", true, "CAR", t0); got != ReviewAvailable {
		t.Fatalf("ApplyReview = %v, want available", got)
	}
}

func TestApprovingAnOfflineDriverBringsThemBack(t *testing.T) {
	rdb := newClient(t)
	seed(t, rdb, "d1", "CAR", Offline)
	if got := apply(t, rdb, "d1", true, "CAR", t0); got != ReviewAvailable {
		t.Fatalf("ApplyReview = %v, want available", got)
	}
	if s := status(t, rdb, "d1"); s != Available {
		t.Errorf("status = %q", s)
	}
}

func TestSuspendingAFreeDriverTakesThemOutAndNothingClaimsThem(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", Available)

	if got := apply(t, rdb, "d1", false, "CAR", t0); got != ReviewOffline {
		t.Fatalf("ApplyReview = %v, want offline", got)
	}
	if s := status(t, rdb, "d1"); s != Offline {
		t.Errorf("status = %q, want OFFLINE", s)
	}
	if on, _ := Suspended(ctx, rdb, "d1"); !on {
		t.Error("the suspended key is not set")
	}
	if _, res, _ := ClaimFirstForOrder(ctx, rdb, "o-1", []string{"d1"}, "CAR", time.Minute); res == Claimed {
		t.Error("ClaimFirstForOrder claimed a suspended driver")
	}
	if res, _ := ClaimForOrder(ctx, rdb, "o-2", "d1", "CAR", time.Minute); res == Claimed {
		t.Error("ClaimForOrder claimed a suspended driver")
	}
	if ok, _ := Claim(ctx, rdb, "d1", "CAR", Available, Assigned); ok {
		t.Error("Claim claimed a suspended driver")
	}
}

// Even if something sets AVAILABLE again (a release, a reseed), a suspended driver stays out.
func TestGivingASuspendedDriverBackLeavesThemOffline(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", Available)
	apply(t, rdb, "d1", false, "CAR", t0)

	if err := Set(ctx, rdb, "d1", Available); err != nil {
		t.Fatal(err)
	}
	if s := status(t, rdb, "d1"); s != Offline {
		t.Errorf("Set(Available) of a suspended driver left %q, want OFFLINE", s)
	}
	Set(ctx, rdb, "d1", OnTrip)
	if ok, _ := Transition(ctx, rdb, "d1", OnTrip, Available); !ok {
		t.Fatal("Transition reported no change")
	}
	if s := status(t, rdb, "d1"); s != Offline {
		t.Errorf("Transition to AVAILABLE of a suspended driver left %q, want OFFLINE", s)
	}
}

// The driver who was on a trip when suspended finishes it, then is not offered another.
func TestASuspendedDriverFinishesTheTripThenStaysOut(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", Available)
	if res, _ := ClaimForOrder(ctx, rdb, "o-1", "d1", "CAR", time.Minute); res != Claimed {
		t.Fatal("setup claim failed")
	}
	if got := apply(t, rdb, "d1", false, "CAR", t0); got != ReviewAfterTrip {
		t.Fatalf("ApplyReview = %v, want after_trip", got)
	}
	if s := status(t, rdb, "d1"); s != Assigned {
		t.Fatalf("status = %q: the current order must not be taken away", s)
	}
	// The trip is created (ON_TRIP), then ends.
	Set(ctx, rdb, "d1", OnTrip)
	if ok, _ := Transition(ctx, rdb, "d1", OnTrip, Available); !ok {
		t.Fatal("release reported no change")
	}
	if s := status(t, rdb, "d1"); s != Offline {
		t.Errorf("after the trip: %q, want OFFLINE", s)
	}
	if _, res, _ := ClaimFirstForOrder(ctx, rdb, "o-2", []string{"d1"}, "CAR", time.Minute); res == Claimed {
		t.Error("offered a new order after the suspension")
	}
}

func TestReapprovingBringsASuspendedDriverBack(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", Available)
	apply(t, rdb, "d1", false, "CAR", t0)
	if got := apply(t, rdb, "d1", true, "BIKE", t0.Add(time.Hour)); got != ReviewAvailable {
		t.Fatalf("ApplyReview = %v, want available", got)
	}
	if on, _ := Suspended(ctx, rdb, "d1"); on {
		t.Error("still suspended after the approval")
	}
	if ok, _ := Claim(ctx, rdb, "d1", "BIKE", Available, Assigned); !ok {
		t.Error("not claimable with the new vehicle")
	}
}

func TestOlderAndRepeatedReviewResultsAreIgnored(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	seed(t, rdb, "d1", "CAR", Available)

	apply(t, rdb, "d1", false, "CAR", t0.Add(2*time.Second)) // suspended at +2s
	// The approval of +1s arrives late: it was decided before the suspension.
	if got := apply(t, rdb, "d1", true, "CAR", t0.Add(time.Second)); got != ReviewStale {
		t.Fatalf("late approval = %v, want stale", got)
	}
	if s := status(t, rdb, "d1"); s != Offline {
		t.Errorf("a late approval changed the status to %q", s)
	}
	if on, _ := Suspended(ctx, rdb, "d1"); !on {
		t.Error("a late approval lifted the suspension")
	}
	// The same result again (a redelivery) does nothing either.
	if got := apply(t, rdb, "d1", false, "CAR", t0.Add(2*time.Second)); got != ReviewStale {
		t.Errorf("redelivery = %v, want stale", got)
	}
	// A redelivered approval must not undo a later suspension, and must not pull an
	// assigned driver back to AVAILABLE.
	apply(t, rdb, "d2", true, "CAR", t0)
	ClaimForOrder(ctx, rdb, "o-1", "d2", "CAR", time.Minute)
	if got := apply(t, rdb, "d2", true, "CAR", t0); got != ReviewStale {
		t.Errorf("redelivered approval = %v, want stale", got)
	}
	if s := status(t, rdb, "d2"); s != Assigned {
		t.Errorf("a redelivered approval changed an ASSIGNED driver to %q", s)
	}
}

// A claim racing a suspension: whichever the script order picks, the outcomes agree. Either
// the claim won (the driver is ASSIGNED, suspended, and finishes the order) or the suspension
// did (OFFLINE, no claim). Never AVAILABLE and suspended, never claimed after being taken out.
func TestConcurrentClaimAndSuspendNeverLeaveAnEligibleSuspendedDriver(t *testing.T) {
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		rdb := newClient(t)
		seed(t, rdb, "d1", "CAR", Available)

		var claim ClaimResult
		var review ReviewResult
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			claim, _ = ClaimForOrder(ctx, rdb, "o-1", "d1", "CAR", time.Minute)
		}()
		go func() {
			defer wg.Done()
			<-start
			review, _ = ApplyReview(ctx, rdb, "d1", false, "CAR", t0)
		}()
		close(start)
		wg.Wait()

		s := status(t, rdb, "d1")
		switch {
		case claim == Claimed && review == ReviewAfterTrip && s == Assigned:
			// the claim ran first
		case claim != Claimed && review == ReviewOffline && s == Offline:
			// the suspension ran first
		default:
			t.Fatalf("iteration %d: claim = %v, review = %v, status = %q", i, claim, review, s)
		}
		if on, _ := Suspended(ctx, rdb, "d1"); !on {
			t.Fatalf("iteration %d: not suspended", i)
		}
	}
}

// Many claims against one suspension: after the suspension returned, none succeed.
func TestNoClaimSucceedsAfterTheSuspensionHasBeenApplied(t *testing.T) {
	rdb := newClient(t)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		seed(t, rdb, fmt.Sprintf("d%d", i), "CAR", Available)
	}
	apply(t, rdb, "d3", false, "CAR", t0)

	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if res, _ := ClaimForOrder(ctx, rdb, fmt.Sprintf("o-%d", i), "d3", "CAR", time.Minute); res == Claimed {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 0 {
		t.Errorf("%d claims of a suspended driver succeeded", wins.Load())
	}
}
