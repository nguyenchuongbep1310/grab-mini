package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"grab-mini/trip-service/internal/domain"
)

var (
	ctx    = context.Background()
	t0     = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	rider  = domain.Actor{ID: "rider-1", Role: domain.RoleRider}
	driver = domain.Actor{ID: "driver-1", Role: domain.RoleDriver}
	pickup = domain.LatLng{Lat: 10.7760, Lng: 106.7000}
	drop   = domain.LatLng{Lat: 10.7900, Lng: 106.7100}
)

func trip(status domain.Status) domain.Trip {
	return domain.Trip{ID: "6f9619ff-8b86-d011-b42d-00c04fc964ff", OrderID: "o1", DriverID: "driver-1", RiderID: "rider-1", Status: status}
}

func off() domain.Proximity { p := domain.DefaultProximity(); p.Off = true; return p }

func northOf(p domain.LatLng, m float64) domain.LatLng {
	return domain.LatLng{Lat: p.Lat + m/111_195, Lng: p.Lng}
}

// ---- the state machine through the use cases

func TestPickupMovesTheTripAndQueuesItsEventTogether(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.Assigned))
	svc := w.service(off(), t0)

	got, err := svc.Pickup(ctx, driver, tr.ID)
	if err != nil || got.Status != domain.InProgress {
		t.Fatalf("pickup = %+v, %v", got, err)
	}
	if w.trips[tr.ID].Status != domain.InProgress {
		t.Errorf("stored status = %s", w.trips[tr.ID].Status)
	}
	if len(w.outbox) != 1 || w.outbox[0].Topic != "trip.status.changed" || w.outbox[0].Key != tr.ID {
		t.Errorf("outbox = %+v, want one status event keyed by the trip", w.outbox)
	}
	if len(w.transitioned) != 1 || w.transitioned[0] != "IN_PROGRESS" {
		t.Errorf("counted %v", w.transitioned)
	}
}

func TestARepeatedStepIsASuccessThatChangesAndQueuesNothing(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.InProgress))
	svc := w.service(domain.DefaultProximity(), t0) // proximity on, no location: a repeat must not need it

	got, err := svc.Pickup(ctx, driver, tr.ID)
	if err != nil || got.Status != domain.InProgress {
		t.Fatalf("repeated pickup = %+v, %v", got, err)
	}
	if len(w.outbox) != 0 || len(w.transitioned) != 0 || len(w.refused) != 0 {
		t.Errorf("a repeat queued %v, counted %v, refused %v", w.outbox, w.transitioned, w.refused)
	}
}

func TestARejectedTransitionQueuesNothingAndIsNotCounted(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.Assigned))
	svc := w.service(off(), t0)

	_, err := svc.Complete(ctx, driver, tr.ID) // needs IN_PROGRESS
	var cannot *CannotTransitionError
	if !errors.As(err, &cannot) || cannot.To != domain.Completed {
		t.Fatalf("complete from ASSIGNED = %v, want CannotTransitionError to COMPLETED", err)
	}
	if len(w.outbox) != 0 || len(w.transitioned) != 0 {
		t.Errorf("queued %v, counted %v", w.outbox, w.transitioned)
	}
	if w.trips[tr.ID].Status != domain.Assigned {
		t.Error("the trip moved")
	}
}

func TestACompletedTripCannotBeCancelledByEitherSide(t *testing.T) {
	for _, who := range []domain.Actor{rider, driver} {
		w := newWorld()
		tr := w.add(trip(domain.Completed))
		var cannot *CannotTransitionError
		if _, err := w.service(off(), t0).Cancel(ctx, who, tr.ID, ""); !errors.As(err, &cannot) {
			t.Errorf("%+v: err = %v, want a refused transition", who, err)
		}
		if len(w.outbox) != 0 {
			t.Errorf("queued %v", w.outbox)
		}
	}
}

func TestNotYoursIsNotFound(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.Assigned))
	svc := w.service(off(), t0)
	stranger := domain.Actor{ID: "driver-2", Role: domain.RoleDriver}
	otherRider := domain.Actor{ID: "rider-2", Role: domain.RoleRider}

	for name, err := range map[string]error{
		"another driver picks up":   second(svc.Pickup(ctx, stranger, tr.ID)),
		"the rider picks up":        second(svc.Pickup(ctx, rider, tr.ID)),
		"another rider cancels":     second(svc.Cancel(ctx, otherRider, tr.ID, "")),
		"an unknown trip":           second(svc.Pickup(ctx, driver, "nope")),
		"the rider completes":       second(svc.Complete(ctx, rider, tr.ID)),
		"a stranger reads the trip": second(svc.Trip(ctx, stranger, tr.ID)),
		"a stranger reads by order": second(svc.TripByOrder(ctx, otherRider, "o1")),
	} {
		if !errors.Is(err, ErrTripNotFound) {
			t.Errorf("%s: err = %v, want not found", name, err)
		}
	}
	if w.trips[tr.ID].Status != domain.Assigned || len(w.outbox) != 0 {
		t.Error("a refused request changed something")
	}
}

func second[T any](_ T, err error) error { return err }

func TestCancelNamesWhoCancelledOnTheRecordAndTheEvent(t *testing.T) {
	for who, want := range map[string]domain.Actor{domain.CancelledByRider: rider, domain.CancelledByDriver: driver} {
		w := newWorld()
		tr := w.add(trip(domain.InProgress))
		if _, err := w.service(off(), t0).Cancel(ctx, want, tr.ID, " too slow "); err != nil {
			t.Fatal(err)
		}
		got := w.trips[tr.ID]
		if got.Status != domain.Cancelled || got.CancelledBy != who || got.CancelReason != "too slow" {
			t.Errorf("%s: trip = %+v", who, got)
		}
		if len(w.outbox) != 1 || w.outbox[0].Payload != "CANCELLED/"+who+"/too slow" {
			t.Errorf("%s: outbox = %+v", who, w.outbox)
		}
	}
}

func TestACancelWithATooLongReasonIsRefusedBeforeAnythingIsRead(t *testing.T) {
	w := newWorld() // no trip at all: the reason is checked first, so this is a 400 and not a 404
	_, err := w.service(off(), t0).Cancel(ctx, rider, "nope", strings.Repeat("x", 201))
	var ve *domain.ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("err = %v, want a ValidationError", err)
	}
}

func TestCancellingTwiceKeepsTheFirstCancellation(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.Assigned))
	svc := w.service(off(), t0)
	for _, c := range []struct {
		who    domain.Actor
		reason string
	}{{rider, "a"}, {rider, "a"}, {driver, "b"}} {
		if _, err := svc.Cancel(ctx, c.who, tr.ID, c.reason); err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
	}
	if got := w.trips[tr.ID]; got.CancelledBy != domain.CancelledByRider || got.CancelReason != "a" {
		t.Errorf("trip = %+v, want the first cancellation kept", got)
	}
	if len(w.outbox) != 1 {
		t.Errorf("%d events, want 1", len(w.outbox))
	}
}

func TestCompleteFreesTheDriverAndReportsTheStoredTime(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.InProgress))
	w.driverStatus["driver-1"] = domain.DriverOnTrip

	got, err := w.service(off(), t0).Complete(ctx, driver, tr.ID)
	if err != nil || got.CompletedAt == nil || !got.CompletedAt.Equal(*w.trips[tr.ID].CompletedAt) {
		t.Fatalf("complete = %+v, %v; want the stored completedAt", got, err)
	}
	if w.driverStatus["driver-1"] != domain.DriverAvailable {
		t.Errorf("driver is %s", w.driverStatus["driver-1"])
	}
}

func TestAPickupDoesNotTouchTheDriverStatus(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.Assigned))
	w.driverStatus["driver-1"] = domain.DriverOnTrip
	if _, err := w.service(off(), t0).Pickup(ctx, driver, tr.ID); err != nil {
		t.Fatal(err)
	}
	if w.driverStatus["driver-1"] != domain.DriverOnTrip {
		t.Errorf("driver is %s", w.driverStatus["driver-1"])
	}
}

func TestAFailedDriverReleaseDoesNotUndoTheTransition(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.Assigned))
	w.setErr = errBoom
	// The release step after a cancel fails; the trip is cancelled all the same
	// (the trip.status.changed consumer is what makes the release certain).
	if _, err := w.service(off(), t0).Cancel(ctx, rider, tr.ID, ""); err != nil {
		t.Fatalf("cancel = %v", err)
	}
	if w.trips[tr.ID].Status != domain.Cancelled {
		t.Error("not cancelled")
	}
}

func TestIfTheEventCannotBeEncodedTheTripDoesNotMove(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.Assigned))
	w.encodeErr = errBoom
	if _, err := w.service(off(), t0).Pickup(ctx, driver, tr.ID); !errors.Is(err, ErrInternal) {
		t.Errorf("err = %v, want ErrInternal", err)
	}
	if w.trips[tr.ID].Status != domain.Assigned || len(w.outbox) != 0 {
		t.Error("state changed without an event")
	}
}

// ---- proximity through the use case

func proxWorld(driverAt *domain.Position) (*world, domain.Trip) {
	w := newWorld()
	tr := w.add(trip(domain.Assigned))
	w.routes[tr.ID] = domain.Route{Pickup: &pickup, Dropoff: &drop}
	if driverAt != nil {
		w.positions["driver-1"] = *driverAt
	}
	return w, tr
}

func pos(p domain.LatLng, age time.Duration) *domain.Position {
	return &domain.Position{Lat: p.Lat, Lng: p.Lng, At: t0.Add(-age)}
}

func TestPickupIsRefusedFromFarAwayAndCounted(t *testing.T) {
	w, tr := proxWorld(pos(northOf(pickup, 1500), time.Second))
	_, err := w.service(domain.DefaultProximity(), t0).Pickup(ctx, driver, tr.ID)
	var refusal *domain.Refusal
	if !errors.As(err, &refusal) || refusal.Reason != domain.ReasonTooFar {
		t.Fatalf("err = %v, want a too_far refusal", err)
	}
	if len(w.refused) != 1 || w.refused[0] != "pickup/too_far" {
		t.Errorf("refusals counted: %v", w.refused)
	}
	if w.trips[tr.ID].Status != domain.Assigned || len(w.outbox) != 0 {
		t.Error("a refused pickup changed something")
	}
}

func TestPickupNearThePickupPointWorks(t *testing.T) {
	w, tr := proxWorld(pos(northOf(pickup, 150), time.Second))
	if _, err := w.service(domain.DefaultProximity(), t0).Pickup(ctx, driver, tr.ID); err != nil {
		t.Fatal(err)
	}
	if len(w.refused) != 0 {
		t.Errorf("refusals counted: %v", w.refused)
	}
}

func TestCompleteChecksTheDropoffRadius(t *testing.T) {
	w, tr := proxWorld(pos(northOf(drop, 250), time.Second)) // inside 300, outside 200
	tr.Status = domain.InProgress
	w.trips[tr.ID] = tr
	if _, err := w.service(domain.DefaultProximity(), t0).Complete(ctx, driver, tr.ID); err != nil {
		t.Fatalf("250 m from the dropoff: %v", err)
	}
}

func TestRefusalsByWhyTheLocationCannotBeTrusted(t *testing.T) {
	for _, c := range []struct {
		name   string
		at     *domain.Position
		locErr error
		reason string
	}{
		{"no location", nil, nil, domain.ReasonLocationUnknown},
		{"a stale location", pos(pickup, 5*time.Minute), nil, domain.ReasonLocationStale},
		{"redis down", nil, errBoom, domain.ReasonLocationUnavailable},
	} {
		w, tr := proxWorld(c.at)
		w.locErr = c.locErr
		_, err := w.service(domain.DefaultProximity(), t0).Pickup(ctx, driver, tr.ID)
		var refusal *domain.Refusal
		if !errors.As(err, &refusal) || refusal.Reason != c.reason {
			t.Errorf("%s: err = %v, want %s", c.name, err, c.reason)
		}
		if len(w.refused) != 1 || w.refused[0] != "pickup/"+c.reason {
			t.Errorf("%s: refusals counted: %v", c.name, w.refused)
		}
	}
}

func TestTheProximityConfigurationIsHonoured(t *testing.T) {
	w, tr := proxWorld(pos(northOf(pickup, 230), time.Second))
	if _, err := w.service(domain.DefaultProximity(), t0).Pickup(ctx, driver, tr.ID); err == nil {
		t.Fatal("230 m accepted with the default 200 m")
	}
	wide := domain.DefaultProximity()
	wide.PickupRadiusM = 250
	if _, err := w.service(wide, t0).Pickup(ctx, driver, tr.ID); err != nil {
		t.Errorf("230 m refused with 250 m: %v", err)
	}
	// A zero value means the defaults (the trip is back to ASSIGNED: the call above moved it).
	w.trips[tr.ID] = tr
	if _, err := w.service(domain.Proximity{}, t0).Pickup(ctx, driver, tr.ID); err == nil {
		t.Error("230 m accepted with a zero (default) configuration")
	}
}

func TestATripWithoutARecordedRouteSkipsTheCheck(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.Assigned)) // no route recorded, no location
	if _, err := w.service(domain.DefaultProximity(), t0).Pickup(ctx, driver, tr.ID); err != nil {
		t.Errorf("trip with no route: %v", err)
	}
	w, tr = proxWorld(nil)
	if _, err := w.service(off(), t0).Pickup(ctx, driver, tr.ID); err != nil {
		t.Errorf("check off: %v", err)
	}
}

func TestAnUnreadableRouteIsAnInternalError(t *testing.T) {
	w, tr := proxWorld(nil)
	w.routeErr = errBoom
	if _, err := w.service(domain.DefaultProximity(), t0).Pickup(ctx, driver, tr.ID); !errors.Is(err, ErrInternal) {
		t.Errorf("err = %v, want ErrInternal", err)
	}
}

// ---- reads

func TestActiveTripIsForTheDriverThemselvesOnly(t *testing.T) {
	w := newWorld()
	w.add(trip(domain.Assigned))
	svc := w.service(off(), t0)
	if got, err := svc.ActiveTrip(ctx, driver, "driver-1"); err != nil || got.OrderID != "o1" {
		t.Errorf("own active trip = %+v, %v", got, err)
	}
	for name, who := range map[string]domain.Actor{
		"another driver": {ID: "driver-2", Role: domain.RoleDriver},
		"a rider":        rider,
		"no side":        {ID: "driver-1"},
	} {
		if _, err := svc.ActiveTrip(ctx, who, "driver-1"); !errors.Is(err, ErrForbidden) {
			t.Errorf("%s: err = %v, want ErrForbidden", name, err)
		}
	}
	w.trips = map[string]domain.Trip{}
	if _, err := svc.ActiveTrip(ctx, driver, "driver-1"); !errors.Is(err, ErrTripNotFound) {
		t.Errorf("no active trip: %v", err)
	}
}

// ---- ratings and receipts

func completed() domain.Trip { return trip(domain.Completed) }

func TestARatingIsSavedAndAnnouncedOnceAndCounted(t *testing.T) {
	w := newWorld()
	tr := w.add(completed())
	svc := w.service(off(), t0)

	got, err := svc.RateTrip(ctx, rider, tr.ID, RateInput{Stars: 5, Comment: " ok ", Tip: 20000})
	if err != nil || got.Stars != 5 || got.Tip != 20000 || got.TripID != tr.ID {
		t.Fatalf("rating = %+v, %v", got, err)
	}
	if len(w.outbox) != 1 || w.outbox[0].Topic != "trip.rated" || w.outbox[0].Key != "driver-1" || w.rated != 1 {
		t.Errorf("outbox %+v, counted %d", w.outbox, w.rated)
	}
	if _, err := svc.RateTrip(ctx, rider, tr.ID, RateInput{Stars: 4}); !errors.Is(err, ErrAlreadyRated) {
		t.Errorf("second rating = %v, want ErrAlreadyRated", err)
	}
	if len(w.outbox) != 1 || w.rated != 1 {
		t.Errorf("a second rating queued %+v, counted %d", w.outbox, w.rated)
	}
}

func TestRatingIsValidatedBeforeAnythingElse(t *testing.T) {
	w := newWorld() // no trip, and a malformed id: validation still comes first
	svc := w.service(off(), t0)
	var ve *domain.ValidationError
	if _, err := svc.RateTrip(ctx, rider, "not-a-uuid", RateInput{Stars: 0}); !errors.As(err, &ve) {
		t.Errorf("bad stars: %v", err)
	}
	if _, err := svc.RateTrip(ctx, rider, "not-a-uuid", RateInput{Stars: 5, RiderID: "rider-2"}); !errors.Is(err, ErrRiderMismatch) {
		t.Errorf("naming someone else: %v", err)
	}
	if _, err := svc.RateTrip(ctx, rider, "not-a-uuid", RateInput{Stars: 5, RiderID: "rider-1"}); !errors.Is(err, ErrTripNotFound) {
		t.Errorf("naming themself, malformed id: %v", err)
	}
}

func TestOnlyTheRiderOfACompletedTripCanRateIt(t *testing.T) {
	w := newWorld()
	done := w.add(completed())
	svc := w.service(off(), t0)
	if _, err := svc.RateTrip(ctx, domain.Actor{ID: "rider-2", Role: domain.RoleRider}, done.ID, RateInput{Stars: 5}); !errors.Is(err, ErrTripNotFound) {
		t.Errorf("another rider: %v", err)
	}
	if _, err := svc.RateTrip(ctx, driver, done.ID, RateInput{Stars: 5}); !errors.Is(err, ErrTripNotFound) {
		t.Errorf("the driver: %v", err)
	}
	w2 := newWorld()
	open := w2.add(trip(domain.InProgress))
	if _, err := w2.service(off(), t0).RateTrip(ctx, rider, open.ID, RateInput{Stars: 5}); !errors.Is(err, ErrNotCompleted) {
		t.Errorf("an unfinished trip: %v", err)
	}
}

func TestNoRatingWithoutItsEvent(t *testing.T) {
	w := newWorld()
	tr := w.add(completed())
	w.encodeErr = errBoom
	if _, err := w.service(off(), t0).RateTrip(ctx, rider, tr.ID, RateInput{Stars: 5}); !errors.Is(err, ErrInternal) {
		t.Errorf("err = %v", err)
	}
	if w.trips[tr.ID].Rating != nil {
		t.Error("rated without an event")
	}
	w.encodeErr = nil
	w.saveErr = errBoom
	if _, err := w.service(off(), t0).RateTrip(ctx, rider, tr.ID, RateInput{Stars: 5}); !errors.Is(err, ErrInternal) {
		t.Errorf("save failure: %v", err)
	}
	if len(w.outbox) != 0 || w.rated != 0 {
		t.Errorf("queued %+v, counted %d", w.outbox, w.rated)
	}
}

func TestClosingAReceipt(t *testing.T) {
	w := newWorld()
	open := w.add(trip(domain.Assigned))
	svc := w.service(off(), t0)
	if _, err := svc.CloseReceipt(ctx, rider, open.ID); !errors.Is(err, ErrNotCompleted) {
		t.Errorf("an unfinished trip: %v", err)
	}
	if _, err := svc.CloseReceipt(ctx, rider, "nope"); !errors.Is(err, ErrTripNotFound) {
		t.Errorf("malformed id: %v", err)
	}
	done := w.add(domain.Trip{ID: "11111111-1111-1111-1111-111111111111", OrderID: "o2", DriverID: "driver-1", RiderID: "rider-1", Status: domain.Completed})
	if _, err := svc.CloseReceipt(ctx, driver, done.ID); !errors.Is(err, ErrTripNotFound) {
		t.Errorf("the driver: %v", err)
	}
	a, err := svc.CloseReceipt(ctx, rider, done.ID)
	b, err2 := svc.CloseReceipt(ctx, rider, done.ID)
	if err != nil || err2 != nil || !a.At.Equal(b.At) || a.TripID != done.ID {
		t.Errorf("closing twice = %+v %v / %+v %v; want the same time", a, err, b, err2)
	}
}

// ---- consumers

func matched(orderID, eventID string) OrderMatched {
	return OrderMatched{EventID: eventID, OrderID: orderID, DriverID: "driver-1", RiderID: "rider-1", Pickup: &pickup, Dropoff: &drop}
}

func TestMatchedOrderCreatesATripAndConfirmsTheDriver(t *testing.T) {
	w := newWorld()
	w.driverStatus["driver-1"] = domain.DriverAssigned
	if err := w.service(off(), t0).OrderMatched(ctx, matched("o1", "e1")); err != nil {
		t.Fatal(err)
	}
	tr, err := w.ByOrderID(ctx, "o1")
	if err != nil || tr.Status != domain.Assigned || tr.DriverID != "driver-1" || tr.RiderID != "rider-1" {
		t.Fatalf("trip = %+v, %v", tr, err)
	}
	if w.driverStatus["driver-1"] != domain.DriverOnTrip {
		t.Errorf("driver is %s, want ON_TRIP", w.driverStatus["driver-1"])
	}
	if r := w.routes[tr.ID]; r.Pickup == nil || *r.Pickup != pickup || *r.Dropoff != drop {
		t.Errorf("route = %+v", r)
	}
}

func TestReplayingAMatchedEventNeverRevertsAFinishedTripsDriver(t *testing.T) {
	w := newWorld()
	svc := w.service(off(), t0)
	if err := svc.OrderMatched(ctx, matched("o1", "e1")); err != nil {
		t.Fatal(err)
	}
	w.driverStatus["driver-1"] = domain.DriverAvailable                          // the trip ended; the driver was released
	for _, e := range []OrderMatched{matched("o1", "e1"), matched("o1", "e2")} { // a replay, then a re-announcement
		if err := svc.OrderMatched(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if w.driverStatus["driver-1"] != domain.DriverAvailable {
		t.Errorf("driver is %s after a replay", w.driverStatus["driver-1"])
	}
	if len(w.trips) != 1 {
		t.Errorf("%d trips for one order", len(w.trips))
	}
}

func TestADuplicateEventWithoutATripSavesNothing(t *testing.T) {
	// The same event id seen before (its trip was removed): nothing is created.
	w := newWorld()
	w.processed["e1"] = true
	if err := w.service(off(), t0).OrderMatched(ctx, matched("o1", "e1")); err != nil {
		t.Fatal(err)
	}
	if len(w.trips) != 0 {
		t.Errorf("trips = %v", w.trips)
	}
}

func TestALateMatchedEventForACancelledOrderCreatesNoTripAndReleasesTheClaim(t *testing.T) {
	w := newWorld()
	w.cancelled["o1"] = true
	w.driverStatus["driver-1"] = domain.DriverAssigned // Matching's claim
	if err := w.service(off(), t0).OrderMatched(ctx, matched("o1", "e1")); err != nil {
		t.Fatal(err)
	}
	if len(w.trips) != 0 {
		t.Error("a trip was created for a cancelled order")
	}
	if w.driverStatus["driver-1"] != domain.DriverAvailable {
		t.Errorf("driver is %s, want AVAILABLE", w.driverStatus["driver-1"])
	}
}

// The cancellation lands between the check and the lock: the transaction sees
// it, creates nothing, and hands the driver back from ON_TRIP (where it had
// just been put).
func TestACancellationThatLandsBeforeTheLockStopsTheTripFromBeingCreated(t *testing.T) {
	w := newWorld()
	w.beforeLock = func() { w.cancelled["o1"] = true }
	if err := w.service(off(), t0).OrderMatched(ctx, matched("o1", "e1")); err != nil {
		t.Fatal(err)
	}
	if len(w.trips) != 0 || w.processed["e1"] {
		t.Errorf("trips %v, processed %v: the unit of work should have rolled back", w.trips, w.processed)
	}
	if w.driverStatus["driver-1"] != domain.DriverAvailable {
		t.Errorf("driver is %s, want AVAILABLE", w.driverStatus["driver-1"])
	}
}

func cancelEvt(orderID string) OrderCancelled {
	return OrderCancelled{OrderID: orderID, CancelledBy: domain.CancelledByRider, Reason: "changed my mind"}
}

func TestAnOrderCancelledEventCancelsItsExistingTrip(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.Assigned))
	if err := w.service(off(), t0).OrderCancelled(ctx, cancelEvt("o1")); err != nil {
		t.Fatal(err)
	}
	got := w.trips[tr.ID]
	if got.Status != domain.Cancelled || got.CancelledBy != domain.CancelledByRider || got.CancelReason != "changed my mind" {
		t.Fatalf("trip = %+v", got)
	}
	if len(w.outbox) != 1 || w.outbox[0].Key != tr.ID || w.outbox[0].Payload != "CANCELLED/RIDER/changed my mind" {
		t.Errorf("outbox = %+v", w.outbox)
	}
	if !w.cancelled["o1"] || len(w.transitioned) != 1 {
		t.Errorf("cancelled %v, counted %v", w.cancelled, w.transitioned)
	}
}

func TestRedeliveringOrderCancelledChangesNothing(t *testing.T) {
	w := newWorld()
	w.add(trip(domain.Assigned))
	svc := w.service(off(), t0)
	for i := 0; i < 3; i++ {
		if err := svc.OrderCancelled(ctx, cancelEvt("o1")); err != nil {
			t.Fatal(err)
		}
	}
	if len(w.outbox) != 1 || len(w.transitioned) != 1 {
		t.Errorf("%d status events and %d counts after redelivery, want 1 each", len(w.outbox), len(w.transitioned))
	}
}

func TestOrderCancelledLeavesFinishedTripsAlone(t *testing.T) {
	for _, status := range []domain.Status{domain.Completed, domain.Cancelled} {
		w := newWorld()
		tr := trip(status)
		tr.CancelledBy = "DRIVER"
		w.add(tr)
		if err := w.service(off(), t0).OrderCancelled(ctx, cancelEvt("o1")); err != nil {
			t.Fatal(err)
		}
		if got := w.trips[tr.ID]; got.Status != status || got.CancelledBy != "DRIVER" {
			t.Errorf("%s trip became %+v", status, got)
		}
		if len(w.outbox) != 0 {
			t.Errorf("%s: queued %+v", status, w.outbox)
		}
	}
}

func TestOrderCancelledBeforeTheTripExistsIsRemembered(t *testing.T) {
	w := newWorld()
	if err := w.service(off(), t0).OrderCancelled(ctx, cancelEvt("o1")); err != nil {
		t.Fatal(err)
	}
	if !w.cancelled["o1"] || len(w.outbox) != 0 {
		t.Errorf("cancelled %v, outbox %v", w.cancelled, w.outbox)
	}
}

// If the trip is created between our read and the lock, the event is retried
// with the trip in hand rather than silently dropped.
func TestATripThatAppearsWhileTheOrderIsBeingCancelledAsksForARetry(t *testing.T) {
	w := newWorld()
	svc := w.service(off(), t0)
	w.beforeLock = func() { w.add(trip(domain.Assigned)) }
	if err := svc.OrderCancelled(ctx, cancelEvt("o1")); !errors.Is(err, errTripAppeared) {
		t.Fatalf("err = %v, want errTripAppeared", err)
	}
	if !w.cancelled["o1"] {
		t.Error("the order was not remembered before the retry")
	}
	if err := svc.OrderCancelled(ctx, cancelEvt("o1")); err != nil { // the redelivery
		t.Fatal(err)
	}
	if w.trips[trip(domain.Assigned).ID].Status != domain.Cancelled {
		t.Error("the redelivery did not cancel the trip")
	}
}

func TestAnOlderOrderCancelledEventWithoutACancellerIsTheRiders(t *testing.T) {
	w := newWorld()
	tr := w.add(trip(domain.Assigned))
	if err := w.service(off(), t0).OrderCancelled(ctx, OrderCancelled{OrderID: "o1"}); err != nil {
		t.Fatal(err)
	}
	if w.trips[tr.ID].CancelledBy != domain.CancelledByRider {
		t.Errorf("cancelled by %q", w.trips[tr.ID].CancelledBy)
	}
}

func ended(status domain.Status) TripEnded {
	return TripEnded{TripID: "t", DriverID: "driver-1", Status: status}
}

func TestAFinishedTripReleasesItsDriver(t *testing.T) {
	for _, status := range []domain.Status{domain.Completed, domain.Cancelled} {
		w := newWorld()
		w.driverStatus["driver-1"] = domain.DriverOnTrip
		if err := w.service(off(), t0).ReleaseDriver(ctx, ended(status)); err != nil {
			t.Fatal(err)
		}
		if w.driverStatus["driver-1"] != domain.DriverAvailable {
			t.Errorf("%s: driver is %s", status, w.driverStatus["driver-1"])
		}
	}
}

func TestEventsThatAreNotTheEndOfATripReleaseNobody(t *testing.T) {
	w := newWorld()
	w.driverStatus["driver-1"] = domain.DriverOnTrip
	svc := w.service(off(), t0)
	for _, s := range []domain.Status{domain.Assigned, domain.InProgress} {
		if err := svc.ReleaseDriver(ctx, ended(s)); err != nil {
			t.Fatal(err)
		}
	}
	if w.driverStatus["driver-1"] != domain.DriverOnTrip {
		t.Errorf("driver is %s", w.driverStatus["driver-1"])
	}
}

func TestAnOldTripEndingDoesNotReleaseADriverOnANewTrip(t *testing.T) {
	w := newWorld()
	w.driverStatus["driver-1"] = domain.DriverOnTrip
	w.add(trip(domain.Assigned)) // the new trip, in flight
	if err := w.service(off(), t0).ReleaseDriver(ctx, ended(domain.Completed)); err != nil {
		t.Fatal(err)
	}
	if w.driverStatus["driver-1"] != domain.DriverOnTrip {
		t.Errorf("driver is %s, want ON_TRIP", w.driverStatus["driver-1"])
	}
}

func TestAnOldTripEndingDoesNotUndoANewClaim(t *testing.T) {
	w := newWorld()
	w.driverStatus["driver-1"] = domain.DriverAssigned // Matching has claimed them; no trip yet
	if err := w.service(off(), t0).ReleaseDriver(ctx, ended(domain.Completed)); err != nil {
		t.Fatal(err)
	}
	if w.driverStatus["driver-1"] != domain.DriverAssigned {
		t.Errorf("driver is %s, want ASSIGNED: the release undid the claim", w.driverStatus["driver-1"])
	}
}
