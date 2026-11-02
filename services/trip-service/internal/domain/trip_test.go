package domain

import "testing"

var someTrip = Trip{ID: "t1", DriverID: "driver-1", RiderID: "rider-1", Status: Assigned}

func TestTheTripsRiderCanRateItAndTheEventNamesThem(t *testing.T) {
	if !someTrip.IsRiderOf(Actor{ID: "rider-1", Role: RoleRider}) {
		t.Error("the trip's rider isn't recognised as its rider")
	}
	if someTrip.IsRiderOf(Actor{ID: "rider-1", Role: RoleDriver}) {
		t.Error("a driver token with the rider's id counted as the rider")
	}
	old := Trip{ID: "x", DriverID: "d", RiderID: ""}
	if old.IsRiderOf(Actor{ID: "", Role: RoleRider}) {
		t.Error("a trip with no recorded rider matched an empty subject")
	}
}

func TestWhoMayDoWhat(t *testing.T) {
	driver := Actor{ID: "driver-1", Role: RoleDriver}
	otherDriver := Actor{ID: "driver-2", Role: RoleDriver}
	rider := Actor{ID: "rider-1", Role: RoleRider}
	otherRider := Actor{ID: "rider-2", Role: RoleRider}
	nobody := Actor{ID: "driver-1"} // an id but no side (an admin token)
	for _, c := range []struct {
		action Action
		who    Actor
		want   bool
	}{
		{Pickup, driver, true}, {Pickup, otherDriver, false}, {Pickup, rider, false}, {Pickup, nobody, false},
		{Complete, driver, true}, {Complete, otherDriver, false}, {Complete, rider, false},
		{Cancel, driver, true}, {Cancel, rider, true},
		{Cancel, otherDriver, false}, {Cancel, otherRider, false}, {Cancel, nobody, false},
	} {
		if got := c.action.MayBeDoneBy(c.who, someTrip); got != c.want {
			t.Errorf("%s by %+v = %v, want %v", c.action.Name(), c.who, got, c.want)
		}
	}
}

func TestTheStateMachine(t *testing.T) {
	all := []Status{Assigned, InProgress, Completed, Cancelled}
	// from which statuses each action can start (a start outside this set is refused).
	starts := map[string][]Status{
		"pickup":   {Assigned},
		"complete": {InProgress},
		"cancel":   {Assigned, InProgress},
	}
	targets := map[string]Status{"pickup": InProgress, "complete": Completed, "cancel": Cancelled}
	for _, a := range []Action{Pickup, Complete, Cancel} {
		if a.To() != targets[a.Name()] {
			t.Errorf("%s leads to %s, want %s", a.Name(), a.To(), targets[a.Name()])
		}
		allowed := map[Status]bool{}
		for _, s := range starts[a.Name()] {
			allowed[s] = true
		}
		for _, s := range all {
			if got := a.CanStartFrom(s); got != allowed[s] {
				t.Errorf("%s from %s = %v, want %v", a.Name(), s, got, allowed[s])
			}
			if got := a.Reached(Trip{Status: s}); got != (s == a.To()) {
				t.Errorf("%s reached at %s = %v", a.Name(), s, got)
			}
		}
	}
	// A completed trip can't be cancelled, and nothing leaves CANCELLED.
	if Cancel.CanStartFrom(Completed) || Cancel.CanStartFrom(Cancelled) {
		t.Error("a finished trip can be cancelled")
	}
}

func TestFromReturnsACopy(t *testing.T) {
	f := Cancel.From()
	f[0] = Completed
	if !Cancel.CanStartFrom(Assigned) || Cancel.CanStartFrom(Completed) {
		t.Error("changing the list From returned changed the action")
	}
}

func TestOnlyPickupAndCompleteNeedProximityAndOnlyTheEndsReleaseTheDriver(t *testing.T) {
	if s, ok := Pickup.ProximityStep(); !ok || s != StepPickup {
		t.Errorf("pickup step = %q, %v", s, ok)
	}
	if s, ok := Complete.ProximityStep(); !ok || s != StepDropoff {
		t.Errorf("complete step = %q, %v", s, ok)
	}
	if _, ok := Cancel.ProximityStep(); ok {
		t.Error("cancelling needs the driver to be somewhere")
	}
	for a, want := range map[string]bool{"pickup": false, "complete": true, "cancel": true} {
		var act Action
		switch a {
		case "pickup":
			act = Pickup
		case "complete":
			act = Complete
		default:
			act = Cancel
		}
		if act.ReleasesDriver() != want {
			t.Errorf("%s releases the driver = %v, want %v", a, act.ReleasesDriver(), want)
		}
	}
}
