package main

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"grab-mini/pkg/events"
)

func TestCleanText(t *testing.T) {
	long := strings.Repeat("ạ", 500) // 500 characters, 1500 bytes
	cases := []struct {
		name, in, want string
		ok             bool
	}{
		{"plain", "I am at the gate", "I am at the gate", true},
		{"trimmed", "  \t hi there \n", "hi there", true},
		{"vietnamese and emoji", "Mình ở cổng số 2 \U0001F44B", "Mình ở cổng số 2 \U0001F44B", true},
		{"500 characters by runes, not bytes", long, long, true},
		{"501 characters", long + "a", "", false},
		{"empty", "", "", false},
		{"only spaces", "  \t ", "", false},
		{"newline inside", "line one\nline two", "", false},
		{"tab inside", "a\tb", "", false},
		{"NUL", "a\x00b", "", false},
		{"escape", "a\x1b[31mb", "", false},
		{"DEL", "a\x7fb", "", false},
		{"line separator", "a b", "", false},
		{"right-to-left override", "a‮b", "", false},
		{"bidi isolate", "a⁦b", "", false},
		{"zero width joiner of an emoji is fine", "\U0001F468‍\U0001F469", "\U0001F468‍\U0001F469", true},
	}
	for _, c := range cases {
		got, why := cleanText(c.in)
		if (why == "") != c.ok {
			t.Errorf("%s: refusal %q, want ok=%v", c.name, why, c.ok)
			continue
		}
		if c.ok && got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestIdempotencyKey(t *testing.T) {
	h := func(vals ...string) http.Header {
		hd := http.Header{}
		for _, v := range vals {
			hd.Add(idempotencyKeyHeader, v)
		}
		return hd
	}
	if k, err := idempotencyKey(h()); k != "" || err != nil {
		t.Errorf("absent: %q, %v", k, err)
	}
	if k, err := idempotencyKey(h("abc-123")); k != "abc-123" || err != nil {
		t.Errorf("valid: %q, %v", k, err)
	}
	for _, bad := range [][]string{{""}, {strings.Repeat("a", 129)}, {"tab\there"}, {"café"}, {"a", "b"}} {
		if _, err := idempotencyKey(h(bad...)); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}

func TestCursorRoundTrips(t *testing.T) {
	m := messageView{ID: "6f1e2d3c-1111-4222-8333-444455556666", CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)}
	at, id, err := parseCursor(cursor(m))
	if err != nil || !at.Equal(m.CreatedAt) || id != m.ID {
		t.Errorf("got %v %s %v", at, id, err)
	}
	for _, bad := range []string{"", "x", "123", "abc.6f1e2d3c-1111-4222-8333-444455556666", "-5.6f1e2d3c-1111-4222-8333-444455556666", "123.not-a-uuid", "1.2.3"} {
		if _, _, err := parseCursor(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}

func TestRankOnlyMovesForwardAndTheEndsAreEqual(t *testing.T) {
	if !(rank(events.TripStatusAssigned) < rank(events.TripStatusInProgress) && rank(events.TripStatusInProgress) < rank(events.TripStatusCompleted)) {
		t.Error("statuses are not ordered")
	}
	if rank(events.TripStatusCompleted) != rank(events.TripStatusCancelled) {
		t.Error("COMPLETED and CANCELLED must rank the same so neither replaces the other")
	}
	if rank("TELEPORTED") != 0 {
		t.Error("an unknown status must rank 0")
	}
}

func TestWhoIsOnATripAndWhenItCloses(t *testing.T) {
	end := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	trip := chatTrip{ID: "t", RiderID: "rider-1", DriverID: "driver-1", Status: events.TripStatusCompleted, UpdatedAt: end}

	for _, c := range []struct{ role, who, want string }{
		{"rider", "rider-1", "rider"}, {"driver", "driver-1", "driver"},
		{"rider", "driver-1", ""}, {"driver", "rider-1", ""}, {"rider", "rider-2", ""}, {"admin", "rider-1", ""}, {"rider", "", ""},
	} {
		if got := trip.roleOf(c.role, c.who); got != c.want {
			t.Errorf("%s %s: %q, want %q", c.role, c.who, got, c.want)
		}
	}
	noRider := chatTrip{DriverID: "driver-1"}
	if noRider.roleOf("rider", "") != "" {
		t.Error("a trip without a rider id matched an empty rider")
	}

	grace := 10 * time.Minute
	if !trip.open(end.Add(10*time.Minute), grace) {
		t.Error("closed at exactly the end of the grace period")
	}
	if trip.open(end.Add(10*time.Minute+time.Second), grace) {
		t.Error("still open after the grace period")
	}
	trip.Status = events.TripStatusInProgress
	if !trip.open(end.Add(48*time.Hour), grace) || trip.closesAt(grace) != nil {
		t.Error("a trip that is on is always open")
	}
}
