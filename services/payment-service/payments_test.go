package main

import (
	"testing"
	"time"
)

// Trip Service stamps events with whole seconds (time.RFC3339); the cancel timing must read them.
func TestParseEventTimeReadsWhatTripServiceWrites(t *testing.T) {
	trip := time.Date(2026, 10, 31, 9, 0, 7, 0, time.UTC).Format(time.RFC3339) // "2026-10-31T09:00:07Z"
	got, err := parseEventTime(trip)
	if err != nil || !got.Equal(time.Date(2026, 10, 31, 9, 0, 7, 0, time.UTC)) {
		t.Errorf("parseEventTime(%q) = %v, %v", trip, got, err)
	}
	if _, err := parseEventTime("2026-10-31T09:00:07.250+07:00"); err != nil {
		t.Errorf("fractional seconds with an offset: %v", err)
	}
	for _, bad := range []string{"", "yesterday", "2026-10-31"} {
		if _, err := parseEventTime(bad); err == nil {
			t.Errorf("parseEventTime(%q) succeeded", bad)
		}
	}
}
