package main

import (
	"errors"
	"testing"

	"grab-mini/pkg/events"
)

func TestAverage(t *testing.T) {
	cases := []struct {
		sum, count int
		want       float64
	}{
		{0, 0, 0},
		{5, 1, 5},
		{14, 3, 4.67},
		{9, 2, 4.5},
		{1, 3, 0.33},
	}
	for _, c := range cases {
		if got := average(c.sum, c.count); got != c.want {
			t.Errorf("average(%d, %d) = %v, want %v", c.sum, c.count, got, c.want)
		}
	}
}

func TestValidateRejectsStarsOutsideTheScaleAsPermanent(t *testing.T) {
	for _, stars := range []int{-1, 0, 6, 99} {
		err := validate(events.TripRated{DriverID: "d", Stars: stars})
		var inv *invalidEvent
		if !errors.As(err, &inv) || !inv.Permanent() {
			t.Errorf("stars=%d: err = %v, want a permanent invalidEvent", stars, err)
		}
	}
	for stars := 1; stars <= 5; stars++ {
		if err := validate(events.TripRated{DriverID: "d", Stars: stars}); err != nil {
			t.Errorf("stars=%d: unexpected error %v", stars, err)
		}
	}
	if validate(events.TripRated{Stars: 5}) == nil {
		t.Error("a rating with no driver should be rejected")
	}
}
