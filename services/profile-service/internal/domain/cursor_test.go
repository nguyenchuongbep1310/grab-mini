package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCursorRoundTripAndRefusals(t *testing.T) {
	at := mustTime("2026-03-04T05:06:07.123456Z")
	d := Driver{ID: "drv.with.dots-1", StatusAt: at}
	c := CursorFor(d)
	if want := "1772600767123456.drv.with.dots-1"; c != "1772600767123456.drv.with.dots-1" {
		t.Fatalf("cursor %q, want %q", c, want)
	}
	got, err := ParseCursor(c)
	if err != nil || !got.At.Equal(at) || got.ID != d.ID || got.At.Location() != time.UTC {
		t.Fatalf("%+v %v", got, err)
	}
	if got, err := ParseCursor("1.5.abc"); err != nil || got.ID != "5.abc" {
		t.Errorf("first dot splits: %+v %v", got, err)
	}
	if got, err := ParseCursor("0.x"); err != nil || !got.At.Equal(time.UnixMicro(0)) {
		t.Errorf("zero micros: %+v %v", got, err)
	}
	if _, err := ParseCursor("5." + strings.Repeat("x", MaxIDLen)); err != nil {
		t.Errorf("id of MaxIDLen refused: %v", err)
	}
	for _, in := range []string{"", "123", "123.", ".abc", "abc.def", "-1.abc", "99999999999999999999.abc", "5." + strings.Repeat("x", MaxIDLen+1)} {
		_, err := ParseCursor(in)
		var ir *InvalidRequestError
		if !errors.As(err, &ir) || err.Error() != "cursor must come from a previous nextCursor" {
			t.Errorf("%q: %v", in, err)
		}
	}
}

func TestParseStatusFilterAndLimitMessagesAndDefaults(t *testing.T) {
	for in, want := range map[string]string{"": StatusPending, "PENDING": StatusPending, "APPROVED": StatusApproved, "REJECTED": StatusRejected, "SUSPENDED": StatusSuspended} {
		if got, err := ParseStatusFilter(in); err != nil || got != want {
			t.Errorf("%q: %q %v", in, got, err)
		}
	}
	for _, in := range []string{"NONE", "pending", "x", " PENDING"} {
		_, err := ParseStatusFilter(in)
		var ir *InvalidRequestError
		if !errors.As(err, &ir) || err.Error() != "status must be PENDING, APPROVED, REJECTED or SUSPENDED" {
			t.Errorf("%q: %v", in, err)
		}
	}
	for in, want := range map[string]int{"": 50, "1": 1, "50": 50, "100": 100, "+7": 7} {
		if got, err := ParseLimit(in); err != nil || got != want {
			t.Errorf("%q: %d %v", in, got, err)
		}
	}
	for _, in := range []string{"0", "101", "-1", "x", "1.5", " 5"} {
		_, err := ParseLimit(in)
		var ir *InvalidRequestError
		if !errors.As(err, &ir) || err.Error() != "limit must be from 1 to 100" {
			t.Errorf("%q: %v", in, err)
		}
	}
}

func TestValidDriverIDAndValidPathID(t *testing.T) {
	long := strings.Repeat("x", MaxIDLen)
	for _, c := range []struct {
		id         string
		driver, pa bool
	}{
		{"", false, true}, // the one difference
		{"driver-1", true, true},
		{long, true, true},
		{long + "x", false, false},
		{"an\xffbinh", false, false},
		{"Nguyễn", true, true},
	} {
		if ValidDriverID(c.id) != c.driver || ValidPathID(c.id) != c.pa {
			t.Errorf("%q: ValidDriverID %v, ValidPathID %v", c.id, ValidDriverID(c.id), ValidPathID(c.id))
		}
	}
}
