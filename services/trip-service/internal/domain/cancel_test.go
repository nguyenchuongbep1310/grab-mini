package domain

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestNormalizeRatingTrimsAndCountsRunesAfterTrimming(t *testing.T) {
	r, err := NewRating(5, "  "+strings.Repeat("é", 500)+"  ", 0)
	if err != nil {
		t.Fatalf("500 runes plus padding rejected: %v", err)
	}
	if utf8.RuneCountInString(r.Comment) != 500 {
		t.Errorf("comment not trimmed: %d runes", utf8.RuneCountInString(r.Comment))
	}
	if _, err := NewRating(5, strings.Repeat("é", 501), 0); err == nil {
		t.Error("501 runes accepted")
	}
}

func TestRatingValidation(t *testing.T) {
	const starsMsg = "stars (1 to 5) are required; comment is at most 500 characters"
	for _, c := range []struct {
		name    string
		stars   int
		comment string
		tip     int64
		msg     string // "" when valid
	}{
		{"one star", 1, "", 0, ""},
		{"five stars", 5, "", 0, ""},
		{"zero stars", 0, "", 0, starsMsg},
		{"six stars", 6, "", 0, starsMsg},
		{"negative stars", -1, "", 0, starsMsg},
		{"comment at the limit", 4, strings.Repeat("x", 500), 0, ""},
		{"comment over the limit", 4, strings.Repeat("x", 501), 0, starsMsg},
		{"multi-line comment", 5, "a\nb\tc", 0, ""},
		{"control character", 5, "a\x00b", 0, "comment contains control characters"},
		{"tip zero", 5, "", 0, ""},
		{"tip at the cap", 5, "", 1_000_000, ""},
		{"tip over the cap", 5, "", 1_000_001, "tipAmount must be a whole number from 0 to 1000000"},
		{"negative tip", 5, "", -1, "tipAmount must be a whole number from 0 to 1000000"},
	} {
		_, err := NewRating(c.stars, c.comment, c.tip)
		var ve *ValidationError
		switch {
		case c.msg == "" && err != nil:
			t.Errorf("%s: refused: %v", c.name, err)
		case c.msg != "" && (!errors.As(err, &ve) || ve.Msg != c.msg):
			t.Errorf("%s: err = %v, want a ValidationError %q", c.name, err, c.msg)
		}
	}
}

func TestCancelInfoNamesWhoCancelledAndChecksTheReason(t *testing.T) {
	for _, c := range []struct {
		who  Actor
		want string
	}{
		{Actor{ID: "r", Role: RoleRider}, CancelledByRider},
		{Actor{ID: "d", Role: RoleDriver}, CancelledByDriver},
		{Actor{ID: "x"}, CancelledByRider}, // as before: anyone who is not the driver
	} {
		got, err := NewCancelInfo(c.who, "  too slow ")
		if err != nil || got.By != c.want || got.Reason != "too slow" {
			t.Errorf("%+v: got %+v, %v; want by %s with the trimmed reason", c.who, got, err, c.want)
		}
	}
	if _, err := NewCancelInfo(Actor{}, strings.Repeat("x", 200)); err != nil {
		t.Errorf("a 200 character reason was refused: %v", err)
	}
	_, err := NewCancelInfo(Actor{}, strings.Repeat("é", 201))
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Msg != "reason is at most 200 characters" {
		t.Errorf("201 characters: %v", err)
	}
	// Padding does not count.
	if _, err := NewCancelInfo(Actor{}, " "+strings.Repeat("x", 200)+" "); err != nil {
		t.Errorf("padding counted against the limit: %v", err)
	}
}

func TestOrderCancelledInfoDefaultsToTheRider(t *testing.T) {
	if got := OrderCancelledInfo("", "why"); got != (CancelInfo{By: CancelledByRider, Reason: "why"}) {
		t.Errorf("older message without cancelledBy = %+v", got)
	}
	if got := OrderCancelledInfo(CancelledByDriver, ""); got.By != CancelledByDriver {
		t.Errorf("named canceller changed: %+v", got)
	}
}
