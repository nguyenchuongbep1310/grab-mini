package main

import (
	"strings"
	"testing"
	"time"
)

func TestCleanName(t *testing.T) {
	cases := []struct {
		name, in, want string
		ok             bool
	}{
		{"plain", "Nguyễn Văn An", "Nguyễn Văn An", true},
		{"trimmed and collapsed", "  Trần \u00a0 Thị   Bích ", "Trần Thị Bích", true},
		{"cjk", "山田 太郎", "山田 太郎", true},
		{"arabic", "محمد علي", "محمد علي", true},
		{"emoji after a letter", "Mai \U0001F338", "Mai \U0001F338", true},
		{"80 characters", strings.Repeat("ạ", 80), strings.Repeat("ạ", 80), true},
		{"81 characters", strings.Repeat("ạ", 81), "", false},
		{"empty", "", "", false},
		{"spaces only", "   ", "", false},
		{"digits only", "12345", "", false},
		{"newline", "An\nBinh", "", false},
		{"tab", "An\tBinh", "", false},
		{"NUL", "An\x00", "", false},
		{"escape", "\x1b[31mAn", "", false},
		{"line separator", "An Binh", "", false},
		{"right-to-left override", "An‮Binh", "", false},
		{"bidi isolate", "An⁦Binh", "", false},
		{"invalid utf-8", "An\xffBinh", "", false},
	}
	for _, c := range cases {
		got, err := cleanName(c.in)
		if (err == nil) != c.ok {
			t.Errorf("%s: err = %v, want ok=%v", c.name, err, c.ok)
			continue
		}
		if c.ok && got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestCleanPhone(t *testing.T) {
	cases := []struct {
		in, want string
		ok       bool
	}{
		{"", "", true}, // optional
		{"0901234567", "0901234567", true},
		{"0901 234 567", "0901 234 567", true},
		{"+84 901 234 567", "+84 901 234 567", true},
		{"(028) 3822-1234", "(028) 3822-1234", true},
		{"+1 (415) 555-0132", "+1 (415) 555-0132", true},
		{"090.123.4567", "090.123.4567", true},
		{"  0901234567  ", "0901234567", true},
		{"123456", "", false}, // 6 digits
		{"1234567", "1234567", true},
		{"1234567890123456", "", false}, // 16 digits
		{"090 123 abc", "", false},
		{"0901234567;", "", false},
		{"090+1234567", "", false}, // + not at the start
		{"0901\n234567", "", false},
		{"+84 (0) 901 234 567 89", "", false}, // 21 characters
	}
	for _, c := range cases {
		got, err := cleanPhone(c.in)
		if (err == nil) != c.ok {
			t.Errorf("%q: err = %v, want ok=%v", c.in, err, c.ok)
			continue
		}
		if c.ok && got != c.want {
			t.Errorf("%q: got %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCleanPlate(t *testing.T) {
	cases := []struct {
		in, want string
		ok       bool
	}{
		{"", "", true}, // required only to apply
		{"51A-123.45", "51A-123.45", true},
		{"59-X1 123.45", "59-X1 123.45", true},
		{"29a-12345", "29A-12345", true}, // capitals
		{" 30E   999.99 ", "30E 999.99", true},
		{"ABC 123", "ABC 123", true}, // another country
		{"B 1234 XYZ", "B 1234 XYZ", true},
		{"ÅÄÖ 123", "ÅÄÖ 123", true},
		{"AB", "", false},
		{"A-", "", false},
		{"51A_123", "", false},
		{"51A/123", "", false},
		{"<script>", "", false},
		{"51A-123.45 extra long", "", false}, // over 16
		{"51A\n123", "", false},
	}
	for _, c := range cases {
		got, err := cleanPlate(c.in)
		if (err == nil) != c.ok {
			t.Errorf("%q: err = %v, want ok=%v", c.in, err, c.ok)
			continue
		}
		if c.ok && got != c.want {
			t.Errorf("%q: got %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCleanVehicle(t *testing.T) {
	for in, want := range map[string]string{"CAR": "CAR", "bike": "BIKE", " Car ": "CAR", "": ""} {
		got, err := cleanVehicle(in)
		if err != nil || string(got) != want {
			t.Errorf("%q: %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"TRUCK", "car bike", "0"} {
		if _, err := cleanVehicle(in); err == nil {
			t.Errorf("%q was accepted", in)
		}
	}
}

func TestCleanReason(t *testing.T) {
	if _, err := cleanReason("  ", true); err == nil {
		t.Error("a blank reason was accepted where required")
	}
	if got, err := cleanReason("", false); err != nil || got != "" {
		t.Errorf("optional empty: %q, %v", got, err)
	}
	if got, err := cleanReason("Biển số không khớp với giấy tờ", true); err != nil || got != "Biển số không khớp với giấy tờ" {
		t.Errorf("%q, %v", got, err)
	}
	if _, err := cleanReason(strings.Repeat("x", 501), true); err == nil {
		t.Error("501 characters were accepted")
	}
	if _, err := cleanReason("a\nb", true); err == nil {
		t.Error("a newline was accepted")
	}
}

func TestNextReviewTimeIsAlwaysLater(t *testing.T) {
	now := mustTime("2026-01-02T03:04:05.000100Z")
	if got := nextReviewTime(nil, now); !got.Equal(now) {
		t.Errorf("no previous: %v", got)
	}
	earlier := mustTime("2026-01-02T03:04:04Z")
	if got := nextReviewTime(&earlier, now); !got.Equal(now) {
		t.Errorf("earlier previous: %v", got)
	}
	same := now
	if got := nextReviewTime(&same, now); !got.After(same) {
		t.Errorf("same time: %v is not after %v", got, same)
	}
	later := mustTime("2026-01-02T03:05:00Z")
	if got := nextReviewTime(&later, now); !got.After(later) {
		t.Errorf("clock stepped back: %v is not after %v", got, later)
	}
}

func mustTime(s string) (t time.Time) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}
