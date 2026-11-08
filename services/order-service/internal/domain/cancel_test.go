package domain

import (
	"strings"
	"testing"
)

func TestCancelReasonIsTrimmedThenCountedInRunes(t *testing.T) {
	core := strings.Repeat("a", 200)
	if got, err := NormalizeCancelReason("  " + core + "  "); err != nil || got != core {
		t.Errorf("200 runes with spaces = %d runes, %v; want accepted and trimmed", len(got), err)
	}
	if _, err := NormalizeCancelReason(core + "a"); err != ErrReasonTooLong {
		t.Errorf("201 runes: err = %v, want ErrReasonTooLong", err)
	}
	multi := strings.Repeat("é", 200) // 400 bytes, 200 runes
	if got, err := NormalizeCancelReason(multi); err != nil || got != multi {
		t.Errorf("200 multibyte runes: %v", err)
	}
	if _, err := NormalizeCancelReason(multi + "é"); err != ErrReasonTooLong {
		t.Errorf("201 multibyte runes: err = %v", err)
	}
	if got, err := NormalizeCancelReason("   "); err != nil || got != "" {
		t.Errorf("blank = %q, %v; want empty", got, err)
	}
}
