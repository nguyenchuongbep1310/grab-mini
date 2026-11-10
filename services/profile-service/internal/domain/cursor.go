package domain

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Cursor is a position in the review queue, ordered by (status time, driver id).
type Cursor struct {
	At time.Time
	ID string
}

// CursorFor is the nextCursor that follows d.
func CursorFor(d Driver) string {
	return fmt.Sprintf("%d.%s", d.StatusAt.UnixMicro(), d.ID)
}

// ParseCursor reads a nextCursor back.
func ParseCursor(v string) (Cursor, error) {
	bad := &InvalidRequestError{"cursor must come from a previous nextCursor"}
	micros, id, ok := strings.Cut(v, ".")
	if !ok || id == "" || len(id) > MaxIDLen {
		return Cursor{}, bad
	}
	n, err := strconv.ParseInt(micros, 10, 64)
	if err != nil || n < 0 {
		return Cursor{}, bad
	}
	return Cursor{At: time.UnixMicro(n).UTC(), ID: id}, nil
}

// ParseStatusFilter reads the status of a queue listing; empty means PENDING.
func ParseStatusFilter(v string) (string, error) {
	switch v {
	case "":
		return StatusPending, nil
	case StatusPending, StatusApproved, StatusRejected, StatusSuspended:
		return v, nil
	}
	return "", &InvalidRequestError{"status must be PENDING, APPROVED, REJECTED or SUSPENDED"}
}

// ParseLimit reads the page size of a queue listing; empty means DefaultPageSize.
func ParseLimit(v string) (int, error) {
	if v == "" {
		return DefaultPageSize, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > MaxPageSize {
		return 0, &InvalidRequestError{fmt.Sprintf("limit must be from 1 to %d", MaxPageSize)}
	}
	return n, nil
}

// ValidDriverID is an id that can name a driver: not empty, not too long, valid UTF-8.
func ValidDriverID(id string) bool {
	return id != "" && len(id) <= MaxIDLen && utf8.ValidString(id)
}

// ValidPathID is the check on the id in a review URL: not too long, valid UTF-8. An empty id
// passes (the route never gives one).
func ValidPathID(id string) bool {
	return len(id) <= MaxIDLen && utf8.ValidString(id)
}
