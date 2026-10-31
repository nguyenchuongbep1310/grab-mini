package main

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"grab-mini/pkg/events"
)

// Limits, in characters (runes, so Vietnamese counts as one each).
const (
	maxNameRunes   = 80
	maxPhoneRunes  = 20
	maxPlateRunes  = 16
	maxModelRunes  = 50
	maxColorRunes  = 30
	maxReasonRunes = 500

	minPhoneDigits = 7
	maxPhoneDigits = 15 // E.164
	minPlateAlnum  = 3
)

// fieldError says which field is wrong and why.
type fieldError struct{ field, msg string }

func (e *fieldError) Error() string { return e.field + ": " + e.msg }

func bad(field, format string, args ...any) *fieldError {
	return &fieldError{field, fmt.Sprintf(format, args...)}
}

// forbidden says whether r can make text read as something it is not or break a
// line: a control character (tab and newline included: every field here is one
// line), a line or paragraph separator, or a bidirectional override or isolate.
func forbidden(r rune) bool {
	return unicode.IsControl(r) || r == ' ' || r == ' ' ||
		(r >= '‪' && r <= '‮') || (r >= '⁦' && r <= '⁩')
}

// clean trims the text, collapses runs of white space to one space and refuses
// control characters and text longer than max characters. Any script is fine.
func clean(field, raw string, max int) (string, *fieldError) {
	if !utf8.ValidString(raw) {
		return "", bad(field, "must be valid UTF-8 text")
	}
	for _, r := range raw {
		if forbidden(r) {
			return "", bad(field, "must not contain control characters")
		}
	}
	text := strings.Join(strings.Fields(raw), " ")
	if n := utf8.RuneCountInString(text); n > max {
		return "", bad(field, "must be at most %d characters (got %d)", max, n)
	}
	return text, nil
}

// cleanName is a person's name: required, and it has to contain a letter (a number
// is not a name). Nothing else is assumed: names come in every script.
func cleanName(raw string) (string, *fieldError) {
	name, err := clean("name", raw, maxNameRunes)
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", bad("name", "is required")
	}
	if !strings.ContainsFunc(name, unicode.IsLetter) {
		return "", bad("name", "must contain a letter")
	}
	return name, nil
}

// cleanPhone is optional. It is checked loosely, since formats differ by country: digits
// with spaces, dots, dashes and brackets, and a "+" only at the start, 7 to 15 digits
// (the E.164 limit). Vietnamese numbers (0901 234 567, +84 901 234 567) pass; it is not
// proof that the number exists or is the person's: it is not verified.
func cleanPhone(raw string) (string, *fieldError) {
	phone, err := clean("phone", raw, maxPhoneRunes)
	if err != nil {
		return "", err
	}
	digits := 0
	for i, r := range phone {
		switch {
		case r >= '0' && r <= '9':
			digits++
		case r == '+' && i == 0:
		case r == ' ' || r == '-' || r == '.' || r == '(' || r == ')':
		default:
			return "", bad("phone", "may contain only digits, spaces, + - . ( )")
		}
	}
	if phone != "" && (digits < minPhoneDigits || digits > maxPhoneDigits) {
		return "", bad("phone", "must have %d to %d digits", minPhoneDigits, maxPhoneDigits)
	}
	return phone, nil
}

// cleanPlate returns the plate in capitals. Loosely checked: letters of any script, digits,
// spaces, dots and dashes, and at least three letters or digits. Vietnamese plates
// (29A-123.45, 59-X1 123.45, 51F-12345) pass, and so do plates of other countries.
// Required only to apply, so an empty plate is returned as empty.
func cleanPlate(raw string) (string, *fieldError) {
	plate, err := clean("plate", raw, maxPlateRunes)
	if err != nil {
		return "", err
	}
	alnum := 0
	for _, r := range plate {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			alnum++
		case r == ' ' || r == '-' || r == '.':
		default:
			return "", bad("plate", "may contain only letters, digits, spaces, - and .")
		}
	}
	if plate != "" && alnum < minPlateAlnum {
		return "", bad("plate", "must have at least %d letters or digits", minPlateAlnum)
	}
	return strings.ToUpper(plate), nil
}

// cleanVehicle accepts CAR or BIKE in any case; empty is allowed until the driver applies.
func cleanVehicle(raw string) (events.VehicleType, *fieldError) {
	v := events.VehicleType(strings.ToUpper(strings.TrimSpace(raw)))
	if v != "" && !v.Valid() {
		return "", bad("vehicleType", "must be CAR or BIKE")
	}
	return v, nil
}

// cleanReason is the text of a review decision: one line, 1 to 500 characters when required.
func cleanReason(raw string, required bool) (string, *fieldError) {
	reason, err := clean("reason", raw, maxReasonRunes)
	if err != nil {
		return "", err
	}
	if required && reason == "" {
		return "", bad("reason", "is required")
	}
	return reason, nil
}
