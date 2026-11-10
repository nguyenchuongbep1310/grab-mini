package domain

import (
	"errors"
	"testing"
)

func TestInvalidEventIsPermanentAndKeepsItsText(t *testing.T) {
	var err error = &InvalidEvent{Reason: "eventId x is not a UUID"}
	if err.Error() != "invalid promotion event: eventId x is not a UUID" {
		t.Errorf("text = %q", err.Error())
	}
	var perm interface{ Permanent() bool }
	if !errors.As(err, &perm) || !perm.Permanent() {
		t.Error("not permanent")
	}
	if ErrNotFound.Error() != "no such promotion" {
		t.Errorf("ErrNotFound = %q", ErrNotFound)
	}
	if (&InvalidRequestError{Message: "m"}).Error() != "m" || (&InvalidPromotionError{Message: "m"}).Error() != "m" {
		t.Error("request/promotion error text")
	}
	if (&Rejection{Code: "a", Message: "b"}).Error() != "a: b" {
		t.Error("rejection text")
	}
}
