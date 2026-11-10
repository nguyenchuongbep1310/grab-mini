package domain

import "testing"

func TestRiderCarriesTheSameCleanedFields(t *testing.T) {
	name, nerr := CleanName("  Lê  Hoa ")
	phone, perr := CleanPhone("0901 234 567")
	if nerr != nil || perr != nil {
		t.Fatal(nerr, perr)
	}
	r := Rider{ID: "r1", Name: name, Phone: phone}
	if r.Name != "Lê Hoa" || r.Phone != "0901 234 567" {
		t.Errorf("%+v", r)
	}
}
