package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"grab-mini/profile-service/internal/domain"
)

func TestAnEmptyRiderIsReturnedWhenThereIsNoRow(t *testing.T) {
	r := newRig(t)
	got, err := r.svc.GetRider(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	if got != (domain.Rider{ID: "r1"}) {
		t.Fatalf("got %+v, want an empty rider with the id", got)
	}
	r.store.loadErr = errors.New("db down")
	if _, err := r.svc.GetRider(context.Background(), "r1"); !errors.Is(err, r.store.loadErr) {
		t.Fatalf("a load error is returned as it is, got %v", err)
	}
}

func TestRiderNameIsCleanedBeforePhone(t *testing.T) {
	r := newRig(t)
	_, err := r.svc.SaveRider(context.Background(), SaveRiderCommand{ID: "r1", Name: "", Phone: "abc"})
	var fe *domain.FieldError
	if !errors.As(err, &fe) || fe.Field != "name" {
		t.Fatalf("want the name refused first, got %v", err)
	}
	_, err = r.svc.SaveRider(context.Background(), SaveRiderCommand{ID: "r1", Name: "Ann", Phone: "abc"})
	if !errors.As(err, &fe) || fe.Field != "phone" {
		t.Fatalf("want the phone refused, got %v", err)
	}
	if len(r.store.calls) != 0 || r.clk.reads != 0 || len(r.m.updated) != 0 {
		t.Fatalf("a refused save touches nothing: calls %v reads %d counters %v", r.store.calls, r.clk.reads, r.m.updated)
	}
}

func TestSavingARiderReadsTheClockBeforeTheWriteAndStoresCleanValues(t *testing.T) {
	r := newRig(t)
	buf := captureLog(t)
	got, err := r.svc.SaveRider(context.Background(), SaveRiderCommand{ID: "r1", Name: "  Ann   Lee ", Phone: "+84 123 4567"})
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 3, 1, 11, 0, 0, 123456000, time.UTC)
	if got.Name != "Ann Lee" || got.UpdatedAt != want || got.UpdatedAt.Location() != time.UTC {
		t.Fatalf("got %+v, want name cleaned and UpdatedAt %v", got, want)
	}
	if len(r.clk.atReads) != 1 || r.clk.atReads[0] != 0 {
		t.Fatalf("the clock must be read once, before the store write: %v", r.clk.atReads)
	}
	if len(r.store.saved) != 1 || r.store.saved[0] != got {
		t.Fatalf("stored %+v, returned %+v", r.store.saved, got)
	}
	if strings.Join(r.m.updated, ",") != "rider" || buf.String() != "rider r1 updated their profile\n" {
		t.Fatalf("counters %v log %q", r.m.updated, buf.String())
	}
	if r.wake.n != 0 {
		t.Fatal("saving a rider never wakes the relay")
	}
}

func TestSavingARiderCountsAndLogsOnlyAfterTheWrite(t *testing.T) {
	r := newRig(t)
	buf := captureLog(t)
	r.store.saveErr = errors.New("write failed")
	_, err := r.svc.SaveRider(context.Background(), SaveRiderCommand{ID: "r1", Name: "Ann", Phone: ""})
	if !errors.Is(err, r.store.saveErr) {
		t.Fatalf("want the store error as it is, got %v", err)
	}
	if len(r.m.updated) != 0 || buf.Len() != 0 {
		t.Fatalf("no counter and no log after a failed write: %v %q", r.m.updated, buf.String())
	}
}

func TestRiderLogLinesCarryOnlyTheId(t *testing.T) {
	r := newRig(t)
	buf := captureLog(t)
	const name, phone = "Zebediah Secretname", "+84 999 888 777"
	if _, err := r.svc.SaveRider(context.Background(), SaveRiderCommand{ID: "r1", Name: name, Phone: phone}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.svc.GetRider(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); strings.Contains(out, "Zebediah") || strings.Contains(out, "Secretname") || strings.Contains(out, "999") || !strings.Contains(out, "r1") {
		t.Fatalf("log must carry the id and no personal data: %q", out)
	}
}
