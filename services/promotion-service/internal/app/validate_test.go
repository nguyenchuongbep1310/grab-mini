package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"grab-mini/promotion-service/internal/domain"
)

func rejection(t *testing.T, err error) *domain.Rejection {
	t.Helper()
	var rej *domain.Rejection
	if !errors.As(err, &rej) {
		t.Fatalf("want *domain.Rejection, got %T %v", err, err)
	}
	return rej
}

func TestAMalformedCodeIsNotFoundAndCountedBeforeThePriceIsRead(t *testing.T) {
	r := newRig(t)
	// The price is garbage too: the malformed code must win, with no DB access.
	_, err := r.svc.Validate(context.Background(), ValidateQuery{RiderID: "r1", Code: "x!", Price: "garbage", VehicleType: "TRUCK"})
	rej := rejection(t, err)
	if rej.Code != "not_found" || rej.Message != "no such promotion code" {
		t.Fatalf("rejection = %+v", rej)
	}
	if !reflect.DeepEqual(r.m.events, []string{"validation:not_found"}) {
		t.Fatalf("counter = %v", r.m.events)
	}
	if len(r.store.calls) != 0 {
		t.Fatalf("the store was touched: %v", r.store.calls)
	}
}

func TestPriceIsCheckedBeforeTheVehicle(t *testing.T) {
	r := newRig(t)
	r.store.put(percent10("SAVE10"))
	_, err := r.svc.Validate(context.Background(), ValidateQuery{RiderID: "r1", Code: "SAVE10", Price: "", VehicleType: "TRUCK"})
	var inv *domain.InvalidRequestError
	if !errors.As(err, &inv) || inv.Message != "price is required" {
		t.Fatalf("err = %v", err)
	}
	if len(r.m.events) != 0 || len(r.store.calls) != 0 {
		t.Fatalf("counter %v, store %v: a request error counts nothing and reads nothing", r.m.events, r.store.calls)
	}
	_, err = r.svc.Validate(context.Background(), ValidateQuery{RiderID: "r1", Code: "SAVE10", Price: "100000", VehicleType: "TRUCK"})
	if !errors.As(err, &inv) || inv.Message != "vehicleType must be CAR or BIKE" {
		t.Fatalf("err = %v", err)
	}
	if len(r.m.events) != 0 || len(r.store.calls) != 0 {
		t.Fatalf("counter %v, store %v", r.m.events, r.store.calls)
	}
}

func TestTheVehicleDefaultsToCarAndIsCaseSensitive(t *testing.T) {
	r := newRig(t)
	r.store.put(percent10("SAVE10"))
	if _, err := r.svc.Validate(context.Background(), ValidateQuery{RiderID: "r1", Code: "SAVE10", Price: "100000"}); err != nil {
		t.Fatal(err)
	}
	if got := r.tokens.calls[0].Vehicle; got != "CAR" {
		t.Fatalf("default vehicle = %q", got)
	}
	_, err := r.svc.Validate(context.Background(), ValidateQuery{RiderID: "r1", Code: "SAVE10", Price: "100000", VehicleType: "bike"})
	var inv *domain.InvalidRequestError
	if !errors.As(err, &inv) {
		t.Fatalf("lower-case vehicle: err = %v", err)
	}
}

func TestAnUnknownCodeIsNotFoundAndCounted(t *testing.T) {
	r := newRig(t)
	_, err := r.svc.Validate(context.Background(), ValidateQuery{RiderID: "r1", Code: "NOPE1", Price: "100000"})
	if rej := rejection(t, err); rej.Code != "not_found" || rej.Message != "no such promotion code" {
		t.Fatalf("rejection = %+v", rej)
	}
	if !reflect.DeepEqual(r.m.events, []string{"validation:not_found"}) {
		t.Fatalf("counter = %v", r.m.events)
	}
	if !reflect.DeepEqual(r.store.calls, []string{"Find"}) {
		t.Fatalf("calls = %v: usage must not be read for a code that does not exist", r.store.calls)
	}
}

func TestEachRejectionIsCountedUnderItsCode(t *testing.T) {
	disabled := percent10("OFF")
	disabled.DisabledAt = &t0
	notStarted := percent10("SOON")
	notStarted.ValidFrom = t0.Add(time.Minute)
	expired := percent10("OLD")
	expired.ValidTo = t0
	bike := percent10("BIKEONLY")
	bike.VehicleType = str("BIKE")
	minFare := percent10("BIG")
	minFare.MinFare = 500000
	limited := percent10("LTD")
	limited.UsageLimit = i64(1)
	once := percent10("ONCE")

	cases := []struct {
		p    domain.Promotion
		code string
	}{
		{disabled, "not_found"}, {notStarted, "not_started"}, {expired, "expired"}, {bike, "wrong_vehicle"},
		{minFare, "min_fare"}, {limited, "limit_reached"}, {once, "already_used"},
	}
	for _, c := range cases {
		t.Run(c.code, func(t *testing.T) {
			r := newRig(t)
			r.store.put(c.p)
			r.store.usage[c.p.Code+"|r1"] = domain.Usage{Total: 1, ByRider: 1}
			_, err := r.svc.Validate(context.Background(), ValidateQuery{RiderID: "r1", Code: c.p.Code, Price: "100000"})
			if rej := rejection(t, err); rej.Code != c.code {
				t.Fatalf("code = %q, want %q", rej.Code, c.code)
			}
			if !reflect.DeepEqual(r.m.events, []string{"validation:" + c.code}) {
				t.Fatalf("counter = %v", r.m.events)
			}
			if len(r.tokens.calls) != 0 {
				t.Fatal("a token was issued for a rejected code")
			}
		})
	}
}

func TestAValidCodeIssuesATokenBoundToRiderPriceAndVehicle(t *testing.T) {
	r := newRig(t)
	r.store.put(percent10("SAVE10"))
	got, err := r.svc.Validate(context.Background(), ValidateQuery{RiderID: "r1", Code: "  save10 ", Price: "87000.4", VehicleType: "BIKE"})
	if err != nil {
		t.Fatal(err)
	}
	want := issued{Code: "SAVE10", Rider: "r1", Vehicle: "BIKE", Price: 87000, Discount: 8700, Now: t0}
	if len(r.tokens.calls) != 1 || r.tokens.calls[0] != want {
		t.Fatalf("issuer saw %+v, want %+v", r.tokens.calls, want)
	}
	exp := Validation{Code: "SAVE10", Discount: 8700, Price: 87000, FinalPrice: 78300, Currency: "VND", Token: "token-1", ExpiresAt: t0.Add(15 * time.Minute)}
	if got != exp {
		t.Fatalf("got %+v, want %+v", got, exp)
	}
}

func TestTheCountOfAnotherRidersUseDoesNotReachThisRider(t *testing.T) {
	r := newRig(t)
	r.store.put(percent10("SAVE10"))
	r.store.usage["SAVE10|r2"] = domain.Usage{Total: 1, ByRider: 1}
	r.store.usage["SAVE10|r1"] = domain.Usage{Total: 1, ByRider: 0}
	if _, err := r.svc.Validate(context.Background(), ValidateQuery{RiderID: "r1", Code: "SAVE10", Price: "100000"}); err != nil {
		t.Fatalf("r1 has not used the code: %v", err)
	}
	_, err := r.svc.Validate(context.Background(), ValidateQuery{RiderID: "r2", Code: "SAVE10", Price: "100000"})
	if rej := rejection(t, err); rej.Code != "already_used" {
		t.Fatalf("r2: %+v", rej)
	}
}

func TestValidateCountsOkOnlyOnSuccess(t *testing.T) {
	r := newRig(t)
	r.store.put(percent10("SAVE10"))
	if _, err := r.svc.Validate(context.Background(), ValidateQuery{RiderID: "r1", Code: "SAVE10", Price: "100000"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.m.events, []string{"validation:ok"}) {
		t.Fatalf("counter = %v", r.m.events)
	}
	// A port failure counts nothing.
	r2 := newRig(t)
	r2.store.put(percent10("SAVE10"))
	r2.store.fail["Usage"] = errors.New("db down")
	if _, err := r2.svc.Validate(context.Background(), ValidateQuery{RiderID: "r1", Code: "SAVE10", Price: "100000"}); err == nil {
		t.Fatal("want an error")
	}
	if len(r2.m.events) != 0 {
		t.Fatalf("counter = %v", r2.m.events)
	}
}
