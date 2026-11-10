package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"grab-mini/promotion-service/internal/domain"
)

func spec(code string) domain.PromotionSpec {
	return domain.PromotionSpec{Code: code, Kind: "percent", Value: 10, ValidTo: t0.Add(24 * time.Hour)}
}

func TestCreateRefusesWhatTheDomainRefuses(t *testing.T) {
	r := newRig(t)
	logs := capture(t)
	bad := spec("ab")
	_, err := r.svc.CreatePromotion(context.Background(), CreatePromotionCommand{AdminID: "a1", Spec: bad})
	var inv *domain.InvalidPromotionError
	if !errors.As(err, &inv) || inv.Message != "code must be 3 to 32 characters: letters, digits, '-' or '_'" {
		t.Fatalf("err = %v", err)
	}
	if len(r.store.calls) != 0 || logs.Len() != 0 {
		t.Fatalf("store %v, log %q", r.store.calls, logs)
	}
}

func TestCreateMapsADuplicateCodeAndLogsOnSuccess(t *testing.T) {
	r := newRig(t)
	logs := capture(t)
	v, err := r.svc.CreatePromotion(context.Background(), CreatePromotionCommand{AdminID: "a1", Spec: spec(" new10 ")})
	if err != nil {
		t.Fatal(err)
	}
	if v.Code != "NEW10" || v.Kind != "PERCENT" || v.Status != "active" || v.CreatedBy != "a1" || v.PerRider != 1 {
		t.Fatalf("view = %+v", v)
	}
	if got, want := logs.String(), "promotion NEW10 created by a1\n"; got != want {
		t.Fatalf("log = %q, want %q", got, want)
	}
	logs.Reset()
	_, err = r.svc.CreatePromotion(context.Background(), CreatePromotionCommand{AdminID: "a1", Spec: spec("NEW10")})
	if !errors.Is(err, domain.ErrCodeExists) {
		t.Fatalf("err = %v", err)
	}
	if logs.Len() != 0 {
		t.Fatalf("a refused create logged %q", logs)
	}
}

func TestDisableIsIdempotentAndKeepsTheFirstTime(t *testing.T) {
	r := newRig(t)
	logs := capture(t)
	r.store.put(percent10("SAVE10"))
	v, err := r.svc.DisablePromotion(context.Background(), "a1", "save10")
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != "disabled" || v.DisabledAt == nil || !v.DisabledAt.Equal(t0) {
		t.Fatalf("view = %+v", v)
	}
	r.clock.at = t0.Add(time.Hour)
	v, err = r.svc.DisablePromotion(context.Background(), "a1", "SAVE10")
	if err != nil {
		t.Fatal(err)
	}
	if !v.DisabledAt.Equal(t0) {
		t.Fatalf("disabledAt = %v, want the first time %v", v.DisabledAt, t0)
	}
	if got, want := logs.String(), "promotion SAVE10 disabled by a1\npromotion SAVE10 disabled by a1\n"; got != want {
		t.Fatalf("log = %q", got)
	}
}

func TestDisableOfAnUnknownCodeIsNotFound(t *testing.T) {
	r := newRig(t)
	logs := capture(t)
	_, err := r.svc.DisablePromotion(context.Background(), "a1", "NOPE1")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if !reflect.DeepEqual(r.store.calls, []string{"Disable"}) || logs.Len() != 0 {
		t.Fatalf("calls %v, log %q", r.store.calls, logs)
	}
}

func TestListIsNeverNilAndCarriesStatus(t *testing.T) {
	r := newRig(t)
	got, err := r.svc.ListPromotions(context.Background())
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty list = %#v, %v", got, err)
	}
	off := percent10("AOFF")
	off.DisabledAt = &t0
	soon := percent10("BSOON")
	soon.ValidFrom = t0.Add(time.Minute)
	old := percent10("COLD")
	old.ValidTo = t0
	for _, p := range []domain.Promotion{off, soon, old, percent10("DLIVE")} {
		r.store.put(p)
	}
	got, err = r.svc.ListPromotions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var statuses []string
	for _, v := range got {
		statuses = append(statuses, v.Code+"="+v.Status)
	}
	if want := "AOFF=disabled BSOON=scheduled COLD=expired DLIVE=active"; strings.Join(statuses, " ") != want {
		t.Fatalf("statuses = %v", statuses)
	}
}

func TestAPortFailureIsReportedWithTheStepName(t *testing.T) {
	boom := errors.New("boom")
	ctx := context.Background()
	cases := []struct {
		step string
		run  func(r *rig) error
	}{
		{"load promotion", func(r *rig) error {
			r.store.put(percent10("SAVE10"))
			r.store.fail["Find"] = boom
			_, err := r.svc.Validate(ctx, ValidateQuery{RiderID: "r1", Code: "SAVE10", Price: "100000"})
			return err
		}},
		{"count redemptions", func(r *rig) error {
			r.store.put(percent10("SAVE10"))
			r.store.fail["Usage"] = boom
			_, err := r.svc.Validate(ctx, ValidateQuery{RiderID: "r1", Code: "SAVE10", Price: "100000"})
			return err
		}},
		{"create promotion", func(r *rig) error {
			r.store.fail["Create"] = boom
			_, err := r.svc.CreatePromotion(ctx, CreatePromotionCommand{AdminID: "a1", Spec: spec("NEW10")})
			return err
		}},
		{"load promotion", func(r *rig) error {
			r.store.fail["View"] = boom
			_, err := r.svc.CreatePromotion(ctx, CreatePromotionCommand{AdminID: "a1", Spec: spec("NEW10")})
			return err
		}},
		{"disable promotion", func(r *rig) error {
			r.store.fail["Disable"] = boom
			_, err := r.svc.DisablePromotion(ctx, "a1", "SAVE10")
			return err
		}},
		{"load promotion", func(r *rig) error {
			r.store.put(percent10("SAVE10"))
			r.store.fail["View"] = boom
			_, err := r.svc.DisablePromotion(ctx, "a1", "SAVE10")
			return err
		}},
		{"list promotions", func(r *rig) error {
			r.store.fail["List"] = boom
			_, err := r.svc.ListPromotions(ctx)
			return err
		}},
	}
	for _, c := range cases {
		r := newRig(t)
		err := c.run(r)
		if !isStep(err, c.step) || !errors.Is(err, boom) {
			t.Fatalf("%s: err = %v", c.step, err)
		}
		if want := c.step + ": boom"; err.Error() != want {
			t.Fatalf("Error() = %q, want %q", err.Error(), want)
		}
	}
}

func TestListKeepsAStepTheStoreAlreadyNamed(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")

	r := newRig(t)
	r.store.fail["List"] = &StepError{Step: "read promotion", Err: boom}
	_, err := r.svc.ListPromotions(ctx)
	if err == nil || err.Error() != "read promotion: boom" {
		t.Fatalf("scan failure = %v, want exactly %q", err, "read promotion: boom")
	}

	r = newRig(t)
	r.store.fail["List"] = boom
	_, err = r.svc.ListPromotions(ctx)
	if err == nil || err.Error() != "list promotions: boom" {
		t.Fatalf("plain failure = %v, want exactly %q", err, "list promotions: boom")
	}
}
