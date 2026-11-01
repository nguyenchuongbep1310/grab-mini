package provider

import (
	"context"
	"testing"

	"grab-mini/payment-service/internal/app"
	"grab-mini/payment-service/internal/domain"
)

func TestFakeProviderIsIdempotentOnTheKey(t *testing.T) {
	p := NewFake()
	req := app.ChargeRequest{IdempotencyKey: "fare:1", RiderID: "rider-1", Amount: 50000, Currency: domain.CurrencyVND}
	a, err := p.Charge(context.Background(), req)
	if err != nil || a.Status != app.ChargeCaptured || a.Ref == "" {
		t.Fatalf("first charge = %+v, %v", a, err)
	}
	b, _ := p.Charge(context.Background(), req)
	if a != b {
		t.Errorf("same key gave %+v then %+v", a, b)
	}
	c, _ := p.Charge(context.Background(), app.ChargeRequest{IdempotencyKey: "fare:2", RiderID: "rider-1", Amount: 50000, Currency: domain.CurrencyVND})
	if c.Ref == a.Ref {
		t.Error("different keys share a provider reference")
	}
}

func TestFakeProviderDeclinesTheSandboxRider(t *testing.T) {
	p := NewFake()
	r, err := p.Charge(context.Background(), app.ChargeRequest{IdempotencyKey: "fare:1", RiderID: "decline-1", Amount: 1000, Currency: domain.CurrencyVND})
	if err != nil || r.Status != app.ChargeDeclined || r.DeclineReason == "" {
		t.Errorf("charge = %+v, %v; want a decline (a normal answer, not an error)", r, err)
	}
}

func TestFakeProviderRejectsNonsenseRequests(t *testing.T) {
	p := NewFake()
	for _, req := range []app.ChargeRequest{
		{RiderID: "r", Amount: 1},
		{IdempotencyKey: "k", Amount: 1},
		{IdempotencyKey: "k", RiderID: "r"},
		{IdempotencyKey: "k", RiderID: "r", Amount: -5},
	} {
		if _, err := p.Charge(context.Background(), req); err == nil {
			t.Errorf("%+v: want an error", req)
		}
	}
}

func TestProviderFromEnv(t *testing.T) {
	for _, name := range []string{"", "fake"} {
		if p, err := FromEnv(name); err != nil || p.Name() != "fake" {
			t.Errorf("FromEnv(%q) = %v, %v", name, p, err)
		}
	}
	if _, err := FromEnv("stripe"); err == nil {
		t.Error("an unknown provider must be refused, not silently replaced by the fake")
	}
}
