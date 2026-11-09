package promotoken

import (
	"errors"
	"testing"
	"time"

	"grab-mini/pkg/promo"
)

func TestATokenIssuedHereVerifiesWithTheSharedSigner(t *testing.T) {
	signer, err := promo.NewSigner("shared-key")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	token, _ := New(signer).Issue("WELCOME10", "rider-1", 87000, "CAR", 8700, now)

	other, _ := promo.NewSigner("shared-key") // Order Service's own signer, same key
	want := promo.Binding{RiderID: "rider-1", Price: 87000, Vehicle: "CAR"}
	claims, err := other.Verify(token, want, now)
	if err != nil {
		t.Fatalf("Verify = %v", err)
	}
	if claims.Code != "WELCOME10" || claims.Discount != 8700 {
		t.Fatalf("claims = %+v", claims)
	}
	for name, b := range map[string]promo.Binding{
		"another rider":   {RiderID: "rider-2", Price: 87000, Vehicle: "CAR"},
		"another price":   {RiderID: "rider-1", Price: 90000, Vehicle: "CAR"},
		"another vehicle": {RiderID: "rider-1", Price: 87000, Vehicle: "BIKE"},
	} {
		if _, err := other.Verify(token, b, now); err == nil {
			t.Errorf("%s: token accepted", name)
		}
	}
	wrongKey, _ := promo.NewSigner("different-key")
	if _, err := wrongKey.Verify(token, want, now); err == nil {
		t.Error("a signer with another key accepted the token")
	}
}

func TestTheExpiryIsTheSignersOwn(t *testing.T) {
	signer, _ := promo.NewSigner("k")
	now := time.Date(2026, 1, 2, 3, 4, 5, 600, time.UTC)
	token, exp := New(signer).Issue("C", "r", 100, "CAR", 10, now)
	_, wantExp := signer.Issue("C", promo.Binding{RiderID: "r", Price: 100, Vehicle: "CAR"}, 10, now)
	if !exp.Equal(wantExp) {
		t.Fatalf("expiry = %v, want the signer's %v", exp, wantExp)
	}
	b := promo.Binding{RiderID: "r", Price: 100, Vehicle: "CAR"}
	if _, err := signer.Verify(token, b, exp.Add(time.Second)); !errors.Is(err, promo.ErrExpired) {
		t.Fatalf("Verify after expiry = %v, want ErrExpired", err)
	}
}
