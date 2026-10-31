package promo

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

var now = time.Unix(1_800_000_000, 0)

func testSigner(t *testing.T) *Signer {
	t.Helper()
	s, err := NewSigner("test-secret")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

var trip = Binding{RiderID: "rider-1", Price: 87000, Vehicle: "CAR"}

func TestAValidTokenCarriesTheDiscountItWasIssuedFor(t *testing.T) {
	s := testSigner(t)
	tok, exp := s.Issue("WELCOME10", trip, 8700, now)
	if want := now.Add(TokenTTL); !exp.Equal(want) {
		t.Errorf("expires at %v, want %v", exp, want)
	}
	c, err := s.Verify(tok, trip, now.Add(9*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if c.Code != "WELCOME10" || c.Discount != 8700 || c.Price != 87000 || c.RiderID != "rider-1" {
		t.Errorf("claims = %+v", c)
	}
}

func TestATokenIsBoundToRiderPriceAndVehicle(t *testing.T) {
	s := testSigner(t)
	tok, _ := s.Issue("WELCOME10", trip, 8700, now)
	cases := []struct {
		name string
		want Binding
		err  error
	}{
		{"other rider", Binding{"rider-2", 87000, "CAR"}, ErrWrongRider},
		{"other price", Binding{"rider-1", 88000, "CAR"}, ErrWrongPrice},
		{"other vehicle", Binding{"rider-1", 87000, "BIKE"}, ErrWrongVehicle},
	}
	for _, tc := range cases {
		if _, err := s.Verify(tok, tc.want, now); !errors.Is(err, tc.err) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.err)
		}
	}
}

func TestAnExpiredTokenIsRefusedAtTheExpiryInstant(t *testing.T) {
	s := testSigner(t)
	tok, exp := s.Issue("WELCOME10", trip, 8700, now)
	if _, err := s.Verify(tok, trip, exp.Add(-time.Second)); err != nil {
		t.Errorf("a second before expiry: %v", err)
	}
	c, err := s.Verify(tok, trip, exp)
	if !errors.Is(err, ErrExpired) {
		t.Fatalf("at expiry: err = %v, want ErrExpired", err)
	}
	if c.Code != "WELCOME10" {
		t.Errorf("an expired token still says what it was for, got %+v", c)
	}
}

func TestForgedAndDamagedTokensAreRefused(t *testing.T) {
	s := testSigner(t)
	tok, _ := s.Issue("WELCOME10", trip, 8700, now)
	payload, sig, _ := strings.Cut(tok, ".")

	// Raising the discount in the body without the key.
	forgedBody := base64.RawURLEncoding.EncodeToString([]byte(`{"c":"WELCOME10","r":"rider-1","p":87000,"v":"CAR","d":87000,"e":2000000000}`))

	// Signed with another key.
	other, _ := NewSigner("another-secret")
	foreign, _ := other.Issue("WELCOME10", trip, 8700, now)

	for name, bad := range map[string]string{
		"body changed":    forgedBody + "." + sig,
		"signature cut":   payload + "." + sig[:len(sig)-2],
		"another key":     foreign,
		"no signature":    payload,
		"empty":           "",
		"just a dot":      ".",
		"not base64":      "%%%." + sig,
		"quote-shaped id": payload + ".AAAA",
	} {
		c, err := s.Verify(bad, trip, now)
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
		if c != (Claims{}) {
			t.Errorf("%s: forged token leaked claims %+v", name, c)
		}
	}
}

func TestATokenSignedUnderTheSameKeyWithoutTheDomainPrefixIsRefused(t *testing.T) {
	// A quote id is HMAC(key, payload); a promo token adds a prefix, so a
	// signature lifted from one cannot pass as the other even under one key.
	s := testSigner(t)
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"c":"X","r":"rider-1","p":87000,"v":"CAR","d":87000,"e":2000000000}`))
	if _, err := s.Verify(payload+"."+quoteStyleMAC(s.key, payload), trip, now); !errors.Is(err, ErrInvalid) {
		t.Errorf("err = %v, want ErrInvalid", err)
	}
}

func TestATokenWithAnImpossibleDiscountIsMalformed(t *testing.T) {
	s := testSigner(t)
	tok, _ := s.Issue("X", trip, 90000, now) // more than the price
	if _, err := s.Verify(tok, trip, now); !errors.Is(err, ErrMalformed) {
		t.Errorf("err = %v, want ErrMalformed", err)
	}
	tok, _ = s.Issue("X", trip, -1, now)
	if _, err := s.Verify(tok, trip, now); !errors.Is(err, ErrMalformed) {
		t.Errorf("negative: err = %v, want ErrMalformed", err)
	}
}

func TestSignerFromEnvRefusesTheDevelopmentSecretOutsideDevelopment(t *testing.T) {
	t.Setenv("PROMO_SECRET", "")
	t.Setenv("PROMO_SECRET_FILE", "")

	t.Setenv("APP_ENV", "development")
	if _, err := SignerFromEnv(); err != nil {
		t.Errorf("development without a secret: %v", err)
	}

	for _, env := range []string{"production", "", "staging"} {
		t.Setenv("APP_ENV", env)
		if _, err := SignerFromEnv(); err == nil {
			t.Errorf("APP_ENV=%q without a secret: accepted the development default", env)
		}
	}

	// Naming the public secret explicitly does not make it safe.
	t.Setenv("APP_ENV", "production")
	t.Setenv("PROMO_SECRET", DevSecret)
	if _, err := SignerFromEnv(); err == nil {
		t.Error("production accepted the public development secret")
	}
	t.Setenv("PROMO_SECRET", "a-real-secret")
	if _, err := SignerFromEnv(); err != nil {
		t.Errorf("production with a real secret: %v", err)
	}
}

// quoteStyleMAC is how package surge signs a quote id: HMAC-SHA256 of the
// payload alone.
func quoteStyleMAC(key []byte, payload string) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}
