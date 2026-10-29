// Package authtest gives other services' tests a token issuer and a verifier
// that trust each other, without any key material in the test.
package authtest

import (
	"crypto/ed25519"
	"testing"
	"time"

	"grab-mini/pkg/auth"
)

// Key returns a fresh Ed25519 key pair.
func Key(t testing.TB) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

// Pair returns an Issuer with a fresh key, and a Verifier that trusts it.
func Pair(t testing.TB, ttl time.Duration) (*auth.Issuer, *auth.Verifier) {
	t.Helper()
	pub, priv := Key(t)
	issuer, err := auth.NewIssuer(priv, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return issuer, auth.NewVerifier(auth.NewKeySet(pub))
}

// Stranger returns an Issuer whose key no verifier from Pair trusts: its
// tokens must be rejected.
func Stranger(t testing.TB, ttl time.Duration) *auth.Issuer {
	t.Helper()
	i, _ := Pair(t, ttl)
	return i
}
