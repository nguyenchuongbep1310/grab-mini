package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// jwksServer serves a mutable body and counts requests.
type jwksServer struct {
	*httptest.Server
	mu     sync.Mutex
	body   []byte
	status int
	hits   atomic.Int32
}

func newJWKSServer(t *testing.T, body []byte) *jwksServer {
	t.Helper()
	s := &jwksServer{body: body, status: http.StatusOK}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		s.mu.Lock()
		defer s.mu.Unlock()
		w.WriteHeader(s.status)
		w.Write(s.body)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *jwksServer) set(status int, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.body = status, body
}

// clock is a controllable time source.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func clientFor(s *jwksServer) (*JWKSClient, *clock) {
	clk := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	c := NewJWKSClient(s.URL)
	c.now = clk.now
	return c, clk
}

func TestJWKSVerifiesWithAFetchedKeyAndCachesIt(t *testing.T) {
	pub, priv := newKey(t)
	i, _ := NewIssuer(priv, time.Hour)
	srv := newJWKSServer(t, i.JWKS())
	c, _ := clientFor(srv)
	v := NewVerifier(c)

	tok, _, _ := i.Issue(Rider, "rider-1")
	for n := 0; n < 5; n++ {
		if id, err := v.Verify(tok); err != nil || id.Subject != "rider-1" {
			t.Fatalf("Verify = %+v, %v", id, err)
		}
	}
	if got := srv.hits.Load(); got != 1 {
		t.Errorf("JWKS fetched %d times for 5 verifications, want 1", got)
	}
	if k, err := c.PublicKey(context.Background(), KeyID(pub)); err != nil || !k.Equal(pub) {
		t.Errorf("PublicKey = %v, %v", k, err)
	}
}

func TestJWKSRotationAddsNewKeyAndKeepsTheOldOne(t *testing.T) {
	oldPub, oldPriv := newKey(t)
	newPub, newPriv := newKey(t)
	oldIssuer, _ := NewIssuer(oldPriv, time.Hour)
	newIssuer, _ := NewIssuer(newPriv, time.Hour)

	srv := newJWKSServer(t, oldIssuer.JWKS())
	c, clk := clientFor(srv)
	v := NewVerifier(c)

	oldTok, _, _ := oldIssuer.Issue(Rider, "rider-1")
	if _, err := v.Verify(oldTok); err != nil {
		t.Fatal(err)
	}

	// The issuer rotates: it signs with the new key and publishes both.
	srv.set(http.StatusOK, newIssuer.JWKS(oldPub))
	newTok, _, _ := newIssuer.Issue(Rider, "rider-2")

	// A token with an unknown kid triggers a refresh, but not before MinRefresh.
	if _, err := v.Verify(newTok); err == nil {
		t.Error("a refresh happened inside the rate limit window")
	}
	clk.advance(c.MinRefresh + time.Second)
	if id, err := v.Verify(newTok); err != nil || id.Subject != "rider-2" {
		t.Fatalf("after rotation: %+v, %v", id, err)
	}
	// Tokens signed with the previous key still verify (it is still published).
	if _, err := v.Verify(oldTok); err != nil {
		t.Errorf("old token after rotation: %v", err)
	}
	_ = newPub

	// The issuer finally drops the old key: once the cache expires it stops working.
	srv.set(http.StatusOK, newIssuer.JWKS())
	clk.advance(c.MaxAge + time.Second)
	if _, err := v.Verify(oldTok); err == nil {
		t.Error("a retired key still verifies after the cache expired")
	}
	if _, err := v.Verify(newTok); err != nil {
		t.Errorf("current key after refresh: %v", err)
	}
}

func TestJWKSUnknownKidsCannotAmplifyIntoRequests(t *testing.T) {
	pub, priv := newKey(t)
	i, _ := NewIssuer(priv, time.Hour)
	srv := newJWKSServer(t, i.JWKS())
	c, clk := clientFor(srv)
	ctx := context.Background()

	if _, err := c.PublicKey(ctx, KeyID(pub)); err != nil {
		t.Fatal(err)
	}
	base := srv.hits.Load()
	for n := 0; n < 50; n++ {
		if _, err := c.PublicKey(ctx, "forged-"+string(rune('a'+n%26))); err == nil {
			t.Fatal("an unknown kid resolved")
		}
	}
	if extra := srv.hits.Load() - base; extra > 1 {
		t.Errorf("50 unknown kids caused %d fetches, want at most 1", extra)
	}
	// After the window, one more is allowed.
	clk.advance(c.MinRefresh + time.Second)
	before := srv.hits.Load()
	c.PublicKey(ctx, "forged-again")
	c.PublicKey(ctx, "forged-again-2")
	if extra := srv.hits.Load() - before; extra != 1 {
		t.Errorf("after the window: %d fetches, want 1", extra)
	}
}

func TestJWKSOversizedResponseIsRefused(t *testing.T) {
	pub, _ := newKey(t)
	big := append(MarshalJWKS(pub), []byte(strings.Repeat(" ", MaxJWKSBytes))...) // valid JSON, but too big
	srv := newJWKSServer(t, big)
	c, _ := clientFor(srv)
	if _, err := c.PublicKey(context.Background(), KeyID(pub)); err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Errorf("err = %v, want the size limit", err)
	}

	// An endless body must be cut off rather than read to the end.
	endless := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chunk := []byte(strings.Repeat("a", 32<<10))
		for n := 0; n < 4096; n++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer endless.Close()
	c2 := NewJWKSClient(endless.URL)
	if _, err := c2.PublicKey(context.Background(), "x"); err == nil {
		t.Error("an endless response was accepted")
	}
}

func TestJWKSBadResponsesNeverReplaceGoodKeys(t *testing.T) {
	pub, priv := newKey(t)
	i, _ := NewIssuer(priv, time.Hour)
	srv := newJWKSServer(t, i.JWKS())
	c, clk := clientFor(srv)
	v := NewVerifier(c)
	tok, _, _ := i.Issue(Rider, "rider-1")
	if _, err := v.Verify(tok); err != nil {
		t.Fatal(err)
	}

	for name, resp := range map[string]struct {
		status int
		body   string
	}{
		"500":                 {500, "boom"},
		"404":                 {404, "{}"},
		"not json":            {200, "<html>"},
		"empty set":           {200, `{"keys":[]}`},
		"only RSA":            {200, `{"keys":[{"kty":"RSA","kid":"r","n":"AQAB","e":"AQAB"}]}`},
		"bad key length":      {200, `{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"k","x":"AAAA"}]}`},
		"no kid":              {200, `{"keys":[{"kty":"OKP","crv":"Ed25519","x":"` + b64(pub) + `"}]}`},
		"duplicate kid":       {200, `{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"k","x":"` + b64(pub) + `"},{"kty":"OKP","crv":"Ed25519","kid":"k","x":"` + b64(pub) + `"}]}`},
		"key for encryption":  {200, `{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"k","use":"enc","x":"` + b64(pub) + `"}]}`},
		"key for another alg": {200, `{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"k","alg":"RS256","x":"` + b64(pub) + `"}]}`},
	} {
		srv.set(resp.status, []byte(resp.body))
		clk.advance(c.MinRefresh + time.Second)
		// A kid that isn't cached forces the refresh; it must fail without
		// dropping the good key.
		if _, err := c.PublicKey(context.Background(), "other"); err == nil {
			t.Errorf("%s: unknown kid resolved", name)
		}
		if _, err := v.Verify(tok); err != nil {
			t.Errorf("%s: good key lost after a bad refresh: %v", name, err)
		}
	}
}

func TestJWKSServesStaleKeysThroughAShortOutageButNotForever(t *testing.T) {
	_, priv := newKey(t)
	i, _ := NewIssuer(priv, time.Hour)
	srv := newJWKSServer(t, i.JWKS())
	c, clk := clientFor(srv)
	v := NewVerifier(c)
	tok, _, _ := i.Issue(Rider, "rider-1")
	if _, err := v.Verify(tok); err != nil {
		t.Fatal(err)
	}

	srv.set(http.StatusServiceUnavailable, nil)
	clk.advance(c.MaxAge + time.Minute) // cache expired, issuer down
	if _, err := v.Verify(tok); err != nil {
		t.Errorf("a short outage logged everyone out: %v", err)
	}
	clk.advance(c.MaxStale + time.Minute)
	if _, err := v.Verify(tok); err == nil {
		t.Error("keys were trusted past MaxStale during an outage")
	}
}

func TestJWKSIssuerDownAtStartIsNotFatalAndRecovers(t *testing.T) {
	_, priv := newKey(t)
	i, _ := NewIssuer(priv, time.Hour)
	srv := newJWKSServer(t, nil)
	srv.set(http.StatusServiceUnavailable, nil)
	c, clk := clientFor(srv)
	v := NewVerifier(c)
	tok, _, _ := i.Issue(Rider, "rider-1")
	if _, err := v.Verify(tok); err == nil {
		t.Fatal("verified with no keys")
	}
	srv.set(http.StatusOK, i.JWKS())
	clk.advance(c.MinRefresh + time.Second)
	if _, err := v.Verify(tok); err != nil {
		t.Errorf("did not recover: %v", err)
	}
}

func TestJWKSPublishedByTheIssuerParses(t *testing.T) {
	_, priv := newKey(t)
	pub2, _ := newKey(t)
	i, _ := NewIssuer(priv, time.Hour)
	keys, err := ParseJWKS(i.JWKS(pub2))
	if err != nil {
		t.Fatal(err)
	}
	// The issuer's own key plus the previous one.
	if len(keys) != 2 || !keys[KeyID(pub2)].Equal(pub2) {
		t.Errorf("keys = %d", len(keys))
	}
	if _, ok := keys[i.KeyID()]; !ok {
		t.Error("the signing key is not in its own JWKS")
	}
	var _ ed25519.PublicKey = keys[i.KeyID()]
}

// ---- environment ----

func clearAuthEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"JWT_PRIVATE_KEY", "JWT_PRIVATE_KEY_FILE", "JWT_PUBLIC_KEY", "JWT_PUBLIC_KEY_FILE",
		"JWT_JWKS_URL", "JWT_SECRET", "JWT_SECRET_FILE", "JWT_PREVIOUS_PUBLIC_KEYS", "JWT_PREVIOUS_PUBLIC_KEYS_FILE", "APP_ENV"} {
		t.Setenv(k, "")
	}
}

func TestTheDevKeyNeedsAppEnvDevelopment(t *testing.T) {
	for _, env := range []string{"", "production", "staging", "test"} {
		clearAuthEnv(t)
		t.Setenv("APP_ENV", env)
		if _, err := PrivateKeyFromEnv(); err == nil {
			t.Errorf("APP_ENV=%q: the private key defaulted to the dev key", env)
		}
		if _, err := VerifierFromEnv(); err == nil {
			t.Errorf("APP_ENV=%q: the verifier defaulted to the dev key", env)
		}
	}

	clearAuthEnv(t)
	t.Setenv("APP_ENV", "development")
	priv, err := PrivateKeyFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	v, err := VerifierFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	i, _ := NewIssuer(priv, time.Hour)
	tok, _, _ := i.Issue(Driver, "driver-1")
	if id, err := v.Verify(tok); err != nil || id.Subject != "driver-1" {
		t.Errorf("dev issuer and dev verifier disagree: %+v, %v", id, err)
	}
}

func writePEMFiles(t *testing.T) (pubFile, privFile string, pub ed25519.PublicKey, priv ed25519.PrivateKey) {
	t.Helper()
	pub, priv = newKey(t)
	dir := t.TempDir()
	der, _ := x509.MarshalPKCS8PrivateKey(priv)
	pubDER, _ := x509.MarshalPKIXPublicKey(pub)
	privFile, pubFile = filepath.Join(dir, "priv.pem"), filepath.Join(dir, "pub.pem")
	if err := os.WriteFile(privFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pubFile, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return
}

func TestKeysFromFilesInProduction(t *testing.T) {
	clearAuthEnv(t)
	t.Setenv("APP_ENV", "production")
	pubFile, privFile, _, _ := writePEMFiles(t)
	t.Setenv("JWT_PRIVATE_KEY_FILE", privFile)
	t.Setenv("JWT_PUBLIC_KEY_FILE", pubFile)

	priv, err := PrivateKeyFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	v, err := VerifierFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	i, _ := NewIssuer(priv, time.Hour)
	tok, _, _ := i.Issue(Rider, "rider-1")
	if _, err := v.Verify(tok); err != nil {
		t.Errorf("file keys: %v", err)
	}
	// The development key's tokens are not trusted by a verifier with real keys.
	dev, _ := NewIssuer(DevPrivateKey(), time.Hour)
	devTok, _, _ := dev.Issue(Rider, "rider-1")
	if _, err := v.Verify(devTok); err == nil {
		t.Error("a token signed with the published dev key was accepted")
	}
}

func TestVerifierEnvRules(t *testing.T) {
	clearAuthEnv(t)
	t.Setenv("APP_ENV", "production")

	// A legacy JWT_SECRET alone gives no way to verify.
	t.Setenv("JWT_SECRET", strings.Repeat("s", 40))
	if _, err := VerifierFromEnv(); err == nil {
		t.Error("JWT_SECRET alone produced a verifier")
	}

	t.Setenv("JWT_JWKS_URL", "ftp://example.com/jwks")
	if _, err := VerifierFromEnv(); err == nil {
		t.Error("a non-http JWKS URL was accepted")
	}
	t.Setenv("JWT_JWKS_URL", "http://auth-service:3005/.well-known/jwks.json")
	if _, err := VerifierFromEnv(); err != nil {
		t.Errorf("a JWKS URL: %v", err)
	}

	pubFile, _, _, _ := writePEMFiles(t)
	t.Setenv("JWT_PUBLIC_KEY_FILE", pubFile)
	if _, err := VerifierFromEnv(); err == nil {
		t.Error("both a JWKS URL and a public key file were accepted")
	}

	t.Setenv("JWT_JWKS_URL", "")
	t.Setenv("JWT_PUBLIC_KEY_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err := VerifierFromEnv(); err == nil {
		t.Error("an unreadable public key file was ignored")
	}
}

func TestPreviousPublicKeysFromEnv(t *testing.T) {
	clearAuthEnv(t)
	if keys, err := PreviousPublicKeysFromEnv(); err != nil || keys != nil {
		t.Errorf("unset: %v, %v", keys, err)
	}
	pubFile, _, pub, _ := writePEMFiles(t)
	t.Setenv("JWT_PREVIOUS_PUBLIC_KEYS_FILE", pubFile)
	keys, err := PreviousPublicKeysFromEnv()
	if err != nil || len(keys) != 1 || !keys[0].Equal(pub) {
		t.Errorf("previous keys = %v, %v", keys, err)
	}
}
