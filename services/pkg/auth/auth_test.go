package auth

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func newKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func newPair(t *testing.T) (*Issuer, *Verifier) {
	t.Helper()
	pub, priv := newKey(t)
	i, err := NewIssuer(priv, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return i, NewVerifier(NewKeySet(pub))
}

func TestATokenRoundTrips(t *testing.T) {
	i, v := newPair(t)
	tok, exp, err := i.Issue(Driver, "driver-1")
	if err != nil {
		t.Fatal(err)
	}
	id, err := v.Verify(tok)
	if err != nil || id.Subject != "driver-1" || id.Role != Driver {
		t.Fatalf("Verify = %+v, %v", id, err)
	}
	if time.Until(exp) < 59*time.Minute {
		t.Errorf("expires in %s, want about an hour", time.Until(exp))
	}
	parsed, _, _ := jwt.NewParser().ParseUnverified(tok, jwt.MapClaims{})
	if parsed.Header["kid"] != i.KeyID() || parsed.Header["alg"] != "EdDSA" {
		t.Errorf("header = %v, want kid %s and alg EdDSA", parsed.Header, i.KeyID())
	}
}

func TestAShortOrWrongKindOfKeyIsRefused(t *testing.T) {
	if _, err := NewIssuer(ed25519.PrivateKey("short"), time.Hour); err == nil {
		t.Error("NewIssuer accepted a short key")
	}
}

func TestIssueRefusesAnUnknownRoleOrNoSubject(t *testing.T) {
	i, _ := newPair(t)
	if _, _, err := i.Issue("superuser", "x"); err == nil {
		t.Error("issued a token for an unknown role")
	}
	if _, _, err := i.Issue(Rider, ""); err == nil {
		t.Error("issued a token with no subject")
	}
}

func TestAdminIsAValidRoleAndRoundTrips(t *testing.T) {
	if !Admin.Valid() || !Rider.Valid() || !Driver.Valid() || Role("superuser").Valid() || Role("").Valid() {
		t.Fatal("Valid() must accept rider, driver and admin only")
	}
	pub, priv := newKey(t)
	i, _ := NewIssuer(priv, time.Hour)
	v := NewVerifier(NewKeySet(pub))
	tok, _, err := i.Issue(Admin, "ops-1")
	if err != nil {
		t.Fatal(err)
	}
	if id, err := v.Verify(tok); err != nil || id.Role != Admin || id.Subject != "ops-1" {
		t.Errorf("Verify = %+v, %v; want admin ops-1", id, err)
	}
}

func TestRequireSeparatesAdminFromRiderAndDriver(t *testing.T) {
	pub, priv := newKey(t)
	i, _ := NewIssuer(priv, time.Hour)
	v := NewVerifier(NewKeySet(pub))
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	call := func(h http.Handler, role Role) int {
		tok, _, _ := i.Issue(role, "x-1")
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	adminOnly, riderOnly, anyone := v.Require(ok, Admin), v.Require(ok, Rider), v.Require(ok)
	if call(adminOnly, Admin) != http.StatusNoContent || call(adminOnly, Rider) != http.StatusForbidden || call(adminOnly, Driver) != http.StatusForbidden {
		t.Error("an admin-only route must admit admin and refuse rider and driver")
	}
	if call(riderOnly, Admin) != http.StatusForbidden {
		t.Error("an admin must not pass a rider-only route")
	}
	if call(anyone, Admin) != http.StatusNoContent {
		t.Error("a route with no roles admits any valid role, admin included")
	}
}

func TestVerifyRejectsWhatItShould(t *testing.T) {
	pub, priv := newKey(t)
	i, _ := NewIssuer(priv, time.Hour)
	v := NewVerifier(NewKeySet(pub))
	_, otherPriv := newKey(t)

	good := func() jwt.RegisteredClaims {
		return jwt.RegisteredClaims{
			Subject: "rider-1", Issuer: "grab-mini", Audience: jwt.ClaimStrings{"grab-mini-api"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		}
	}
	// sign builds a token by hand, with a kid unless the case says otherwise.
	sign := func(method jwt.SigningMethod, key any, kid string, c claims) string {
		tok := jwt.NewWithClaims(method, c)
		if kid != "" {
			tok.Header["kid"] = kid
		}
		s, err := tok.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	kid := i.KeyID()

	expired := good()
	expired.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Hour))
	noExp := good()
	noExp.ExpiresAt = nil
	wrongAud := good()
	wrongAud.Audience = jwt.ClaimStrings{"somebody-else"}
	wrongIss := good()
	wrongIss.Issuer = "evil"
	noSub := good()
	noSub.Subject = ""

	mine, _, _ := i.Issue(Rider, "rider-1")
	cases := map[string]string{
		"garbage":        "not a token",
		"empty":          "",
		"other key":      sign(jwt.SigningMethodEdDSA, otherPriv, kid, claims{Rider, good()}),
		"expired":        sign(jwt.SigningMethodEdDSA, priv, kid, claims{Rider, expired}),
		"no expiry":      sign(jwt.SigningMethodEdDSA, priv, kid, claims{Rider, noExp}),
		"wrong audience": sign(jwt.SigningMethodEdDSA, priv, kid, claims{Rider, wrongAud}),
		"wrong issuer":   sign(jwt.SigningMethodEdDSA, priv, kid, claims{Rider, wrongIss}),
		"no subject":     sign(jwt.SigningMethodEdDSA, priv, kid, claims{Rider, noSub}),
		"unknown role":   sign(jwt.SigningMethodEdDSA, priv, kid, claims{"superuser", good()}),
		"no kid":         sign(jwt.SigningMethodEdDSA, priv, "", claims{Rider, good()}),
		"unknown kid":    sign(jwt.SigningMethodEdDSA, priv, "not-a-kid", claims{Rider, good()}),
		"alg none":       sign(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, kid, claims{Rider, good()}),
		"tampered":       mine[:len(mine)-2] + "xx",
	}
	for name, tok := range cases {
		if _, err := v.Verify(tok); err == nil {
			t.Errorf("%s: token was accepted", name)
		} else if !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: error %v doesn't wrap ErrInvalidToken", name, err)
		}
	}
	if _, err := v.Verify(mine); err != nil {
		t.Errorf("a good token was rejected: %v", err)
	}
}

// The classic algorithm-confusion attacks: a token "signed" with HMAC using
// the public key (or the published JWKS bytes, or a legacy shared secret) as
// the secret, or with an RSA/ECDSA algorithm, must never verify.
func TestAlgorithmConfusionIsRejected(t *testing.T) {
	pub, priv := newKey(t)
	i, _ := NewIssuer(priv, time.Hour)
	v := NewVerifier(NewKeySet(pub))
	c := claims{Rider, jwt.RegisteredClaims{
		Subject: "rider-1", Issuer: "grab-mini", Audience: jwt.ClaimStrings{"grab-mini-api"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}}
	hmacSecrets := map[string][]byte{
		"raw public key":         pub,
		"public key as base64":   []byte(b64(pub)),
		"published JWKS":         i.JWKS(),
		"legacy dev HS256 value": []byte("grab-mini-dev-secret-not-for-production!"),
	}
	for name, secretBytes := range hmacSecrets {
		for _, m := range []jwt.SigningMethod{jwt.SigningMethodHS256, jwt.SigningMethodHS384, jwt.SigningMethodHS512} {
			tok := jwt.NewWithClaims(m, c)
			tok.Header["kid"] = i.KeyID()
			s, err := tok.SignedString(secretBytes)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := v.Verify(s); err == nil {
				t.Errorf("%s with %s was accepted", m.Alg(), name)
			}
		}
	}

	// A hand-built HS256 token that is *correctly* signed with the public key.
	hdr := b64([]byte(`{"alg":"HS256","typ":"JWT","kid":"` + i.KeyID() + `"}`))
	body := b64([]byte(`{"role":"rider","sub":"rider-1","iss":"grab-mini","aud":["grab-mini-api"],"exp":` + strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10) + `}`))
	mac := hmac.New(sha256.New, pub)
	mac.Write([]byte(hdr + "." + body))
	if _, err := v.Verify(hdr + "." + body + "." + b64(mac.Sum(nil))); err == nil {
		t.Error("an HS256 token keyed with the public key was accepted")
	}

	// alg:none with and without a signature segment.
	none := b64([]byte(`{"alg":"none","typ":"JWT","kid":"`+i.KeyID()+`"}`)) + "." + body
	for _, tok := range []string{none + ".", none + "." + b64([]byte("sig"))} {
		if _, err := v.Verify(tok); err == nil {
			t.Error("an alg:none token was accepted")
		}
	}
}

func TestKeyIDIsTheRFC7638Thumbprint(t *testing.T) {
	// RFC 8037 appendix A.3: the thumbprint of the example Ed25519 key.
	x, _ := base64.RawURLEncoding.DecodeString("11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo")
	if got, want := KeyID(ed25519.PublicKey(x)), "kPrK_qmxVWaYVA9wwBF6Iuo3vVzz7TxHCTwXBygrS4k"; got != want {
		t.Errorf("KeyID = %s, want %s", got, want)
	}
}

func TestPEMRoundTrip(t *testing.T) {
	pub, priv := newKey(t)
	der, _ := x509.MarshalPKCS8PrivateKey(priv)
	got, err := ParsePrivateKey(string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})))
	if err != nil || !got.Equal(priv) {
		t.Fatalf("ParsePrivateKey = %v", err)
	}
	pubDER, _ := x509.MarshalPKIXPublicKey(pub)
	bundle := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})) +
		string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}))
	keys, err := ParsePublicKeys(bundle)
	if err != nil || len(keys) != 2 || !keys[0].Equal(pub) {
		t.Fatalf("ParsePublicKeys = %v, %v", keys, err)
	}
	if _, err := ParsePrivateKey("nope"); err == nil {
		t.Error("garbage parsed as a private key")
	}
	if _, err := ParsePublicKeys("nope"); err == nil {
		t.Error("garbage parsed as a public key")
	}
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, _ := From(r.Context())
		w.Write([]byte(string(id.Role) + ":" + id.Subject))
	})
}

func call(h http.Handler, authz string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if authz != "" {
		req.Header.Set("Authorization", authz)
	}
	h.ServeHTTP(rec, req)
	return rec
}

func TestRequireStatusCodes(t *testing.T) {
	i, v := newPair(t)
	rider, _, _ := i.Issue(Rider, "rider-1")
	h := v.Require(okHandler(), Driver)

	if rec := call(h, ""); rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Errorf("no header: %d", rec.Code)
	}
	if rec := call(h, "Bearer junk"); rec.Code != http.StatusUnauthorized {
		t.Errorf("bad token: %d", rec.Code)
	}
	if rec := call(h, "Basic abc"); rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong scheme: %d", rec.Code)
	}
	if rec := call(h, "Bearer "+rider); rec.Code != http.StatusForbidden {
		t.Errorf("a rider on a driver-only route: %d, want 403", rec.Code)
	}

	driver, _, _ := i.Issue(Driver, "driver-1")
	if rec := call(h, "Bearer "+driver); rec.Code != http.StatusOK || rec.Body.String() != "driver:driver-1" {
		t.Errorf("a driver: %d %q", rec.Code, rec.Body.String())
	}
}

func TestRequireWithNoRolesAcceptsAnyValidCaller(t *testing.T) {
	i, v := newPair(t)
	tok, _, _ := i.Issue(Rider, "rider-1")
	if rec := call(v.Require(okHandler()), "Bearer "+tok); rec.Code != http.StatusOK {
		t.Errorf("any role: %d", rec.Code)
	}
}

func TestTokenFromWebSocket(t *testing.T) {
	cases := map[string]struct {
		header string
		want   string
		ok     bool
	}{
		"bearer then token":  {"bearer, abc.def.ghi", "abc.def.ghi", true},
		"extra space":        {"bearer,   tok", "tok", true},
		"no bearer":          {"chat, tok", "", false},
		"bearer, no token":   {"bearer", "", false},
		"bearer, empty":      {"bearer, ", "", false},
		"empty":              {"", "", false},
		"other protocol too": {"graphql-ws, bearer, tok", "tok", true},
	}
	for name, c := range cases {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		if c.header != "" {
			r.Header.Set("Sec-WebSocket-Protocol", c.header)
		}
		got, ok := TokenFromWebSocket(r)
		if got != c.want || ok != c.ok {
			t.Errorf("%s: got %q, %v; want %q, %v", name, got, ok, c.want, c.ok)
		}
	}
}

func TestRefusalsLookTheSame(t *testing.T) {
	rec := httptest.NewRecorder()
	Deny(rec, http.StatusForbidden, "nope")
	if !strings.Contains(rec.Body.String(), `"error":"nope"`) || rec.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Deny wrote %q (%s)", rec.Body.String(), rec.Header().Get("Content-Type"))
	}
}

// The guarantee the HS256 secret had, carried over to the Ed25519 keys: the published development
// key is never trusted outside APP_ENV=development, even when it is configured explicitly.
func TestTheDevKeyIsRefusedOutsideDevelopmentEvenWhenSetExplicitly(t *testing.T) {
	devPriv := pemPrivateKey(t, DevPrivateKey())
	devPub := pemPublicKey(t, DevPublicKey())
	for _, env := range []string{"", "production", "staging"} {
		clearAuthEnv(t)
		t.Setenv("APP_ENV", env)
		t.Setenv("JWT_PRIVATE_KEY", devPriv)
		if _, err := PrivateKeyFromEnv(); err == nil {
			t.Errorf("APP_ENV=%q accepted the published development private key", env)
		}
		clearAuthEnv(t)
		t.Setenv("APP_ENV", env)
		t.Setenv("JWT_PUBLIC_KEY", devPub)
		if _, err := VerifierFromEnv(); err == nil {
			t.Errorf("APP_ENV=%q trusted the published development public key", env)
		}
	}

	// The same keys are fine in development, and a real key is fine anywhere.
	clearAuthEnv(t)
	t.Setenv("APP_ENV", "development")
	t.Setenv("JWT_PRIVATE_KEY", devPriv)
	if _, err := PrivateKeyFromEnv(); err != nil {
		t.Errorf("development refused the development key: %v", err)
	}
	_, real, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	clearAuthEnv(t)
	t.Setenv("APP_ENV", "production")
	t.Setenv("JWT_PRIVATE_KEY", pemPrivateKey(t, real))
	if _, err := PrivateKeyFromEnv(); err != nil {
		t.Errorf("production refused a real key: %v", err)
	}
}

func pemPrivateKey(t *testing.T, k ed25519.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

func pemPublicKey(t *testing.T, k ed25519.PublicKey) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}
