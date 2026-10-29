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
	if _, _, err := i.Issue("admin", "x"); err == nil {
		t.Error("issued a token for an unknown role")
	}
	if _, _, err := i.Issue(Rider, ""); err == nil {
		t.Error("issued a token with no subject")
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
		"unknown role":   sign(jwt.SigningMethodEdDSA, priv, kid, claims{"admin", good()}),
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
