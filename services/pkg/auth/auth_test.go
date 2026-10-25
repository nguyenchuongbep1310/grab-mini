package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const testSecret = "0123456789abcdef0123456789abcdef" // 32 bytes

func newPair(t *testing.T) (*Issuer, *Verifier) {
	t.Helper()
	i, err := NewIssuer(testSecret, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	v, err := NewVerifier(testSecret)
	if err != nil {
		t.Fatal(err)
	}
	return i, v
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
}

func TestAShortSecretIsRefused(t *testing.T) {
	if _, err := NewIssuer("short", time.Hour); err == nil {
		t.Error("NewIssuer accepted a short testSecret")
	}
	if _, err := NewVerifier("short"); err == nil {
		t.Error("NewVerifier accepted a short testSecret")
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
	i, v := newPair(t)
	good := func() jwt.RegisteredClaims {
		return jwt.RegisteredClaims{
			Subject: "rider-1", Issuer: "grab-mini", Audience: jwt.ClaimStrings{"grab-mini-api"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		}
	}
	sign := func(method jwt.SigningMethod, key any, c claims) string {
		s, err := jwt.NewWithClaims(method, c).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

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
		"garbage":            "not a token",
		"empty":              "",
		"wrong testSecret":   sign(jwt.SigningMethodHS256, []byte("a-different-testSecret-of-32-bytes!!!!"), claims{Rider, good()}),
		"expired":            sign(jwt.SigningMethodHS256, []byte(testSecret), claims{Rider, expired}),
		"no expiry":          sign(jwt.SigningMethodHS256, []byte(testSecret), claims{Rider, noExp}),
		"wrong audience":     sign(jwt.SigningMethodHS256, []byte(testSecret), claims{Rider, wrongAud}),
		"wrong issuer":       sign(jwt.SigningMethodHS256, []byte(testSecret), claims{Rider, wrongIss}),
		"no subject":         sign(jwt.SigningMethodHS256, []byte(testSecret), claims{Rider, noSub}),
		"unknown role":       sign(jwt.SigningMethodHS256, []byte(testSecret), claims{"admin", good()}),
		"alg none":           sign(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, claims{Rider, good()}),
		"other HMAC variant": sign(jwt.SigningMethodHS512, []byte(testSecret), claims{Rider, good()}),
		"tampered":           mine[:len(mine)-2] + "xx",
	}
	for name, tok := range cases {
		if _, err := v.Verify(tok); err == nil {
			t.Errorf("%s: token was accepted", name)
		}
	}
	if _, err := v.Verify(mine); err != nil {
		t.Errorf("a good token was rejected: %v", err)
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

func TestSecretFromEnvFailsClosed(t *testing.T) {
	t.Setenv("JWT_SECRET", "")
	t.Setenv("JWT_SECRET_FILE", "")
	for _, env := range []string{"", "production", "staging"} {
		t.Setenv("APP_ENV", env)
		if _, err := SecretFromEnv(); err == nil {
			t.Errorf("APP_ENV=%q without JWT_SECRET fell back to the public secret", env)
		}
		if _, err := VerifierFromEnv(); err == nil {
			t.Errorf("APP_ENV=%q: VerifierFromEnv succeeded without a secret", env)
		}
	}

	t.Setenv("APP_ENV", "development")
	if s, err := SecretFromEnv(); err != nil || s != DevSecret {
		t.Errorf("development: SecretFromEnv = %q, %v; want the dev secret", s, err)
	}

	// Naming the public secret does not make it safe.
	t.Setenv("JWT_SECRET", DevSecret)
	for _, env := range []string{"", "production"} {
		t.Setenv("APP_ENV", env)
		if _, err := SecretFromEnv(); err == nil {
			t.Errorf("APP_ENV=%q accepted the public development secret", env)
		}
	}

	t.Setenv("JWT_SECRET", "0123456789abcdef0123456789abcdef")
	t.Setenv("APP_ENV", "production")
	if s, err := SecretFromEnv(); err != nil || s != "0123456789abcdef0123456789abcdef" {
		t.Errorf("production with a real secret: %q, %v", s, err)
	}
}
