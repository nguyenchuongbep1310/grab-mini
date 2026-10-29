// Package auth identifies who is calling, from a signed JWT.
//
// A token says who the caller is (Subject: "rider-1") and what kind of caller
// they are (Role: rider or driver). Services never trust an id sent in a
// request body or URL as proof of identity: they take it from the token and
// compare, so a rider can't rate someone else's trip by naming their id.
//
// Tokens are EdDSA (Ed25519) JWTs with a `kid` header. Only the issuer
// (auth-service, or a real identity provider) holds the private key; every
// other service verifies with public keys alone, taken from the issuer's
// JWKS endpoint (JWT_JWKS_URL) or from a local PEM file (JWT_PUBLIC_KEY_FILE),
// so a compromised service cannot mint tokens. There is deliberately no
// HS256 / shared-secret mode: see docs/security.md for the migration.
package auth

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"grab-mini/pkg/secret"
)

type Role string

const (
	Rider  Role = "rider"
	Driver Role = "driver"
)

// Valid reports whether r is a role this system issues tokens for.
func (r Role) Valid() bool { return r == Rider || r == Driver }

// PrivateKeyFromEnv is the issuer's signing key: a PKCS#8 PEM Ed25519 key in
// JWT_PRIVATE_KEY or the file JWT_PRIVATE_KEY_FILE. The published development
// key is used only when neither is set and APP_ENV=development; with
// anything else (including APP_ENV unset) a missing key is an error, and so is
// the development key set explicitly: naming a public key does not make it safe.
func PrivateKeyFromEnv() (ed25519.PrivateKey, error) {
	pemText, err := secret.Get("JWT_PRIVATE_KEY")
	switch {
	case err == nil:
		key, err := ParsePrivateKey(pemText)
		if err != nil {
			return nil, err
		}
		if key.Equal(DevPrivateKey()) && !secret.Development() {
			return nil, errors.New("JWT_PRIVATE_KEY is the published development key; it is accepted only with APP_ENV=development")
		}
		return key, nil
	case !errors.Is(err, secret.ErrMissing):
		return nil, err
	case secret.Development():
		return DevPrivateKey(), nil
	}
	return nil, fmt.Errorf("%w (set it or JWT_PRIVATE_KEY_FILE to an Ed25519 PEM key; the development key is only used when APP_ENV=development)", err)
}

// PreviousPublicKeysFromEnv are public keys the issuer keeps publishing after
// it stopped signing with them, so tokens signed before a rotation stay valid
// until they expire: JWT_PREVIOUS_PUBLIC_KEYS (or _FILE), a PEM bundle. Unset
// is fine.
func PreviousPublicKeysFromEnv() ([]ed25519.PublicKey, error) {
	pemText, err := secret.Get("JWT_PREVIOUS_PUBLIC_KEYS")
	if errors.Is(err, secret.ErrMissing) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ParsePublicKeys(pemText)
}

// KeySourceFromEnv says where a verifying service gets its public keys:
//
//	JWT_JWKS_URL                  the issuer's JWKS endpoint (fetched, cached)
//	JWT_PUBLIC_KEY / _FILE        a PEM bundle of trusted public keys
//	APP_ENV=development, neither  the published development public key
//
// Exactly one of the first two may be set. JWT_SECRET is no longer used.
func KeySourceFromEnv() (KeySource, error) {
	if os.Getenv("JWT_SECRET") != "" || os.Getenv("JWT_SECRET_FILE") != "" {
		log.Printf("auth: JWT_SECRET is ignored: tokens are verified with the issuer's public key (JWT_JWKS_URL or JWT_PUBLIC_KEY_FILE)")
	}
	jwksURL := os.Getenv("JWT_JWKS_URL")
	pemText, pemErr := secret.Get("JWT_PUBLIC_KEY")
	if pemErr != nil && !errors.Is(pemErr, secret.ErrMissing) {
		return nil, pemErr
	}
	havePEM := pemErr == nil

	switch {
	case jwksURL != "" && havePEM:
		return nil, errors.New("set only one of JWT_JWKS_URL and JWT_PUBLIC_KEY(_FILE)")
	case jwksURL != "":
		u, err := url.Parse(jwksURL)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			return nil, fmt.Errorf("JWT_JWKS_URL %q is not an http(s) URL", jwksURL)
		}
		return NewJWKSClient(jwksURL), nil
	case havePEM:
		keys, err := ParsePublicKeys(pemText)
		if err != nil {
			return nil, fmt.Errorf("JWT_PUBLIC_KEY: %w", err)
		}
		if !secret.Development() {
			for _, k := range keys {
				if k.Equal(DevPublicKey()) {
					return nil, errors.New("JWT_PUBLIC_KEY trusts the published development key, whose private half is in this repository; it is accepted only with APP_ENV=development")
				}
			}
		}
		return NewKeySet(keys...), nil
	case secret.Development():
		return NewKeySet(DevPublicKey()), nil
	}
	return nil, errors.New("no way to verify tokens: set JWT_JWKS_URL, or JWT_PUBLIC_KEY_FILE (the development key is only used when APP_ENV=development)")
}

// VerifierFromEnv is a Verifier using KeySourceFromEnv.
func VerifierFromEnv() (*Verifier, error) {
	src, err := KeySourceFromEnv()
	if err != nil {
		return nil, err
	}
	return NewVerifier(src), nil
}

// Identity is who a verified token belongs to.
type Identity struct {
	Subject string
	Role    Role
}

const (
	issuer   = "grab-mini"
	audience = "grab-mini-api"
)

type claims struct {
	Role Role `json:"role"`
	jwt.RegisteredClaims
}

// Issuer signs tokens.
type Issuer struct {
	key ed25519.PrivateKey
	kid string
	ttl time.Duration
	now func() time.Time
}

// NewIssuer returns an Issuer whose tokens last ttl.
func NewIssuer(key ed25519.PrivateKey, ttl time.Duration) (*Issuer, error) {
	if len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("the signing key is not an Ed25519 private key")
	}
	pub := key.Public().(ed25519.PublicKey)
	return &Issuer{key: key, kid: KeyID(pub), ttl: ttl, now: time.Now}, nil
}

// WithTTL returns an Issuer with the same key and a different token lifetime
// (a negative one makes already-expired tokens, for tests).
func (i *Issuer) WithTTL(ttl time.Duration) *Issuer {
	c := *i
	c.ttl = ttl
	return &c
}

// KeyID is the kid of the tokens this Issuer signs.
func (i *Issuer) KeyID() string { return i.kid }

// JWKS is the JWK Set to publish: the signing key's public half plus any
// previous keys still needed to verify unexpired tokens.
func (i *Issuer) JWKS(previous ...ed25519.PublicKey) []byte {
	return MarshalJWKS(append([]ed25519.PublicKey{i.key.Public().(ed25519.PublicKey)}, previous...)...)
}

// Issue signs a token for role/subject and returns it with its expiry.
func (i *Issuer) Issue(role Role, subject string) (string, time.Time, error) {
	if !role.Valid() || subject == "" {
		return "", time.Time{}, errors.New("a token needs a valid role and a subject")
	}
	now := i.now()
	exp := now.Add(i.ttl)
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims{
		Role: role,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   subject,
			Issuer:    issuer,
			Audience:  jwt.ClaimStrings{audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	})
	token.Header["kid"] = i.kid
	signed, err := token.SignedString(i.key)
	return signed, exp, err
}

// Verifier checks tokens.
type Verifier struct {
	keys   KeySource
	parser *jwt.Parser
}

// NewVerifier returns a Verifier that trusts the public keys keys provides.
func NewVerifier(keys KeySource) *Verifier {
	return &Verifier{
		keys: keys,
		parser: jwt.NewParser(
			// Pin the algorithm: accepting whatever the token's header says is
			// how "alg: none" and key-confusion attacks work. Only EdDSA is
			// allowed, and the key type is checked again in the key function.
			jwt.WithValidMethods([]string{jwtAlg}),
			jwt.WithIssuer(issuer),
			jwt.WithAudience(audience),
			jwt.WithExpirationRequired(),
			jwt.WithLeeway(5*time.Second),
		),
	}
}

// ErrInvalidToken is wrapped by every verification failure.
var ErrInvalidToken = errors.New("invalid token")

// Verify checks token's signature, issuer, audience and expiry.
func (v *Verifier) Verify(token string) (Identity, error) {
	return v.VerifyContext(context.Background(), token)
}

// VerifyContext is Verify with a context bounding any key fetch it causes.
func (v *Verifier) VerifyContext(ctx context.Context, token string) (Identity, error) {
	var c claims
	keyFunc := func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodEd25519); !ok {
			return nil, fmt.Errorf("unexpected signing method %q", t.Method.Alg())
		}
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			return nil, errors.New("token has no kid")
		}
		return v.keys.PublicKey(ctx, kid)
	}
	if _, err := v.parser.ParseWithClaims(token, &c, keyFunc); err != nil {
		return Identity{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if c.Subject == "" || !c.Role.Valid() {
		return Identity{}, fmt.Errorf("%w: no subject or unknown role", ErrInvalidToken)
	}
	return Identity{Subject: c.Subject, Role: c.Role}, nil
}

type ctxKey struct{}

// With returns ctx carrying id, as Require does after verifying a token.
// Handler tests use it to call a handler as someone.
func With(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// From returns the caller a Require middleware verified.
func From(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(ctxKey{}).(Identity)
	return id, ok
}

// Require wraps h so it only runs for a caller with a valid token and one of
// roles (any role if none are given), answering 401 or 403 otherwise.
// Read the caller with From.
func (v *Verifier) Require(h http.Handler, roles ...Role) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			Deny(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		id, err := v.VerifyContext(r.Context(), token)
		if err != nil {
			Deny(w, http.StatusUnauthorized, "invalid or expired token")
			return
		}
		if !allowed(id.Role, roles) {
			Deny(w, http.StatusForbidden, "this role can't do that")
			return
		}
		h.ServeHTTP(w, r.WithContext(With(r.Context(), id)))
	})
}

func allowed(role Role, roles []Role) bool {
	if len(roles) == 0 {
		return true
	}
	for _, r := range roles {
		if r == role {
			return true
		}
	}
	return false
}

// Deny writes a JSON error. Services use it for ownership failures too, so
// every refusal looks the same.
func Deny(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer`)
	}
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// WebSocketSubprotocol is how a browser passes a token on a WebSocket, which
// can't set an Authorization header: new WebSocket(url, ["bearer", token]).
// A query string would work too but ends up in access logs and browser history.
const WebSocketSubprotocol = "bearer"

// TokenFromWebSocket extracts the token from the Sec-WebSocket-Protocol
// header ("bearer, <token>").
func TokenFromWebSocket(r *http.Request) (string, bool) {
	for _, line := range r.Header.Values("Sec-WebSocket-Protocol") {
		parts := strings.Split(line, ",")
		for i, p := range parts {
			if strings.TrimSpace(p) == WebSocketSubprotocol && i+1 < len(parts) {
				if t := strings.TrimSpace(parts[i+1]); t != "" {
					return t, true
				}
			}
		}
	}
	return "", false
}
