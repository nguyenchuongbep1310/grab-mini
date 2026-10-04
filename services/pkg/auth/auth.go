// Package auth identifies who is calling, from a signed JWT.
//
// A token says who the caller is (Subject: "rider-1") and what kind of caller
// they are (Role: rider or driver). Services never trust an id sent in a
// request body or URL as proof of identity: they take it from the token and
// compare, so a rider can't rate someone else's trip by naming their id.
//
// Tokens are HS256, signed with a secret shared by the issuer and the
// services. That is enough while one component issues them; with a real
// identity provider, switch to asymmetric keys so services hold only the
// public half and can't mint tokens themselves.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

// DevSecret signs tokens when JWT_SECRET isn't set. It is public (it is in
// this repository), so SecretFromEnv refuses it in production.
const DevSecret = "grab-mini-dev-secret-not-for-production!"

// SecretFromEnv is the signing secret: JWT_SECRET, or JWT_SECRET_FILE, or
// DevSecret outside production. Every service that issues or verifies tokens
// must use the same one.
func SecretFromEnv() (string, error) { return secret.GetOr("JWT_SECRET", DevSecret) }

// VerifierFromEnv is a Verifier using SecretFromEnv.
func VerifierFromEnv() (*Verifier, error) {
	s, err := SecretFromEnv()
	if err != nil {
		return nil, err
	}
	return NewVerifier(s)
}

// Identity is who a verified token belongs to.
type Identity struct {
	Subject string
	Role    Role
}

const (
	issuer   = "grab-mini"
	audience = "grab-mini-api"

	// MinSecretLength is the shortest signing secret accepted: HS256 is only
	// as strong as its key.
	MinSecretLength = 32
)

type claims struct {
	Role Role `json:"role"`
	jwt.RegisteredClaims
}

// Issuer signs tokens.
type Issuer struct {
	secret []byte
	ttl    time.Duration
	now    func() time.Time
}

// NewIssuer returns an Issuer whose tokens last ttl.
func NewIssuer(secret string, ttl time.Duration) (*Issuer, error) {
	if len(secret) < MinSecretLength {
		return nil, fmt.Errorf("the JWT secret must be at least %d bytes, got %d", MinSecretLength, len(secret))
	}
	return &Issuer{secret: []byte(secret), ttl: ttl, now: time.Now}, nil
}

// Issue signs a token for role/subject and returns it with its expiry.
func (i *Issuer) Issue(role Role, subject string) (string, time.Time, error) {
	if !role.Valid() || subject == "" {
		return "", time.Time{}, errors.New("a token needs a valid role and a subject")
	}
	now := i.now()
	exp := now.Add(i.ttl)
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims{
		Role: role,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   subject,
			Issuer:    issuer,
			Audience:  jwt.ClaimStrings{audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	})
	signed, err := token.SignedString(i.secret)
	return signed, exp, err
}

// Verifier checks tokens.
type Verifier struct {
	secret []byte
	parser *jwt.Parser
}

func NewVerifier(secret string) (*Verifier, error) {
	if len(secret) < MinSecretLength {
		return nil, fmt.Errorf("the JWT secret must be at least %d bytes, got %d", MinSecretLength, len(secret))
	}
	return &Verifier{
		secret: []byte(secret),
		parser: jwt.NewParser(
			// Pin the algorithm: accepting whatever the token's header says is
			// how "alg: none" and key-confusion attacks work.
			jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
			jwt.WithIssuer(issuer),
			jwt.WithAudience(audience),
			jwt.WithExpirationRequired(),
			jwt.WithLeeway(5*time.Second),
		),
	}, nil
}

// ErrInvalidToken is wrapped by every verification failure.
var ErrInvalidToken = errors.New("invalid token")

func (v *Verifier) Verify(token string) (Identity, error) {
	var c claims
	if _, err := v.parser.ParseWithClaims(token, &c, func(*jwt.Token) (any, error) { return v.secret, nil }); err != nil {
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
		id, err := v.Verify(token)
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
