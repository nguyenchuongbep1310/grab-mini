package auth

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

const (
	// MaxJWKSBytes bounds the JWKS response read. A set of a few Ed25519
	// keys is under 1 KB; anything near this is not a key set.
	MaxJWKSBytes = 64 << 10
	// maxJWKSKeys bounds how many keys one response may carry.
	maxJWKSKeys = 20

	defaultJWKSMaxAge     = 10 * time.Minute
	defaultJWKSMinRefresh = 15 * time.Second
	defaultJWKSMaxStale   = time.Hour
	defaultJWKSTimeout    = 5 * time.Second
)

// KeySource finds the public key that verifies tokens carrying a kid.
type KeySource interface {
	PublicKey(ctx context.Context, kid string) (ed25519.PublicKey, error)
}

// JWKSClient is a KeySource backed by an issuer's JWKS endpoint.
//
//   - Keys are fetched lazily on first use (a service can start before the
//     issuer is up) and kept for MaxAge; after that the next verification
//     refreshes them, so a key the issuer removed stops working.
//   - A token with a kid that isn't cached triggers a refresh (the issuer
//     rotated), but no more than one fetch per MinRefresh however many such
//     tokens arrive: forged kids cannot turn this service into a request
//     amplifier against the issuer.
//   - If a refresh fails, keys already held keep working for up to MaxStale
//     beyond MaxAge, so a short issuer outage doesn't log everybody out.
//   - The response is size-limited, must be 200, and a response with no
//     usable key never replaces the cache.
type JWKSClient struct {
	URL        string
	HTTP       *http.Client
	MaxAge     time.Duration
	MinRefresh time.Duration
	MaxStale   time.Duration

	now func() time.Time

	fetchMu sync.Mutex // one fetch at a time; guards lastAttempt and lastErr

	mu          sync.RWMutex // guards keys and fetched
	keys        map[string]ed25519.PublicKey
	fetched     time.Time
	lastAttempt time.Time
	lastErr     error
}

// NewJWKSClient returns a client for the JWKS at url with default timings.
func NewJWKSClient(url string) *JWKSClient {
	return &JWKSClient{
		URL:        url,
		HTTP:       &http.Client{Timeout: defaultJWKSTimeout},
		MaxAge:     defaultJWKSMaxAge,
		MinRefresh: defaultJWKSMinRefresh,
		MaxStale:   defaultJWKSMaxStale,
		now:        time.Now,
	}
}

// PublicKey implements KeySource.
func (c *JWKSClient) PublicKey(ctx context.Context, kid string) (ed25519.PublicKey, error) {
	key, ok, age := c.lookup(kid)
	if ok && age < c.MaxAge {
		return key, nil
	}

	refreshErr := c.refresh(ctx, kid)

	if key, ok, age = c.lookup(kid); ok && (refreshErr == nil || age < c.MaxAge+c.MaxStale) {
		return key, nil
	}
	if refreshErr != nil {
		return nil, fmt.Errorf("%w: %q: %v", ErrUnknownKey, kid, refreshErr)
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownKey, kid)
}

func (c *JWKSClient) lookup(kid string) (key ed25519.PublicKey, ok bool, age time.Duration) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key, ok = c.keys[kid]
	return key, ok, c.now().Sub(c.fetched)
}

// refresh fetches the set unless another goroutine already did, or one was
// tried less than MinRefresh ago (its error, if it failed, is returned again).
func (c *JWKSClient) refresh(ctx context.Context, kid string) error {
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()

	// Someone refreshed while this call waited for the lock.
	if _, ok, age := c.lookup(kid); ok && age < c.MaxAge {
		return nil
	}
	if !c.lastAttempt.IsZero() && c.now().Sub(c.lastAttempt) < c.MinRefresh {
		return c.lastErr
	}

	c.lastAttempt = c.now()
	keys, err := c.fetch(ctx)
	c.lastErr = err
	if err != nil {
		log.Printf("auth: refreshing signing keys from %s: %v", c.URL, err)
		return err
	}
	c.mu.Lock()
	c.keys, c.fetched = keys, c.now()
	c.mu.Unlock()
	return nil
}

func (c *JWKSClient) fetch(ctx context.Context) (map[string]ed25519.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	res, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jwks endpoint answered %d", res.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, MaxJWKSBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > MaxJWKSBytes {
		return nil, fmt.Errorf("jwks response is larger than %d bytes", MaxJWKSBytes)
	}
	return ParseJWKS(body)
}

// ParseJWKS reads the Ed25519 signing keys out of a JWK Set. Keys of other
// types, and keys meant for something other than EdDSA signatures, are
// ignored (an issuer may publish RSA keys for other consumers); it is an
// error if none is usable or a kid appears twice.
func ParseJWKS(data []byte) (map[string]ed25519.PublicKey, error) {
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(data, &set); err != nil {
		return nil, fmt.Errorf("jwks: %w", err)
	}
	if len(set.Keys) > maxJWKSKeys {
		return nil, fmt.Errorf("jwks: %d keys, at most %d accepted", len(set.Keys), maxJWKSKeys)
	}
	keys := map[string]ed25519.PublicKey{}
	for _, k := range set.Keys {
		if k.Kty != "OKP" || k.Crv != "Ed25519" || (k.Alg != "" && k.Alg != jwtAlg) || (k.Use != "" && k.Use != "sig") {
			continue
		}
		if k.Kid == "" {
			return nil, errors.New("jwks: a signing key has no kid")
		}
		x, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil || len(x) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("jwks: key %q is not a valid Ed25519 public key", k.Kid)
		}
		if _, dup := keys[k.Kid]; dup {
			return nil, fmt.Errorf("jwks: kid %q appears twice", k.Kid)
		}
		keys[k.Kid] = ed25519.PublicKey(x)
	}
	if len(keys) == 0 {
		return nil, errors.New("jwks: no usable Ed25519 signing key")
	}
	return keys, nil
}
