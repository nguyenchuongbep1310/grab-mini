package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"sort"
)

// Tokens are signed with Ed25519 (JWT alg "EdDSA"). The issuer holds the
// private key; every other service holds only public keys, so a service that
// is compromised can verify tokens but cannot mint one. Each key has a `kid`
// (its RFC 7638 thumbprint), which lets the issuer rotate: publish the new
// key beside the old one, sign with the new one, drop the old one once its
// last token has expired.

const jwtAlg = "EdDSA"

// devKeySeed is the seed of the development signing key. It is public (it is
// in this repository): PrivateKeyFromEnv and VerifierFromEnv only use it when
// APP_ENV=development.
const devKeySeed = "aCHXeNzknap3F7bSO1AI5VnjZD5oSpfGJnKmYqxb+M4="

// DevPrivateKey is the development signing key.
func DevPrivateKey() ed25519.PrivateKey {
	seed, err := base64.StdEncoding.DecodeString(devKeySeed)
	if err != nil {
		panic(err) // a constant
	}
	return ed25519.NewKeyFromSeed(seed)
}

// DevPublicKey is the public half of DevPrivateKey.
func DevPublicKey() ed25519.PublicKey { return DevPrivateKey().Public().(ed25519.PublicKey) }

// KeyID is the `kid` of a public key: its RFC 7638 JWK thumbprint
// (base64url of the SHA-256 of the canonical JWK).
func KeyID(pub ed25519.PublicKey) string {
	// The required members, in lexical order, with no whitespace.
	in := `{"crv":"Ed25519","kty":"OKP","x":"` + b64(pub) + `"}`
	sum := sha256.Sum256([]byte(in))
	return b64(sum[:])
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// ErrUnknownKey means no key with the token's kid is trusted.
var ErrUnknownKey = errors.New("unknown signing key")

// KeySet is a fixed set of public keys by kid. It is a KeySource that never
// touches the network.
type KeySet map[string]ed25519.PublicKey

// NewKeySet indexes keys by KeyID.
func NewKeySet(keys ...ed25519.PublicKey) KeySet {
	s := KeySet{}
	for _, k := range keys {
		s[KeyID(k)] = k
	}
	return s
}

// PublicKey implements KeySource.
func (s KeySet) PublicKey(_ context.Context, kid string) (ed25519.PublicKey, error) {
	if k, ok := s[kid]; ok {
		return k, nil
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownKey, kid)
}

// jwk is the JSON Web Key form of an Ed25519 public key (RFC 8037).
type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Kid string `json:"kid"`
	X   string `json:"x"`
	Use string `json:"use,omitempty"`
	Alg string `json:"alg,omitempty"`
}

// MarshalJWKS renders keys as a JWK Set, ordered by kid so the output is stable.
func MarshalJWKS(keys ...ed25519.PublicKey) []byte {
	set := NewKeySet(keys...)
	kids := make([]string, 0, len(set))
	for kid := range set {
		kids = append(kids, kid)
	}
	sort.Strings(kids)
	out := struct {
		Keys []jwk `json:"keys"`
	}{Keys: []jwk{}}
	for _, kid := range kids {
		out.Keys = append(out.Keys, jwk{Kty: "OKP", Crv: "Ed25519", Kid: kid, X: b64(set[kid]), Use: "sig", Alg: jwtAlg})
	}
	b, _ := json.Marshal(out)
	return b
}

// ParsePrivateKey reads an Ed25519 private key from PEM (PKCS#8), the format
// of `openssl genpkey -algorithm ed25519`.
func ParsePrivateKey(data string) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode([]byte(data))
	if block == nil {
		return nil, errors.New("the signing key is not PEM")
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("the signing key is not PKCS#8: %w", err)
	}
	priv, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("the signing key is %T, want an Ed25519 key", k)
	}
	return priv, nil
}

// ParsePublicKeys reads one or more Ed25519 public keys from PEM (PKIX, the
// output of `openssl pkey -pubout`). Several keys in one bundle is how a
// rotation is expressed.
func ParsePublicKeys(data string) ([]ed25519.PublicKey, error) {
	var keys []ed25519.PublicKey
	rest := []byte(data)
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		k, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("public key: %w", err)
		}
		pub, ok := k.(ed25519.PublicKey)
		if !ok {
			return nil, fmt.Errorf("public key is %T, want an Ed25519 key", k)
		}
		keys = append(keys, pub)
	}
	if len(keys) == 0 {
		return nil, errors.New("no PEM public key found")
	}
	return keys, nil
}
