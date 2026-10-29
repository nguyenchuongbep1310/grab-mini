package main

import (
	"crypto/ed25519"
	"log"
	"os"
	"strings"
	"time"

	"grab-mini/pkg/auth"
)

type Env struct {
	Port string
	// SigningKey signs tokens; it is the only copy of the private key in the system.
	SigningKey ed25519.PrivateKey
	// PreviousKeys are retired public keys still published until their tokens expire.
	PreviousKeys []ed25519.PublicKey
	TokenTTL     time.Duration
	// AllowDevTokens must be set to run in production: this service hands a
	// token to anyone who asks, which is only right for development.
	AllowDevTokens bool
}

func loadEnv() Env {
	key, err := auth.PrivateKeyFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	previous, err := auth.PreviousPublicKeysFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	ttl, err := time.ParseDuration(getEnv("TOKEN_TTL", "1h"))
	if err != nil {
		log.Fatalf("TOKEN_TTL: %v", err)
	}
	return Env{
		Port:           getEnv("PORT", "3005"),
		SigningKey:     key,
		PreviousKeys:   previous,
		TokenTTL:       ttl,
		AllowDevTokens: strings.EqualFold(os.Getenv("ALLOW_DEV_TOKENS"), "true"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
