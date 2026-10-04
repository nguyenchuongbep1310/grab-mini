package main

import (
	"log"
	"os"
	"strings"
	"time"

	"grab-mini/pkg/auth"
)

type Env struct {
	Port      string
	JWTSecret string
	TokenTTL  time.Duration
	// AllowDevTokens must be set to run in production: this service hands a
	// token to anyone who asks, which is only right for development.
	AllowDevTokens bool
}

func loadEnv() Env {
	jwtSecret, err := auth.SecretFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	ttl, err := time.ParseDuration(getEnv("TOKEN_TTL", "1h"))
	if err != nil {
		log.Fatalf("TOKEN_TTL: %v", err)
	}
	return Env{
		Port:           getEnv("PORT", "3005"),
		JWTSecret:      jwtSecret,
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
