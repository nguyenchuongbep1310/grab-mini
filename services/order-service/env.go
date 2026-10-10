package main

import (
	"log"
	"os"
	"strconv"

	"grab-mini/pkg/secret"
)

type Env struct {
	SchemaRegistryURL string
	Port              string
	DatabaseURL       string
	KafkaBrokers      []string
	// Surge multipliers are read from here; if it is down bookings price at 1.0.
	RedisAddr string
	// How far the multiplier may move from an expired quote before the rider
	// is asked to confirm a new price (409).
	QuoteTolerance float64
}

func loadEnv() Env {
	return Env{
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		Port:              getEnv("PORT", "3001"),
		DatabaseURL:       databaseURL(),
		KafkaBrokers:      []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		QuoteTolerance:    getEnvFloat("SURGE_QUOTE_TOLERANCE", 0.2),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// databaseURL holds the database password, so it is a secret: DATABASE_URL_FILE
// or DATABASE_URL, and the local development default only outside production.
func databaseURL() string {
	v, err := secret.GetOr("DATABASE_URL", "postgres://grabmini:grabmini@localhost:5432/grabmini")
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func getEnvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n >= 0 {
			return n
		}
	}
	return fallback
}
