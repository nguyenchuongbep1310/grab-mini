package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"grab-mini/pkg/secret"
)

type Env struct {
	SchemaRegistryURL string
	Port              string
	DatabaseURL       string
	KafkaBrokers      []string
	Provider          string
	Policy            Policy
}

func loadEnv() Env {
	env := Env{
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		Port:              getEnv("PORT", "3008"),
		DatabaseURL:       databaseURL(),
		KafkaBrokers:      []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		Provider:          getEnv("PAYMENT_PROVIDER", "fake"),
		Policy: Policy{
			// 20% of a fare is the platform's; tips are the driver's in full.
			CommissionBps: getInt("PLATFORM_COMMISSION_BPS", 2000),
			// A rider who cancels after the driver was assigned and the grace period
			// has passed pays this much (capped at the ride's price); 0 makes
			// cancelling free.
			CancellationFee:   getInt("CANCELLATION_FEE_VND", 10000),
			CancellationGrace: time.Duration(getInt("CANCELLATION_GRACE_SECONDS", 180)) * time.Second,
		},
	}
	if err := env.Policy.Validate(); err != nil {
		log.Fatalf("invalid payment policy: %v", err)
	}
	return env
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getInt reads an integer setting, and refuses to start on a malformed one: a
// typo in a money setting must not silently fall back to a default.
func getInt(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		log.Fatalf("%s=%q is not an integer", key, v)
	}
	return n
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
