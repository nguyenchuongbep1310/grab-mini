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

	// Grace is how long a finished trip's chat stays writable (CHAT_GRACE_MINUTES):
	// time for "thanks" or "you left your phone in the car".
	Grace time.Duration
	// RetentionDays is how long messages are kept (CHAT_RETENTION_DAYS).
	RetentionDays int
	// RatePerMinute is the sustained number of messages one sender may send per
	// minute (CHAT_RATE_PER_MINUTE), on each instance.
	RatePerMinute int
}

func loadEnv() Env {
	return Env{
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		Port:              getEnv("PORT", "3011"),
		DatabaseURL:       databaseURL(),
		KafkaBrokers:      []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		Grace:             time.Duration(intEnv("CHAT_GRACE_MINUTES", 10, 0, 24*60)) * time.Minute,
		RetentionDays:     intEnv("CHAT_RETENTION_DAYS", 30, 1, 3650),
		RatePerMinute:     intEnv("CHAT_RATE_PER_MINUTE", 20, 1, 6000),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// intEnv reads a whole number in [min, max]; a bad value stops the service at
// startup instead of running with a limit nobody chose.
func intEnv(key string, fallback, min, max int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min || n > max {
		log.Fatalf("%s: %q is not a whole number from %d to %d", key, v, min, max)
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
