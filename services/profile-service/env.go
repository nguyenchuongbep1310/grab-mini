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

	// PublicRatePerMinute is the sustained number of public driver lookups one
	// caller may make per minute (PUBLIC_RATE_PER_MINUTE), on each instance.
	PublicRatePerMinute int
}

func loadEnv() Env {
	return Env{
		SchemaRegistryURL:   getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		Port:                getEnv("PORT", "3012"),
		DatabaseURL:         databaseURL(),
		KafkaBrokers:        []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		PublicRatePerMinute: intEnv("PUBLIC_RATE_PER_MINUTE", 120, 1, 60000),
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
