package main

import (
	"log"
	"os"

	"grab-mini/pkg/secret"
)

type Env struct {
	SchemaRegistryURL string
	Port              string
	DatabaseURL       string
	KafkaBrokers      []string
	RedisAddr         string
	// Pickup/complete driver proximity check (see proximity.go).
	Proximity Proximity
}

func loadEnv() Env {
	prox, err := loadProximity(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	if prox.Off {
		log.Printf("WARNING: PROXIMITY_CHECK=off (APP_ENV=development): drivers can pick up and complete from anywhere")
	}
	return Env{
		Proximity:         prox,
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		Port:              getEnv("PORT", "3002"),
		DatabaseURL:       databaseURL(),
		KafkaBrokers:      []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
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
