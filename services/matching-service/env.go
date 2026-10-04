package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Env struct {
	SchemaRegistryURL string
	RedisAddr         string
	KafkaBrokers      []string
	MatchRadiusKm     float64
	MatchRetries      int
	MatchRetryDelay   int // milliseconds
	InstanceID        string
	// How long a claimed driver stays ASSIGNED waiting for Trip Service to
	// confirm the trip, before they can be claimed again.
	AssignLease time.Duration
	// Port for /livez and /readyz; empty (the default) means no health server.
	HealthPort string
}

func loadEnv() Env {
	return Env{
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:      []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		MatchRadiusKm:     getEnvFloat("MATCH_RADIUS_KM", 5),
		MatchRetries:      getEnvInt("MATCH_RETRIES", 3),
		MatchRetryDelay:   getEnvInt("MATCH_RETRY_DELAY_MS", 2000),
		InstanceID:        getEnv("INSTANCE_ID", fmt.Sprintf("matching-%d", os.Getpid())),
		AssignLease:       time.Duration(getEnvInt("ASSIGN_LEASE_SECONDS", 120)) * time.Second,
		HealthPort:        os.Getenv("HEALTH_PORT"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return fallback
}
