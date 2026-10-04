package main

import "os"

type Env struct {
	SchemaRegistryURL string
	RedisAddr         string
	KafkaBrokers      []string
	// Port for /livez and /readyz; empty (the default) means no health server.
	HealthPort string
}

func loadEnv() Env {
	return Env{
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:      []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		HealthPort:        os.Getenv("HEALTH_PORT"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
