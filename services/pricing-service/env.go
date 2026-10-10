package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"grab-mini/pkg/surge"
)

type Env struct {
	SchemaRegistryURL string
	RedisAddr         string
	KafkaBrokers      []string
	Port              string
	InstanceID        string

	// Surge model (see package surge).
	K              float64       // how much each extra unit of demand per driver adds
	MaxMultiplier  float64       // ceiling on a multiplier
	Alpha          float64       // EMA weight of the newest sample, (0,1]
	MaxStep        float64       // largest change of a multiplier per tick
	WindowMinutes  int           // demand is averaged over this many minute buckets
	SampleInterval time.Duration // how often supply is sampled and multipliers refreshed
	CellDegrees    float64
}

func loadEnv() Env {
	return Env{
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:      []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		Port:              getEnv("PORT", "3007"),
		InstanceID:        getEnv("INSTANCE_ID", fmt.Sprintf("pricing-%d", os.Getpid())),

		K:              getEnvFloat("SURGE_K", 0.5),
		MaxMultiplier:  getEnvFloat("SURGE_MAX", surge.MaxMultiplier),
		Alpha:          getEnvFloat("SURGE_ALPHA", 0.5),
		MaxStep:        getEnvFloat("SURGE_MAX_STEP", 0.3),
		WindowMinutes:  getEnvInt("SURGE_WINDOW_MINUTES", 3),
		SampleInterval: time.Duration(getEnvInt("SURGE_SAMPLE_SECONDS", 5)) * time.Second,
		CellDegrees:    surge.CellDegrees(),
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
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
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
