package main

import (
	"log"
	"os"
	"strconv"
	"time"
)

type Env struct {
	Port              string
	KafkaBrokers      []string
	SchemaRegistryURL string

	MinInterval time.Duration // fastest a driver may report
	MinMove     float64       // metres
	Heartbeat   time.Duration // publish at least this often even if parked
	MaxSkew     time.Duration // how far a device clock may differ from ours before we ignore it
}

func loadEnv() Env {
	return Env{
		Port:              getEnv("PORT", "3006"),
		KafkaBrokers:      []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		MinInterval:       getDuration("MIN_REPORT_INTERVAL", time.Second),
		MinMove:           getFloat("MIN_MOVE_METERS", 5),
		Heartbeat:         getDuration("HEARTBEAT", 30*time.Second),
		MaxSkew:           getDuration("MAX_CLOCK_SKEW", 30*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("%s: %v", key, err)
	}
	return d
}

func getFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Fatalf("%s: %v", key, err)
	}
	return f
}
