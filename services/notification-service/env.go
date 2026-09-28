package main

import "os"

type Env struct {
	Port         string
	KafkaBrokers []string
}

func loadEnv() Env {
	return Env{
		Port:         getEnv("PORT", "3003"),
		KafkaBrokers: []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
