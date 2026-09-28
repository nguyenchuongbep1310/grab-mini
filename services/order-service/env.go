package main

import "os"

type Env struct {
	Port         string
	DatabaseURL  string
	KafkaBrokers []string
}

func loadEnv() Env {
	return Env{
		Port:         getEnv("PORT", "3001"),
		DatabaseURL:  getEnv("DATABASE_URL", "postgres://grabmini:grabmini@localhost:5432/grabmini"),
		KafkaBrokers: []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
