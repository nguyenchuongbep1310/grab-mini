package main

import "os"

type Env struct {
	RedisAddr    string
	KafkaBrokers []string
}

func loadEnv() Env {
	return Env{
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers: []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
