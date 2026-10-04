package main

import (
	"log"
	"os"
	"strings"

	"grab-mini/pkg/secret"
)

type Env struct {
	SchemaRegistryURL string
	Port              string
	KafkaBrokers      []string
	RedisAddr         string
	// Origins allowed to open a WebSocket from a browser page.
	AllowedOrigins []string
}

func loadEnv() Env {
	return Env{
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		Port:              getEnv("PORT", "3003"),
		KafkaBrokers:      []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		AllowedOrigins:    allowedOrigins(),
	}
}

// devOrigins are where the three Vite dev servers run.
var devOrigins = "http://localhost:5171,http://localhost:5172,http://localhost:5173," +
	"http://127.0.0.1:5171,http://127.0.0.1:5172,http://127.0.0.1:5173"

// allowedOrigins is ALLOWED_ORIGINS (comma separated). Production must set
// it: an unset list would otherwise quietly mean "the dev servers".
func allowedOrigins() []string {
	v := os.Getenv("ALLOWED_ORIGINS")
	if v == "" {
		if secret.Production() {
			log.Fatal("ALLOWED_ORIGINS must be set when APP_ENV=production (comma-separated, e.g. https://app.example.com)")
		}
		v = devOrigins
	}
	var out []string
	for _, o := range strings.Split(v, ",") {
		if o = strings.TrimSpace(o); o != "" {
			out = append(out, o)
		}
	}
	return out
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
