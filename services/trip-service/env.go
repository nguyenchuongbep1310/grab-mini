package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"grab-mini/pkg/secret"
	"grab-mini/trip-service/internal/domain"
)

type Env struct {
	SchemaRegistryURL string
	Port              string
	DatabaseURL       string
	KafkaBrokers      []string
	RedisAddr         string
	// Pickup/complete driver proximity check (see domain.Proximity).
	Proximity domain.Proximity
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

// loadProximity reads PICKUP_RADIUS_M, DROPOFF_RADIUS_M, LOCATION_MAX_AGE (a
// Go duration, e.g. 60s) and PROXIMITY_CHECK. PROXIMITY_CHECK=off is an error
// unless APP_ENV=development exactly: unset, "production" or anything else
// refuses it, so the bypass can't be switched on by accident.
func loadProximity(getenv func(string) string) (domain.Proximity, error) {
	p := domain.DefaultProximity()
	for _, f := range []struct {
		name string
		dst  *float64
	}{{"PICKUP_RADIUS_M", &p.PickupRadiusM}, {"DROPOFF_RADIUS_M", &p.DropoffRadiusM}} {
		if v := getenv(f.name); v != "" {
			n, err := strconv.ParseFloat(v, 64)
			if err != nil || n <= 0 || n > 100_000 {
				return p, fmt.Errorf("%s=%q: want a number of metres above 0", f.name, v)
			}
			*f.dst = n
		}
	}
	if v := getenv("LOCATION_MAX_AGE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return p, fmt.Errorf("LOCATION_MAX_AGE=%q: want a positive duration such as 60s", v)
		}
		p.MaxLocationAge = d
	}
	switch strings.ToLower(getenv("PROXIMITY_CHECK")) {
	case "", "on":
	case "off":
		if getenv("APP_ENV") != "development" {
			return p, errors.New("PROXIMITY_CHECK=off is only accepted with APP_ENV=development")
		}
		p.Off = true
	default:
		return p, fmt.Errorf("PROXIMITY_CHECK=%q: want on or off", getenv("PROXIMITY_CHECK"))
	}
	return p, nil
}
