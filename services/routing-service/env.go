package main

import "os"

type Env struct {
	Port       string
	InstanceID string
}

func loadEnv() Env {
	return Env{
		Port:       getEnv("PORT", "3009"),
		InstanceID: getEnv("INSTANCE_ID", "routing"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
