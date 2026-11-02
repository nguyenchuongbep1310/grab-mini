package main

import (
	"testing"
	"time"

	"grab-mini/trip-service/internal/domain"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadProximityDefaults(t *testing.T) {
	p, err := loadProximity(env(nil))
	if err != nil || p != domain.DefaultProximity() {
		t.Fatalf("got %+v, %v; want the defaults", p, err)
	}
	if p.PickupRadiusM != 200 || p.DropoffRadiusM != 300 || p.MaxLocationAge != time.Minute || p.Off {
		t.Errorf("defaults are %+v, want 200m / 300m / 60s / on", p)
	}
}

func TestLoadProximityReadsTheRadii(t *testing.T) {
	p, err := loadProximity(env(map[string]string{"PICKUP_RADIUS_M": "50.5", "DROPOFF_RADIUS_M": "1000", "LOCATION_MAX_AGE": "15s"}))
	if err != nil || p.PickupRadiusM != 50.5 || p.DropoffRadiusM != 1000 || p.MaxLocationAge != 15*time.Second {
		t.Errorf("got %+v, %v", p, err)
	}
	for _, bad := range []map[string]string{
		{"PICKUP_RADIUS_M": "abc"}, {"PICKUP_RADIUS_M": "0"}, {"DROPOFF_RADIUS_M": "-5"},
		{"LOCATION_MAX_AGE": "soon"}, {"LOCATION_MAX_AGE": "0s"}, {"PROXIMITY_CHECK": "maybe"},
	} {
		if _, err := loadProximity(env(bad)); err == nil {
			t.Errorf("%v was accepted", bad)
		}
	}
}

func TestTheCheckCanOnlyBeTurnedOffInDevelopment(t *testing.T) {
	for _, appEnv := range []string{"", "production", "staging", "Development "} {
		if _, err := loadProximity(env(map[string]string{"PROXIMITY_CHECK": "off", "APP_ENV": appEnv})); err == nil {
			t.Errorf("PROXIMITY_CHECK=off was accepted with APP_ENV=%q", appEnv)
		}
	}
	p, err := loadProximity(env(map[string]string{"PROXIMITY_CHECK": "off", "APP_ENV": "development"}))
	if err != nil || !p.Off {
		t.Errorf("development + off = %+v, %v; want the check off", p, err)
	}
	if p, err := loadProximity(env(map[string]string{"PROXIMITY_CHECK": "on", "APP_ENV": "production"})); err != nil || p.Off {
		t.Errorf("explicit on = %+v, %v", p, err)
	}
}
