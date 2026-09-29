package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Validation happens before the database or Kafka is touched, so a server
// with no dependencies is enough to test what gets rejected.
func post(body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	(&server{}).handleCreateOrder(rec, req)
	return rec
}

func TestCreateOrderRejectsBadInput(t *testing.T) {
	const where = `"pickup":{"lat":10.776,"lng":106.7},"dropoff":{"lat":10.79,"lng":106.71}`
	cases := map[string]string{
		"not JSON":              `nope`,
		"missing rider":         `{` + where + `}`,
		"missing pickup":        `{"riderId":"r","dropoff":{"lat":10.79,"lng":106.71}}`,
		"latitude out of range": `{"riderId":"r","pickup":{"lat":91,"lng":0},"dropoff":{"lat":10.79,"lng":106.71}}`,
		"unknown vehicle type":  `{"riderId":"r",` + where + `,"vehicleType":"TRUCK"}`,
		"lowercase vehicle":     `{"riderId":"r",` + where + `,"vehicleType":"car"}`,
	}
	for name, body := range cases {
		if rec := post(body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
	}
}

func TestVehicleTypeValidation(t *testing.T) {
	rec := post(`{"riderId":"r","pickup":{"lat":10.776,"lng":106.7},"dropoff":{"lat":10.79,"lng":106.71},"vehicleType":"TRUCK"}`)
	if !strings.Contains(rec.Body.String(), "CAR or BIKE") {
		t.Errorf("body = %q, want a message naming the allowed types", rec.Body.String())
	}
}

func TestAnOutcomeForANonUUIDOrderIsPermanent(t *testing.T) {
	// The guard runs before the database, so no connection is needed.
	err := setOutcome(context.Background(), nil, "not-a-uuid", "MATCHED")
	var bad *badOutcome
	if !errors.As(err, &bad) || !bad.Permanent() {
		t.Errorf("err = %v, want a permanent badOutcome (retrying can't fix it)", err)
	}
}
