package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/events"
)

// Proximity: a driver can only confirm pickup near the pickup point and
// complete near the dropoff, going by the last location they reported (the
// same Redis data Matching uses). Without it a driver could "complete" trips
// from the couch.

const (
	defaultPickupRadiusM  = 200
	defaultDropoffRadiusM = 300
	defaultMaxLocationAge = 60 * time.Second
)

// Proximity is the configuration of the check.
type Proximity struct {
	PickupRadiusM  float64
	DropoffRadiusM float64
	// A last known location older than this is refused as stale.
	MaxLocationAge time.Duration
	// Off skips the check. Only a development stack may turn it off (see
	// loadProximity): the driver-app-sim random-walks near its seed point and
	// never drives to a rider.
	Off bool
}

func defaultProximity() Proximity {
	return Proximity{PickupRadiusM: defaultPickupRadiusM, DropoffRadiusM: defaultDropoffRadiusM, MaxLocationAge: defaultMaxLocationAge}
}

// loadProximity reads PICKUP_RADIUS_M, DROPOFF_RADIUS_M, LOCATION_MAX_AGE (a
// Go duration, e.g. 60s) and PROXIMITY_CHECK. PROXIMITY_CHECK=off is an error
// unless APP_ENV=development exactly: unset, "production" or anything else
// refuses it, so the bypass can't be switched on by accident.
func loadProximity(getenv func(string) string) (Proximity, error) {
	p := defaultProximity()
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

var proximityRefusals = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "trip_proximity_refused_total",
	Help: "Pickup/complete requests refused by the driver proximity check, by step and reason (too_far, location_unknown, location_stale, location_unavailable).",
}, []string{"step", "reason"})

// proximityRefusal is why a pickup/complete was refused.
type proximityRefusal struct {
	Status int    // HTTP status
	Code   string // stable machine-readable reason
	Msg    string
	// Set for too_far.
	DistanceM, MaxM float64
}

func (r *proximityRefusal) write(w http.ResponseWriter) {
	body := map[string]any{"error": r.Msg, "code": r.Code}
	if r.Code == "driver_too_far" {
		body["distanceM"], body["maxM"] = int(r.DistanceM+0.5), int(r.MaxM)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(r.Status)
	json.NewEncoder(w).Encode(body)
}

// tripRoute is where the trip starts and ends; nil for trips created before
// the route was recorded.
type tripRoute struct{ Pickup, Dropoff *events.LatLng }

func getTripRoute(ctx context.Context, db *pgxpool.Pool, tripID string) (tripRoute, error) {
	var pLat, pLng, dLat, dLng *float64
	err := db.QueryRow(ctx, `SELECT pickup_lat, pickup_lng, dropoff_lat, dropoff_lng FROM trips WHERE id = $1`, tripID).
		Scan(&pLat, &pLng, &dLat, &dLng)
	if errors.Is(err, pgx.ErrNoRows) {
		return tripRoute{}, errTripNotFound
	}
	if err != nil {
		return tripRoute{}, err
	}
	var r tripRoute
	if pLat != nil && pLng != nil {
		r.Pickup = &events.LatLng{Lat: *pLat, Lng: *pLng}
	}
	if dLat != nil && dLng != nil {
		r.Dropoff = &events.LatLng{Lat: *dLat, Lng: *dLng}
	}
	return r, nil
}

// checkProximity decides whether the driver is close enough to target. It
// returns nil to allow, or the refusal to send.
//
// Policy: the check fails closed. Redis being down is a 503 (the driver
// retries), not a pass: letting everyone through whenever Redis hiccups would
// make the check worthless. A trip with no recorded route (created before
// this feature) is allowed, with a log line; those trips are short-lived.
func checkProximity(ctx context.Context, rdb *redis.Client, cfg Proximity, now time.Time, step, driverID string, target *events.LatLng, radiusM float64) *proximityRefusal {
	if cfg.Off {
		return nil
	}
	if target == nil {
		log.Printf("trip proximity: no recorded %s for driver=%s's trip, skipping the check", step, driverID)
		return nil
	}
	refuse := func(status int, code, reason, msg string, dist float64) *proximityRefusal {
		proximityRefusals.WithLabelValues(step, reason).Inc()
		return &proximityRefusal{Status: status, Code: code, Msg: msg, DistanceM: dist, MaxM: radiusM}
	}
	pos, found, err := driverlocation.Get(ctx, rdb, driverID)
	if err != nil {
		log.Printf("trip proximity: reading location of driver=%s: %v", driverID, err)
		return refuse(http.StatusServiceUnavailable, "location_unavailable", "location_unavailable",
			"can't verify your location right now: try again", 0)
	}
	if !found {
		return refuse(http.StatusConflict, "driver_location_unknown", "location_unknown",
			"no location reported for this driver: keep the app open so your position is shared, then retry", 0)
	}
	if age := pos.Age(now); age > cfg.MaxLocationAge {
		return refuse(http.StatusConflict, "driver_location_stale", "location_stale",
			fmt.Sprintf("last reported location is %ds old (max %ds): wait for a fresh position, then retry", int(age.Seconds()), int(cfg.MaxLocationAge.Seconds())), 0)
	}
	if d := driverlocation.DistanceM(pos.Lat, pos.Lng, target.Lat, target.Lng); d > radiusM {
		return refuse(http.StatusConflict, "driver_too_far", "too_far",
			fmt.Sprintf("you are about %dm from the %s: get within %dm", int(d+0.5), step, int(radiusM)), d)
	}
	return nil
}
