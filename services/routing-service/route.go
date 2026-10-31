package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
)

var (
	routesServed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "routing_routes_total",
		Help: "Routes computed, by vehicle type and whether the period was peak.",
	}, []string{"vehicle_type", "peak"})
	routeDistance = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "routing_route_distance_km",
		Help:    "Modelled distance of the routes computed.",
		Buckets: []float64{1, 2, 5, 10, 20, 50, 100, 200},
	})
	routesRefused = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "routing_refused_total",
		Help: "Route requests refused, by reason (invalid, too_far).",
	}, []string{"reason"})
)

var errTooFar = errors.New("route is longer than the model supports")

type routeServer struct {
	now func() time.Time
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func refuse(w http.ResponseWriter, code int, reason, msg string) {
	routesRefused.WithLabelValues(reason).Inc()
	writeJSON(w, code, map[string]string{"error": msg})
}

// coord reads one required, finite number within +-limit from the query.
func coord(q map[string][]string, name string, limit float64) (float64, bool) {
	vals := q[name]
	if len(vals) != 1 {
		return 0, false
	}
	f, err := strconv.ParseFloat(vals[0], 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || math.Abs(f) > limit {
		return 0, false
	}
	return f, true
}

// handleRoute is GET /route?fromLat&fromLng&toLat&toLng&vehicle=CAR|BIKE
//
// vehicle defaults to CAR when absent, as GET /quotes does. Any valid token
// may ask: a route is not personal data, and Pricing forwards the rider's own.
func (s *routeServer) handleRoute(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	fromLat, ok1 := coord(q, "fromLat", 90)
	fromLng, ok2 := coord(q, "fromLng", 180)
	toLat, ok3 := coord(q, "toLat", 90)
	toLng, ok4 := coord(q, "toLng", 180)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		refuse(w, http.StatusBadRequest, "invalid", "fromLat, fromLng, toLat and toLng are required numbers (latitude within 90, longitude within 180)")
		return
	}
	vehicle := events.VehicleType(q.Get("vehicle"))
	if vehicle == "" {
		vehicle = events.VehicleCar
	}
	if !vehicle.Valid() {
		refuse(w, http.StatusBadRequest, "invalid", "vehicle must be CAR or BIKE")
		return
	}

	now := s.now()
	route, err := Estimate(Point{fromLat, fromLng}, Point{toLat, toLng}, vehicle, now)
	if errors.Is(err, errTooFar) {
		refuse(w, http.StatusUnprocessableEntity, "too_far", fmt.Sprintf("the route is longer than %.0f km, which this model does not support", MaxRouteKm))
		return
	}
	routesServed.WithLabelValues(string(vehicle), strconv.FormatBool(isPeak(now))).Inc()
	routeDistance.Observe(route.DistanceKm)
	writeJSON(w, http.StatusOK, route)
}

func (s *routeServer) routes(verifier *auth.Verifier) *http.ServeMux {
	// Pricing asks once per quote and the Rider App when the pins move.
	limiter := auth.LimiterFromEnv(10, 30)
	mux := http.NewServeMux()
	mux.Handle("GET /route", verifier.Require(limiter.Wrap(http.HandlerFunc(s.handleRoute))))
	return mux
}
