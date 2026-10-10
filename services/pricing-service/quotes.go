package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/auth"
	"grab-mini/pkg/events"
	"grab-mini/pkg/surge"
)

var (
	quotesIssued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pricing_quotes_total",
		Help: "Quotes issued, by vehicle type and whether surge applied.",
	}, []string{"vehicle_type", "surged"})
	failOpen = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pricing_fail_open_total",
		Help: "Quotes priced at 1.0 because the multiplier could not be read from Redis.",
	})
)

// readTimeout bounds the Redis read behind a quote: pricing must never make
// a rider wait on a struggling Redis.
const readTimeout = 300 * time.Millisecond

// currentMultiplier reads a cell's multiplier. A missing key is 1.0, and so
// is any Redis error (counted in pricing_fail_open_total): a booking is never
// blocked, and never over-charged, because Redis is down.
func currentMultiplier(ctx context.Context, rdb *redis.Client, vehicle, cell string) float64 {
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	v, err := rdb.Get(ctx, surge.MultKey(vehicle, cell)).Result()
	if err == redis.Nil {
		return 1.0
	}
	if err != nil {
		failOpen.Inc()
		log.Printf("surge multiplier read failed, using 1.0: %v", err)
		return 1.0
	}
	m, err := strconv.ParseFloat(v, 64)
	if err != nil || m < 1 || math.IsNaN(m) || math.IsInf(m, 0) {
		return 1.0
	}
	return m
}

type quoteServer struct {
	rdb    *redis.Client
	signer *surge.Signer
	env    Env
	now    func() time.Time
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func badRequest(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": msg})
}

func coord(q map[string][]string, name string, limit float64) (float64, bool) {
	vals := q[name]
	if len(vals) != 1 {
		return 0, false
	}
	f, err := strconv.ParseFloat(vals[0], 64)
	if err != nil || math.IsNaN(f) || math.Abs(f) > limit {
		return 0, false
	}
	return f, true
}

// handleQuote is GET /quotes?pickupLat&pickupLng&dropoffLat&dropoffLng&vehicleType
func (s *quoteServer) handleQuote(w http.ResponseWriter, r *http.Request) {
	caller, ok := auth.From(r.Context())
	if !ok {
		auth.Deny(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	q := r.URL.Query()
	var req surge.QuoteRequest
	var ok1, ok2, ok3, ok4 bool
	req.PickupLat, ok1 = coord(q, "pickupLat", 90)
	req.PickupLng, ok2 = coord(q, "pickupLng", 180)
	req.DropoffLat, ok3 = coord(q, "dropoffLat", 90)
	req.DropoffLng, ok4 = coord(q, "dropoffLng", 180)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		badRequest(w, "pickupLat, pickupLng, dropoffLat and dropoffLng are required numbers")
		return
	}
	vehicle := events.VehicleType(q.Get("vehicleType"))
	if vehicle == "" {
		vehicle = events.VehicleCar
	}
	if !vehicle.Valid() {
		badRequest(w, "vehicleType must be CAR or BIKE")
		return
	}
	req.Vehicle = string(vehicle)
	req.RiderID = caller.Subject

	cell := surge.CellID(s.env.CellDegrees, req.PickupLat, req.PickupLng)
	mult := currentMultiplier(r.Context(), s.rdb, req.Vehicle, cell)
	quote := s.signer.Issue(req, mult, s.now())

	surged := "false"
	if mult > 1 {
		surged = "true"
	}
	quotesIssued.WithLabelValues(req.Vehicle, surged).Inc()
	writeJSON(w, http.StatusOK, quote)
}

func (s *quoteServer) routes(verifier *auth.Verifier) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /quotes", verifier.Require(http.HandlerFunc(s.handleQuote), auth.Rider))
	return mux
}
