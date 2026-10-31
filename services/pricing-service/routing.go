package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"grab-mini/pkg/surge"
	"grab-mini/pkg/tracing"
)

var (
	routingFailOpen = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pricing_routing_fail_open_total",
		Help: "Quotes priced on the straight-line distance because Routing Service did not give one, by reason (disabled, timeout, error, unsupported).",
	}, []string{"reason"})
	routingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pricing_routing_duration_seconds",
		Help:    "Time spent asking Routing Service for a distance, whatever the outcome.",
		Buckets: []float64{.005, .01, .025, .05, .1, .2, .3, .5},
	})
)

// Why a quote fell back to the straight line.
const (
	reasonDisabled    = "disabled"    // ROUTING_URL is empty, or the request carried no token to forward
	reasonTimeout     = "timeout"     // no answer within the timeout
	reasonError       = "error"       // connection error, a non-200 answer, an unreadable or implausible body
	reasonUnsupported = "unsupported" // 422: the trip is beyond what the model covers; not a fault
)

// errUnsupported is Routing Service saying the trip is out of its range.
var errUnsupported = errors.New("routing: trip beyond the supported distance")

// routingClient asks Routing Service how far a trip is by road. It forwards
// the caller's own bearer token: there is no service-to-service credential in
// this system, and Routing accepts a token of any role.
type routingClient struct {
	base    string
	timeout time.Duration
	hc      *http.Client
}

// newRoutingClient returns nil when base is empty: routing is then disabled
// and every quote uses the straight line.
func newRoutingClient(base string, timeout time.Duration) (*routingClient, error) {
	if base == "" {
		return nil, nil
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("ROUTING_URL %q is not an http(s) URL", base)
	}
	return &routingClient{
		base:    u.String(),
		timeout: timeout,
		hc:      &http.Client{Transport: tracing.Transport(nil)},
	}, nil
}

type routeResponse struct {
	DistanceKm float64 `json:"distanceKm"`
}

// distanceKm is the road distance of the trip. authorization is the caller's
// Authorization header, forwarded as is.
func (c *routingClient) distanceKm(ctx context.Context, authorization string, req surge.QuoteRequest) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	q := url.Values{}
	q.Set("fromLat", fmt.Sprint(req.PickupLat))
	q.Set("fromLng", fmt.Sprint(req.PickupLng))
	q.Set("toLat", fmt.Sprint(req.DropoffLat))
	q.Set("toLng", fmt.Sprint(req.DropoffLng))
	q.Set("vehicle", req.Vehicle)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/route?"+q.Encode(), nil)
	if err != nil {
		return 0, err
	}
	httpReq.Header.Set("Authorization", authorization)

	resp, err := c.hc.Do(httpReq)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnprocessableEntity:
		return 0, errUnsupported
	default:
		return 0, fmt.Errorf("routing answered %d", resp.StatusCode)
	}
	var out routeResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return 0, fmt.Errorf("routing answer: %w", err)
	}
	// Never price a ride on a nonsense distance: fall back instead.
	if math.IsNaN(out.DistanceKm) || math.IsInf(out.DistanceKm, 0) || out.DistanceKm < 0 {
		return 0, fmt.Errorf("routing answered a distance of %v km", out.DistanceKm)
	}
	return out.DistanceKm, nil
}

// failOpenReason classifies why distanceKm failed, for the metric.
func failOpenReason(err error) string {
	var netErr net.Error
	switch {
	case errors.Is(err, errUnsupported):
		return reasonUnsupported
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return reasonTimeout
	}
	return reasonError
}

// tripDistanceKm is the distance a quote is priced on: the road distance from
// Routing Service, or the straight line when routing is off or fails. It never
// returns an error and never takes longer than the routing timeout: a rider is
// not blocked, and not charged a made-up number, because Routing is down.
// Every fallback is counted in pricing_routing_fail_open_total.
func (s *quoteServer) tripDistanceKm(ctx context.Context, authorization string, req surge.QuoteRequest) float64 {
	straight := surge.HaversineKm(req.PickupLat, req.PickupLng, req.DropoffLat, req.DropoffLng)
	if s.routing == nil || authorization == "" {
		routingFailOpen.WithLabelValues(reasonDisabled).Inc()
		return straight
	}
	start := time.Now()
	km, err := s.routing.distanceKm(ctx, authorization, req)
	routingDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		reason := failOpenReason(err)
		routingFailOpen.WithLabelValues(reason).Inc()
		if reason != reasonUnsupported {
			log.Printf("routing distance unavailable, using the straight line (%s): %v", reason, err)
		}
		return straight
	}
	return km
}
