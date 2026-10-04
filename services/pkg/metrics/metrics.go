// Package metrics is the Prometheus side of observability: the /metrics
// endpoint, HTTP request metrics, and the few instruments several services
// share. Instruments that belong to one place (the Kafka consumer loop, the
// outbox relay, a service's own business figures) are declared next to the code
// they measure and register with the same default registry, so one /metrics
// endpoint serves them all.
//
// Label values must stay a small fixed set (a route pattern, a topic, a result),
// never an id: a label per order or driver would give Prometheus an unbounded
// number of series.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests served, by route pattern, method and status code.",
	}, []string{"route", "method", "code"})

	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "Time to serve an HTTP request, by route pattern.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"route", "method"})

	publishDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_publish_duration_seconds",
		Help:    "Time to publish a message to Kafka and get it acknowledged, by topic.",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"topic"})

	publishErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_publish_errors_total",
		Help: "Messages that could not be published to Kafka, by topic.",
	}, []string{"topic"})
)

// Handler serves the default registry (Go runtime, process, and every
// instrument registered by the packages linked into the binary).
func Handler() http.Handler { return promhttp.Handler() }

// ObservePublish records one publish attempt that began at start.
func ObservePublish(topic string, start time.Time, err error) {
	publishDuration.WithLabelValues(topic).Observe(time.Since(start).Seconds())
	if err != nil {
		publishErrors.WithLabelValues(topic).Inc()
	}
}

// statusWriter remembers the status code a handler wrote. It keeps the
// optional interfaces the wrapped writer has (WebSocket upgrades need Hijack,
// streaming needs Flush) via Unwrap, which http.ResponseController uses.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// HTTP records a request count and duration for every request h serves. The
// route label is the pattern the mux matched ("GET /orders/{id}"), not the
// URL, so ids don't multiply the series; a request no route matched is
// "unmatched". Health and metrics scrapes are left out: they say nothing about
// the service's traffic and a probe every few seconds would swamp it.
func HTTP(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		h.ServeHTTP(sw, r)

		route := r.Pattern // set by the mux on the request it was given
		if route == "" {
			route = "unmatched"
		}
		switch r.URL.Path {
		case "/metrics", "/livez", "/readyz", "/health":
			return
		}
		code := sw.status
		if code == 0 {
			code = http.StatusOK
		}
		// A hijacked connection (a WebSocket upgrade) reports 101 through the
		// upgrader writing to the raw connection, so no status is seen here.
		httpRequests.WithLabelValues(route, r.Method, strconv.Itoa(code)).Inc()
		httpDuration.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
	})
}
