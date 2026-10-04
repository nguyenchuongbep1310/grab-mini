package metrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func scrape(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

func mux() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) })
	m.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	m.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) {})
	return HTTP(m)
}

func call(h http.Handler, method, path string) {
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))
}

func TestRequestsAreCountedByRoutePatternNotByURL(t *testing.T) {
	h := mux()
	for _, id := range []string{"a", "b", "c"} {
		call(h, "GET", "/orders/"+id)
	}
	call(h, "POST", "/orders")
	out := scrape(t)

	if !strings.Contains(out, `http_requests_total{code="418",method="GET",route="GET /orders/{id}"} 3`) {
		t.Errorf("three ids should be one series with count 3:\n%s", grep(out, "http_requests_total"))
	}
	if !strings.Contains(out, `http_requests_total{code="200",method="POST",route="POST /orders"} 1`) {
		t.Errorf("a handler that never calls WriteHeader is a 200:\n%s", grep(out, "http_requests_total"))
	}
	if strings.Contains(out, `/orders/a`) {
		t.Error("an id leaked into a label")
	}
}

func TestUnmatchedRequestsShareOneLabel(t *testing.T) {
	h := mux()
	call(h, "GET", "/no/such/thing/1")
	call(h, "GET", "/no/such/thing/2")
	if out := scrape(t); !strings.Contains(out, `route="unmatched"`) || strings.Contains(out, "/no/such") {
		t.Errorf("unmatched requests:\n%s", grep(out, "http_requests_total"))
	}
}

func TestProbesAreNotCounted(t *testing.T) {
	call(mux(), "GET", "/livez")
	if out := scrape(t); strings.Contains(out, "livez") {
		t.Errorf("a liveness probe was counted:\n%s", grep(out, "livez"))
	}
}

func TestPublishMetrics(t *testing.T) {
	ObservePublish("order.created", time.Now().Add(-20*time.Millisecond), nil)
	ObservePublish("order.created", time.Now(), errors.New("broker down"))
	out := scrape(t)
	if !strings.Contains(out, `kafka_publish_errors_total{topic="order.created"} 1`) {
		t.Errorf("the failed publish wasn't counted:\n%s", grep(out, "kafka_publish"))
	}
	if !strings.Contains(out, `kafka_publish_duration_seconds_count{topic="order.created"} 2`) {
		t.Errorf("both attempts should be timed:\n%s", grep(out, "kafka_publish"))
	}
}

func grep(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) && !strings.HasPrefix(l, "#") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
