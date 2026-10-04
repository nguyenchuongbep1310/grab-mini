package main

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestPercentilesUseTheNearestRank(t *testing.T) {
	r := newRecorder()
	for i := 1; i <= 100; i++ {
		r.add(time.Duration(i)*time.Millisecond, "200")
	}
	s := r.summarize()
	if s.Count != 100 || s.P50 != 50 || s.P95 != 95 || s.P99 != 99 || s.Max != 100 {
		t.Errorf("summary = %+v", s)
	}
	if s.Outcomes["200"] != 100 {
		t.Errorf("outcomes %v", s.Outcomes)
	}
}

func TestAnEmptyRecorderSummarizesToZeros(t *testing.T) {
	r := newRecorder()
	r.count("timeout")
	if s := r.summarize(); s.Count != 0 || s.P99 != 0 || s.Outcomes["timeout"] != 1 {
		t.Errorf("summary = %+v", s)
	}
}

const exposition = `# HELP http_requests_total x
# TYPE http_requests_total counter
http_requests_total{code="200",method="GET",route="GET /a"} 7
http_requests_total{code="500",method="GET",route="GET /a"} 2
kafka_consumer_messages_total{group="g",result="ok",topic="t,with,commas"} 5
process_cpu_seconds_total 12.5
matching_latency_seconds_bucket{le="1"} 10
matching_latency_seconds_bucket{le="3"} 20
matching_latency_seconds_bucket{le="+Inf"} 20
`

func TestParsingMetricsAndSummingBySelector(t *testing.T) {
	s, err := parseMetrics(strings.NewReader(exposition))
	if err != nil {
		t.Fatal(err)
	}
	if got := sum(s, "http_requests_total", nil); got != 9 {
		t.Errorf("all requests = %v, want 9", got)
	}
	if got := sum(s, "http_requests_total", map[string]string{"code": "500"}); got != 2 {
		t.Errorf("500s = %v, want 2", got)
	}
	if got := sum(s, "process_cpu_seconds_total", nil); got != 12.5 {
		t.Errorf("a metric with no labels = %v", got)
	}
	if got := sum(s, "kafka_consumer_messages_total", map[string]string{"topic": "t,with,commas"}); got != 5 {
		t.Errorf("a label value with commas = %v, want 5", got)
	}
}

func TestQuantilesInterpolateWithinABucketAsPrometheusDoes(t *testing.T) {
	s, _ := parseMetrics(strings.NewReader(exposition))
	bs := buckets(s, "matching_latency_seconds", nil)
	// 10 observations up to 1s, 10 more between 1s and 3s: the median is the 1s bound,
	// and the 75th percentile is halfway through the second bucket.
	if v, n := quantile(0.50, bs); math.Abs(v-1) > 1e-9 || n != 20 {
		t.Errorf("p50 = %v of %v, want 1 of 20", v, n)
	}
	if v, _ := quantile(0.75, bs); math.Abs(v-2) > 1e-9 {
		t.Errorf("p75 = %v, want 2", v)
	}
}

func TestADeltaIsTheWindowNotTheLifetime(t *testing.T) {
	start := []bucket{{1, 5}, {3, 5}, {math.Inf(1), 5}}
	end := []bucket{{1, 15}, {3, 25}, {math.Inf(1), 25}}
	d := delta(start, end)
	if v, n := quantile(0.5, d); n != 20 || math.Abs(v-1) > 1e-9 {
		t.Errorf("windowed p50 = %v of %v observations", v, n)
	}
}

func TestNoObservationsGiveNoQuantile(t *testing.T) {
	if v, n := quantile(0.95, nil); !math.IsNaN(v) || n != 0 {
		t.Errorf("quantile of nothing = %v, %v", v, n)
	}
	if v, _ := quantile(0.95, []bucket{{1, 0}, {math.Inf(1), 0}}); !math.IsNaN(v) {
		t.Errorf("quantile of an all-zero histogram = %v", v)
	}
}
