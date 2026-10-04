package main

import (
	"math"
	"sort"
	"sync"
	"time"
)

// recorder collects the durations of one kind of operation and counts how it
// ended (an HTTP status, or an outcome name).
type recorder struct {
	mu        sync.Mutex
	durations []time.Duration
	outcomes  map[string]int
}

func newRecorder() *recorder { return &recorder{outcomes: map[string]int{}} }

func (r *recorder) add(d time.Duration, outcome string) {
	r.mu.Lock()
	r.durations = append(r.durations, d)
	r.outcomes[outcome]++
	r.mu.Unlock()
}

// count records an outcome with no duration.
func (r *recorder) count(outcome string) {
	r.mu.Lock()
	r.outcomes[outcome]++
	r.mu.Unlock()
}

type summary struct {
	Count    int            `json:"count"`
	P50      float64        `json:"p50_ms"`
	P95      float64        `json:"p95_ms"`
	P99      float64        `json:"p99_ms"`
	Max      float64        `json:"max_ms"`
	Outcomes map[string]int `json:"outcomes"`
}

func (r *recorder) summarize() summary {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := append([]time.Duration(nil), r.durations...)
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	s := summary{Count: len(d), Outcomes: map[string]int{}}
	for k, v := range r.outcomes {
		s.Outcomes[k] = v
	}
	if len(d) == 0 {
		return s
	}
	s.P50, s.P95, s.P99 = ms(percentile(d, 0.50)), ms(percentile(d, 0.95)), ms(percentile(d, 0.99))
	s.Max = ms(d[len(d)-1])
	return s
}

// percentile of a sorted slice, nearest rank.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[min(max(rank, 0), len(sorted)-1)]
}

func ms(d time.Duration) float64 { return math.Round(float64(d)/float64(time.Millisecond)*10) / 10 }
