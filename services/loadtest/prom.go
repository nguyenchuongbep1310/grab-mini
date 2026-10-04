package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// A sample is one line of Prometheus text exposition: a metric name, its
// labels, and a value.
type sample struct {
	name   string
	labels map[string]string
	value  float64
}

// scrape fetches and parses /metrics from url.
func scrape(client *http.Client, url string) ([]sample, error) {
	res, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return parseMetrics(res.Body)
}

func parseMetrics(r io.Reader) ([]sample, error) {
	var out []sample
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		s, ok := parseLine(line)
		if ok {
			out = append(out, s)
		}
	}
	return out, sc.Err()
}

func parseLine(line string) (sample, bool) {
	var name, rest string
	labels := map[string]string{}
	if i := strings.IndexByte(line, '{'); i >= 0 {
		j := strings.LastIndexByte(line, '}')
		if j < i {
			return sample{}, false
		}
		name, rest = line[:i], strings.TrimSpace(line[j+1:])
		for _, pair := range splitLabels(line[i+1 : j]) {
			k, v, ok := strings.Cut(pair, "=")
			if !ok {
				continue
			}
			labels[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"`)
		}
	} else {
		f := strings.Fields(line)
		if len(f) < 2 {
			return sample{}, false
		}
		name, rest = f[0], f[1]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return sample{}, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return sample{}, false
	}
	return sample{name, labels, v}, true
}

// splitLabels splits `a="x",b="y,z"` on the commas outside quotes.
func splitLabels(s string) []string {
	var parts []string
	depth, start := false, 0
	for i, c := range s {
		switch c {
		case '"':
			depth = !depth
		case ',':
			if !depth {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}

// sum adds up every sample of name whose labels include all of want.
func sum(samples []sample, name string, want map[string]string) float64 {
	var total float64
	for _, s := range samples {
		if s.name == name && matches(s.labels, want) {
			total += s.value
		}
	}
	return total
}

func matches(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// bucket is one cumulative histogram bucket.
type bucket struct {
	le    float64
	count float64
}

// buckets collects the cumulative buckets of histogram name (the _bucket
// series), summed over every series matching want, sorted by upper bound.
func buckets(samples []sample, name string, want map[string]string) []bucket {
	acc := map[float64]float64{}
	for _, s := range samples {
		if s.name != name+"_bucket" || !matches(s.labels, want) {
			continue
		}
		le := math.Inf(1)
		if v := s.labels["le"]; v != "+Inf" {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				continue
			}
			le = f
		}
		acc[le] += s.value
	}
	out := make([]bucket, 0, len(acc))
	for le, c := range acc {
		out = append(out, bucket{le, c})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].le < out[j].le })
	return out
}

// delta is the buckets of a histogram over a window: end minus start.
func delta(start, end []bucket) []bucket {
	before := map[float64]float64{}
	for _, b := range start {
		before[b.le] = b.count
	}
	out := make([]bucket, len(end))
	for i, b := range end {
		out[i] = bucket{b.le, b.count - before[b.le]}
	}
	return out
}

// quantile estimates the q-th quantile from cumulative buckets by linear
// interpolation inside the bucket the rank falls in, as Prometheus does.
func quantile(q float64, bs []bucket) (value float64, total float64) {
	if len(bs) == 0 {
		return math.NaN(), 0
	}
	total = bs[len(bs)-1].count
	if total == 0 {
		return math.NaN(), 0
	}
	rank := q * total
	prevLe, prevCount := 0.0, 0.0
	for _, b := range bs {
		if b.count >= rank {
			if math.IsInf(b.le, 1) {
				return prevLe, total // beyond the last finite bound: report that bound
			}
			if b.count == prevCount {
				return b.le, total
			}
			return prevLe + (b.le-prevLe)*(rank-prevCount)/(b.count-prevCount), total
		}
		prevLe, prevCount = b.le, b.count
	}
	return bs[len(bs)-1].le, total
}

func sec(v float64) string {
	if math.IsNaN(v) {
		return "n/a"
	}
	return fmt.Sprintf("%.0f ms", v*1000)
}
