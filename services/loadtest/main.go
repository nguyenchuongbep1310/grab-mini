// Command loadtest drives a running grab-mini with realistic traffic and
// reports what it measured, from the client's side (how long each call took)
// and from the services' own /metrics (where the time went, what it cost).
//
// Two streams run at once:
//
//   - drivers: -drivers of them report a position through the Location Gateway
//     every -report-interval, each doing a small random walk;
//   - bookings: -orders-per-sec new orders a second, open loop (a slow system
//     doesn't slow the arrivals, which is what makes queues build). Each is
//     polled until matched, then the matched driver picks up and completes the
//     trip, and the rider usually rates it.
//
// It seeds the drivers into Redis the way matching-service seed-drivers does,
// so point it at a throwaway Redis. See deploy/loadtest/README.md.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"grab-mini/pkg/driverlocation"
	"grab-mini/pkg/driverstatus"
)

type config struct {
	AuthURL, OrderURL, TripURL, RatingURL, GatewayURL string
	RedisAddr                                         string
	Drivers, Riders                                   int
	ReportInterval                                    time.Duration
	OrdersPerSec                                      float64
	Duration, Drain                                   time.Duration
	TripTime                                          time.Duration
	RateFraction                                      float64
	MatchTimeout, PollInterval                        time.Duration
	Out                                               string
	// Where each service's /metrics lives, by name.
	Metrics map[string]string
}

func main() {
	var c config
	flag.StringVar(&c.AuthURL, "auth", "http://localhost:3005", "Auth Service")
	flag.StringVar(&c.OrderURL, "order", "http://localhost:3001", "Order Service")
	flag.StringVar(&c.TripURL, "trip", "http://localhost:3002", "Trip Service")
	flag.StringVar(&c.RatingURL, "rating", "http://localhost:3004", "Rating Service")
	flag.StringVar(&c.GatewayURL, "gateway", "http://localhost:3006", "Location Gateway")
	flag.StringVar(&c.RedisAddr, "redis", "localhost:6379", "Redis, where drivers are seeded")
	flag.IntVar(&c.Drivers, "drivers", 1000, "drivers reporting positions")
	flag.IntVar(&c.Riders, "riders", 500, "distinct rider ids to book with")
	flag.DurationVar(&c.ReportInterval, "report-interval", 4*time.Second, "how often each driver reports (the gateway allows 1s at best)")
	flag.Float64Var(&c.OrdersPerSec, "orders-per-sec", 10, "new orders per second (0: only location traffic)")
	flag.DurationVar(&c.Duration, "duration", 60*time.Second, "how long to generate load")
	flag.DurationVar(&c.Drain, "drain", 30*time.Second, "how long to wait afterwards for in-flight work and backlogs to clear")
	flag.DurationVar(&c.TripTime, "trip-time", 500*time.Millisecond, "time between pickup and complete")
	flag.Float64Var(&c.RateFraction, "rate-fraction", 0.8, "fraction of completed trips the rider rates")
	flag.DurationVar(&c.MatchTimeout, "match-timeout", 30*time.Second, "give up waiting for an order to be matched")
	flag.DurationVar(&c.PollInterval, "poll-interval", 100*time.Millisecond,
		"how often to ask whether an order is matched. A real rider app is pushed the answer, so polling is load the "+
			"system wouldn't have: at high rates raise this (and trust the server-side match latency) or the polling swamps the Order Service")
	flag.StringVar(&c.Out, "out", "", "also write the results as JSON to this file")
	metricsFlag := flag.String("metrics", "order=http://localhost:3001,trip=http://localhost:3002,rating=http://localhost:3004,notification=http://localhost:3003,gateway=http://localhost:3006,auth=http://localhost:3005,matching=http://localhost:9101,driver-location=http://localhost:9102",
		"name=url of each service's /metrics (base URL), comma separated")
	flag.Parse()
	c.Metrics = map[string]string{}
	for _, kv := range strings.Split(*metricsFlag, ",") {
		if name, url, ok := strings.Cut(kv, "="); ok {
			c.Metrics[name] = url
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, c); err != nil {
		log.Fatal(err)
	}
}

type world struct {
	c      config
	http   *http.Client
	tokens *tokens
	rng    *rand.Rand
	rngMu  sync.Mutex

	driverIDs []string
	driverPos []point

	rec map[string]*recorder
	// bookings still in flight, so the end can wait for them.
	inflight sync.WaitGroup
}

type point struct{ lat, lng float64 }

var recorders = []string{
	"gateway_report", "order_create", "create_to_matched", "trip_lookup", "pickup", "complete", "rate",
}

func (w *world) float() float64 {
	w.rngMu.Lock()
	defer w.rngMu.Unlock()
	return w.rng.Float64()
}

func (w *world) intn(n int) int {
	w.rngMu.Lock()
	defer w.rngMu.Unlock()
	return w.rng.Intn(n)
}

func run(ctx context.Context, c config) error {
	w := &world{
		c: c,
		http: &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{
			MaxIdleConns: 4000, MaxIdleConnsPerHost: 1000, IdleConnTimeout: 90 * time.Second,
		}},
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
		rec: map[string]*recorder{},
	}
	for _, name := range recorders {
		w.rec[name] = newRecorder()
	}
	w.tokens = newTokens(w.http, c.AuthURL)

	if err := w.seedDrivers(ctx); err != nil {
		return err
	}
	log.Printf("seeded %d drivers; generating load for %s (%d drivers reporting every %s, %.1f orders/s)",
		c.Drivers, c.Duration, c.Drivers, c.ReportInterval, c.OrdersPerSec)

	before := w.snapshot()
	started := time.Now()

	runCtx, cancel := context.WithTimeout(ctx, c.Duration)
	defer cancel()

	var gen sync.WaitGroup
	for i := range w.driverIDs {
		gen.Add(1)
		go func() { defer gen.Done(); w.reportLoop(runCtx, i) }()
	}
	if c.OrdersPerSec > 0 {
		gen.Add(1)
		go func() { defer gen.Done(); w.bookingLoop(runCtx) }()
	}
	sampler := newSampler(w)
	samplerDone := make(chan struct{})
	go func() { sampler.run(ctx); close(samplerDone) }()

	gen.Wait() // the generators stop at the end of the duration
	loadEnd := time.Now()

	// Let bookings in flight finish, and the location backlog drain.
	drained := make(chan struct{})
	go func() { w.inflight.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(c.Drain):
		log.Printf("some bookings were still in flight after the %s drain", c.Drain)
	case <-ctx.Done():
	}
	backlogClear := sampler.waitForBacklogClear(ctx, c.Drain)
	finished := time.Now()
	sampler.stop()
	<-samplerDone
	after := w.snapshot()

	rep := w.report(before, after, started, loadEnd, finished, sampler, backlogClear)
	rep.print(os.Stdout)
	if c.Out != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(c.Out, b, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// ------------------------------------------------------------------ seeding ----

// The region the drivers live in: central Ho Chi Minh City, a box about 13 km
// by 15 km, roughly the area a few thousand drivers would cover.
const (
	minLat, maxLat = 10.72, 10.84
	minLng, maxLng = 106.64, 106.78
)

func (w *world) seedDrivers(ctx context.Context) error {
	rdb := redis.NewClient(&redis.Options{Addr: w.c.RedisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis %s: %w", w.c.RedisAddr, err)
	}
	pipe := rdb.Pipeline()
	for i := 0; i < w.c.Drivers; i++ {
		id := fmt.Sprintf("lt-driver-%d", i)
		p := point{minLat + w.float()*(maxLat-minLat), minLng + w.float()*(maxLng-minLng)}
		w.driverIDs = append(w.driverIDs, id)
		w.driverPos = append(w.driverPos, p)
		vehicle := "CAR"
		if i%10 >= 7 {
			vehicle = "BIKE"
		}
		driverstatus.SetVehicle(ctx, rdb, id, vehicle)
		driverstatus.Set(ctx, rdb, id, driverstatus.Available)
		// timestamp 0: the first real report from the gateway takes over.
		driverlocation.Update(ctx, rdb, id, p.lat, p.lng, 0)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// ------------------------------------------------------------------ drivers ----

func (w *world) reportLoop(ctx context.Context, i int) {
	id := w.driverIDs[i]
	pos := w.driverPos[i]
	// Spread the first reports across the interval so they don't arrive as one burst.
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Duration(w.float() * float64(w.c.ReportInterval))):
	}
	tick := time.NewTicker(w.c.ReportInterval)
	defer tick.Stop()
	for {
		// A step of up to about 30 m, more than the gateway's 5 m "hasn't moved".
		pos.lat += (w.float()*2 - 1) * 0.0003
		pos.lng += (w.float()*2 - 1) * 0.0003
		w.sendReport(context.WithoutCancel(ctx), id, pos)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (w *world) sendReport(ctx context.Context, id string, p point) {
	body, _ := json.Marshal(map[string]any{"lat": p.lat, "lng": p.lng, "timestamp": time.Now().UTC().Format(time.RFC3339)})
	status, d, err := w.do(ctx, "driver", id, http.MethodPost, w.c.GatewayURL+"/locations", body, nil)
	w.rec["gateway_report"].add(d, outcome(status, err))
}

// --------------------------------------------------------------- bookings ----

func (w *world) bookingLoop(ctx context.Context) {
	interval := time.Duration(float64(time.Second) / w.c.OrdersPerSec)
	next := time.Now()
	for n := 0; ; n++ {
		next = next.Add(interval)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next)):
		}
		w.inflight.Add(1)
		go func() { defer w.inflight.Done(); w.bookOne(context.WithoutCancel(ctx)) }()
	}
}

func (w *world) bookOne(ctx context.Context) {
	rider := fmt.Sprintf("lt-rider-%d", w.intn(w.c.Riders))
	// Book near a real driver, so this measures matching, not a map with nobody on it.
	near := w.driverPos[w.intn(len(w.driverPos))]
	vehicle := "CAR"
	if w.float() < 0.3 {
		vehicle = "BIKE"
	}
	body, _ := json.Marshal(map[string]any{
		"pickup":      map[string]float64{"lat": near.lat + (w.float()*2-1)*0.004, "lng": near.lng + (w.float()*2-1)*0.004},
		"dropoff":     map[string]float64{"lat": near.lat + (w.float()*2-1)*0.03, "lng": near.lng + (w.float()*2-1)*0.03},
		"vehicleType": vehicle,
	})
	t0 := time.Now()
	var created struct {
		OrderID string `json:"orderId"`
	}
	status, d, err := w.do(ctx, "rider", rider, http.MethodPost, w.c.OrderURL+"/orders", body, &created)
	w.rec["order_create"].add(d, outcome(status, err))
	if status != http.StatusCreated || created.OrderID == "" {
		return
	}

	// Poll until matching has an answer. Polling every 100ms bounds how precisely
	// this can see the time to match; the server's own histogram has the exact figure.
	deadline := t0.Add(w.c.MatchTimeout)
	var final string
	for time.Now().Before(deadline) {
		var o struct {
			Status string `json:"status"`
		}
		st, _, err := w.do(ctx, "rider", rider, http.MethodGet, w.c.OrderURL+"/orders/"+created.OrderID, nil, &o)
		if err == nil && st == http.StatusOK && o.Status != "PENDING" {
			final = o.Status
			break
		}
		time.Sleep(w.c.PollInterval)
	}
	if final == "" {
		w.rec["create_to_matched"].add(time.Since(t0), "timeout")
		return
	}
	w.rec["create_to_matched"].add(time.Since(t0), final)
	if final != "MATCHED" {
		return
	}

	var trip struct {
		ID       string `json:"id"`
		DriverID string `json:"driverId"`
	}
	var tripStatus int
	var lookup time.Duration
	for try := 0; try < 20; try++ { // the trip is created a moment after order.matched
		tripStatus, lookup, err = w.do(ctx, "rider", rider, http.MethodGet, w.c.TripURL+"/trips/by-order/"+created.OrderID, nil, &trip)
		if tripStatus == http.StatusOK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	w.rec["trip_lookup"].add(lookup, outcome(tripStatus, err))
	if tripStatus != http.StatusOK {
		return
	}

	st, d, err := w.do(ctx, "driver", trip.DriverID, http.MethodPost, w.c.TripURL+"/trips/"+trip.ID+"/pickup", nil, nil)
	w.rec["pickup"].add(d, outcome(st, err))
	time.Sleep(w.c.TripTime)
	st, d, err = w.do(ctx, "driver", trip.DriverID, http.MethodPost, w.c.TripURL+"/trips/"+trip.ID+"/complete", nil, nil)
	w.rec["complete"].add(d, outcome(st, err))
	if st == http.StatusOK && w.float() < w.c.RateFraction {
		rating, _ := json.Marshal(map[string]any{"stars": 1 + w.intn(5), "comment": "load test"})
		st, d, err = w.do(ctx, "rider", rider, http.MethodPost, w.c.TripURL+"/trips/"+trip.ID+"/rating", rating, nil)
		w.rec["rate"].add(d, outcome(st, err))
	}
}

// -------------------------------------------------------------------- HTTP ----

func outcome(status int, err error) string {
	if err != nil {
		return "error"
	}
	return fmt.Sprint(status)
}

// do sends one request as role/id and decodes a JSON answer into out (if non-nil).
// It returns the status (0 on a transport error), how long it took, and any error.
func (w *world) do(ctx context.Context, role, id, method, url string, body []byte, out any) (int, time.Duration, error) {
	token, err := w.tokens.get(ctx, role, id)
	if err != nil {
		return 0, 0, err
	}
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	start := time.Now()
	res, err := w.http.Do(req)
	if err != nil {
		return 0, time.Since(start), err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	d := time.Since(start)
	if out != nil && res.StatusCode < 300 {
		json.Unmarshal(raw, out)
	}
	return res.StatusCode, d, nil
}

type tokens struct {
	client *http.Client
	url    string
	mu     sync.Mutex
	cache  map[string]string
}

func newTokens(c *http.Client, url string) *tokens {
	return &tokens{client: c, url: url, cache: map[string]string{}}
}

func (t *tokens) get(ctx context.Context, role, id string) (string, error) {
	key := role + "/" + id
	t.mu.Lock()
	tok, ok := t.cache[key]
	t.mu.Unlock()
	if ok {
		return tok, nil
	}
	body, _ := json.Marshal(map[string]string{"role": role, "id": id})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, t.url+"/token", bytes.NewReader(body))
	res, err := t.client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil || out.Token == "" {
		return "", fmt.Errorf("auth-service answered %d", res.StatusCode)
	}
	t.mu.Lock()
	t.cache[key] = out.Token
	t.mu.Unlock()
	return out.Token, nil
}

// ------------------------------------------------------- server-side metrics ----

type snapshot map[string][]sample // service name -> its metrics

func (w *world) snapshot() snapshot {
	snap := snapshot{}
	for name, base := range w.c.Metrics {
		s, err := scrape(w.http, base+"/metrics")
		if err != nil {
			log.Printf("cannot scrape %s: %v", name, err)
			continue
		}
		snap[name] = s
	}
	return snap
}

// sampler watches the location pipeline while the test runs: how many
// positions the gateway has published to Kafka against how many Driver Location
// Service has applied (the difference is the backlog in Kafka), and how old the
// oldest unpublished outbox event gets.
type sampler struct {
	w         *world
	mu        sync.Mutex
	maxLag    float64
	lagNow    float64
	maxOutbox float64
	samples   int
	cancel    context.CancelFunc
	stopped   atomic.Bool
}

func newSampler(w *world) *sampler { return &sampler{w: w} }

func (s *sampler) run(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		s.sample()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *sampler) stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *sampler) sample() {
	m := s.w.c.Metrics
	get := func(name string) []sample {
		if base, ok := m[name]; ok {
			if out, err := scrape(s.w.http, base+"/metrics"); err == nil {
				return out
			}
		}
		return nil
	}
	published := sum(get("gateway"), "location_reports_total", map[string]string{"result": "published"})
	dl := get("driver-location")
	applied := sum(dl, "location_updates_total", nil)
	lag := math.Max(0, published-applied)
	outbox := math.Max(sum(get("order"), "outbox_oldest_pending_age_seconds", nil), sum(get("trip"), "outbox_oldest_pending_age_seconds", nil))

	s.mu.Lock()
	s.lagNow = lag
	s.maxLag = math.Max(s.maxLag, lag)
	s.maxOutbox = math.Max(s.maxOutbox, outbox)
	s.samples++
	s.mu.Unlock()
}

// waitForBacklogClear returns how long after now the location backlog took to
// reach zero (or -1 if it did not within limit).
func (s *sampler) waitForBacklogClear(ctx context.Context, limit time.Duration) time.Duration {
	start := time.Now()
	for time.Since(start) < limit {
		s.sample()
		s.mu.Lock()
		lag := s.lagNow
		s.mu.Unlock()
		if lag <= 0 {
			return time.Since(start)
		}
		select {
		case <-ctx.Done():
			return -1
		case <-time.After(time.Second):
		}
	}
	return -1
}

// ------------------------------------------------------------------ report ----

type serverSide struct {
	CPUCores      map[string]float64 `json:"cpu_cores"`
	HandlerP95Ms  map[string]float64 `json:"consumer_handler_p95_ms"`
	MatchLatency  map[string]float64 `json:"match_latency_seconds"`
	HTTP5xx       float64            `json:"http_5xx"`
	HTTPTotal     float64            `json:"http_requests"`
	DeadLettered  float64            `json:"dead_lettered"`
	Retried       float64            `json:"retried"`
	PublishErrors float64            `json:"publish_errors"`
	Quarantined   float64            `json:"outbox_quarantined"`
	Locations     map[string]float64 `json:"location_reports"`
	Matching      map[string]float64 `json:"matching_outcomes"`
}

type report struct {
	Config        map[string]any     `json:"config"`
	LoadSeconds   float64            `json:"load_seconds"`
	TotalSeconds  float64            `json:"total_seconds"`
	Client        map[string]summary `json:"client"`
	Rates         map[string]float64 `json:"rates_per_sec"`
	Server        serverSide         `json:"server"`
	MaxLocLag     float64            `json:"max_location_backlog_messages"`
	BacklogClearS float64            `json:"location_backlog_cleared_after_s"`
	MaxOutboxAgeS float64            `json:"max_outbox_age_s"`
}

func (w *world) report(before, after snapshot, started, loadEnd, finished time.Time, s *sampler, backlogClear time.Duration) *report {
	load := loadEnd.Sub(started).Seconds()
	total := finished.Sub(started).Seconds()
	r := &report{
		Config: map[string]any{
			"drivers": w.c.Drivers, "report_interval": w.c.ReportInterval.String(), "orders_per_sec": w.c.OrdersPerSec,
			"duration": w.c.Duration.String(), "rate_fraction": w.c.RateFraction,
		},
		LoadSeconds: round1(load), TotalSeconds: round1(total),
		Client: map[string]summary{}, Rates: map[string]float64{},
	}
	for _, name := range recorders {
		sm := w.rec[name].summarize()
		r.Client[name] = sm
		if sm.Count > 0 {
			r.Rates[name] = round1(float64(sm.Count) / load)
		}
	}
	s.mu.Lock()
	r.MaxLocLag, r.MaxOutboxAgeS = s.maxLag, round1(s.maxOutbox)
	s.mu.Unlock()
	r.BacklogClearS = -1
	if backlogClear >= 0 {
		r.BacklogClearS = round1(backlogClear.Seconds())
	}

	sv := serverSide{
		CPUCores: map[string]float64{}, HandlerP95Ms: map[string]float64{}, MatchLatency: map[string]float64{},
		Locations: map[string]float64{}, Matching: map[string]float64{},
	}
	groups := map[string]bool{}
	for name, end := range after {
		start := before[name]
		cpu := sum(end, "process_cpu_seconds_total", nil) - sum(start, "process_cpu_seconds_total", nil)
		sv.CPUCores[name] = round2(cpu / total)
		sv.HTTPTotal += sum(end, "http_requests_total", nil) - sum(start, "http_requests_total", nil)
		sv.HTTP5xx += sumRe5xx(end) - sumRe5xx(start)
		sv.DeadLettered += sum(end, "kafka_consumer_messages_total", map[string]string{"result": "dead_lettered"}) - sum(start, "kafka_consumer_messages_total", map[string]string{"result": "dead_lettered"})
		sv.Retried += sum(end, "kafka_consumer_messages_total", map[string]string{"result": "retried"}) - sum(start, "kafka_consumer_messages_total", map[string]string{"result": "retried"})
		sv.PublishErrors += sum(end, "kafka_publish_errors_total", nil) - sum(start, "kafka_publish_errors_total", nil)
		sv.Quarantined += sum(end, "outbox_quarantined_total", nil) - sum(start, "outbox_quarantined_total", nil)
		for _, smp := range end {
			if smp.name == "kafka_consumer_handle_duration_seconds_count" {
				groups[smp.labels["group"]+"|"+name] = true
			}
		}
		for _, result := range []string{"published", "stationary", "too_fast", "invalid", "error"} {
			if v := sum(end, "location_reports_total", map[string]string{"result": result}) - sum(start, "location_reports_total", map[string]string{"result": result}); v != 0 {
				sv.Locations[result] = v
			}
		}
		for _, result := range []string{"matched", "no_driver"} {
			if v := sum(end, "matching_orders_total", map[string]string{"result": result}) - sum(start, "matching_orders_total", map[string]string{"result": result}); v != 0 {
				sv.Matching[result] = v
			}
		}
	}
	var keys []string
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		group, svc, _ := strings.Cut(k, "|")
		d := delta(buckets(before[svc], "kafka_consumer_handle_duration_seconds", map[string]string{"group": group}),
			buckets(after[svc], "kafka_consumer_handle_duration_seconds", map[string]string{"group": group}))
		if v, n := quantile(0.95, d); n > 0 && !math.IsNaN(v) {
			sv.HandlerP95Ms[group] = round1(v * 1000)
		}
	}
	d := delta(buckets(before["matching"], "matching_latency_seconds", nil), buckets(after["matching"], "matching_latency_seconds", nil))
	for _, q := range []float64{0.5, 0.95, 0.99} {
		if v, n := quantile(q, d); n > 0 && !math.IsNaN(v) {
			sv.MatchLatency[fmt.Sprintf("p%.0f", q*100)] = round2(v)
		}
	}
	r.Server = sv
	return r
}

func sumRe5xx(s []sample) float64 {
	var t float64
	for _, x := range s {
		if x.name == "http_requests_total" && strings.HasPrefix(x.labels["code"], "5") {
			t += x.value
		}
	}
	return t
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
func round2(v float64) float64 { return math.Round(v*100) / 100 }

func (r *report) print(out io.Writer) {
	fmt.Fprintf(out, "\n=== load test: %.0fs of load, %.0fs including the drain ===\n", r.LoadSeconds, r.TotalSeconds)
	fmt.Fprintf(out, "\nClient side (what a caller saw)\n")
	fmt.Fprintf(out, "  %-18s %8s %8s %9s %9s %9s %9s   %s\n", "operation", "count", "per sec", "p50 ms", "p95 ms", "p99 ms", "max ms", "outcomes")
	for _, name := range recorders {
		sm := r.Client[name]
		if sm.Count == 0 && len(sm.Outcomes) == 0 {
			continue
		}
		fmt.Fprintf(out, "  %-18s %8d %8.1f %9.1f %9.1f %9.1f %9.1f   %s\n", name, sm.Count, r.Rates[name], sm.P50, sm.P95, sm.P99, sm.Max, fmtOutcomes(sm.Outcomes))
	}
	fmt.Fprintf(out, "\nServer side (from /metrics)\n")
	fmt.Fprintf(out, "  match latency:        %s\n", fmtMap(r.Server.MatchLatency, "s"))
	fmt.Fprintf(out, "  matching outcomes:    %s\n", fmtMap(r.Server.Matching, ""))
	fmt.Fprintf(out, "  location reports:     %s\n", fmtMap(r.Server.Locations, ""))
	fmt.Fprintf(out, "  consumer handler p95: %s\n", fmtMap(r.Server.HandlerP95Ms, " ms"))
	fmt.Fprintf(out, "  CPU used (cores):     %s\n", fmtMap(r.Server.CPUCores, ""))
	fmt.Fprintf(out, "  http requests %.0f (5xx: %.0f)   retried %.0f   dead-lettered %.0f   publish errors %.0f   quarantined %.0f\n",
		r.Server.HTTPTotal, r.Server.HTTP5xx, r.Server.Retried, r.Server.DeadLettered, r.Server.PublishErrors, r.Server.Quarantined)
	fmt.Fprintf(out, "\nPipelines\n")
	fmt.Fprintf(out, "  location backlog in Kafka:  peaked at %.0f messages", r.MaxLocLag)
	if r.BacklogClearS >= 0 {
		fmt.Fprintf(out, ", cleared %.1fs after the load stopped\n", r.BacklogClearS)
	} else {
		fmt.Fprintf(out, ", NOT cleared within the drain\n")
	}
	fmt.Fprintf(out, "  oldest unpublished outbox event: peaked at %.1fs\n", r.MaxOutboxAgeS)
}

func fmtOutcomes(m map[string]int) string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, m[k]))
	}
	return strings.Join(parts, " ")
}

func fmtMap(m map[string]float64, unit string) string {
	if len(m) == 0 {
		return "n/a"
	}
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%g%s", k, m[k], unit))
	}
	return strings.Join(parts, "  ")
}
