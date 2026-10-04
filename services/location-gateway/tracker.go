package main

import (
	"math"
	"sync"
	"time"
)

// decision is what to do with one location report.
type decision int

const (
	publish    decision = iota // send it on to Kafka
	tooFast                    // this driver reported again too soon: refuse it
	stationary                 // they haven't moved: accept it but don't publish
)

// tracker remembers, per driver, when they last reported and where, so the
// gateway can refuse a driver that reports too often and skip reports that
// carry nothing new. A moving driver reports every few seconds; at 100,000
// drivers, a parked one that still reports every 4 seconds is most of the
// traffic for no information.
type tracker struct {
	mu      sync.Mutex
	drivers map[string]*driverState

	minInterval time.Duration // fastest a driver may report
	minMove     float64       // metres: closer than this to the last published position is "not moved"
	heartbeat   time.Duration // but publish at least this often, so the position never goes stale
}

type driverState struct {
	lastReport time.Time // last accepted report, for the rate limit
	lastSent   time.Time // last report published
	lat, lng   float64   // where that was
	sent       bool      // whether anything was published yet
}

func newTracker(minInterval time.Duration, minMove float64, heartbeat time.Duration) *tracker {
	return &tracker{drivers: map[string]*driverState{}, minInterval: minInterval, minMove: minMove, heartbeat: heartbeat}
}

// decide classifies a report. A report it lets through (publish or
// stationary) counts against the rate limit; a refused one does not. For
// tooFast it also returns how long until the driver may report again.
func (t *tracker) decide(driverID string, lat, lng float64, now time.Time) (decision, time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	s, ok := t.drivers[driverID]
	if !ok {
		s = &driverState{}
		t.drivers[driverID] = s
	}
	if !s.lastReport.IsZero() {
		if wait := t.minInterval - now.Sub(s.lastReport); wait > 0 {
			return tooFast, wait
		}
	}
	s.lastReport = now

	if s.sent && distanceMetres(s.lat, s.lng, lat, lng) < t.minMove && now.Sub(s.lastSent) < t.heartbeat {
		return stationary, 0
	}
	return publish, 0
}

// sent records that a report was published, so later ones are compared with it.
// It is called only after Kafka acknowledged the message: if the publish fails
// the next report must not be taken for a duplicate.
func (t *tracker) sent(driverID string, lat, lng float64, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.drivers[driverID]; ok {
		s.lat, s.lng, s.lastSent, s.sent = lat, lng, now, true
	}
}

// refund undoes what the report that was just let through counted for, after a
// failed publish, so the driver can simply retry.
func (t *tracker) refund(driverID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.drivers[driverID]; ok {
		s.lastReport = time.Time{}
	}
}

// sweep forgets drivers silent for longer than idle, bounding the memory.
func (t *tracker) sweep(now time.Time, idle time.Duration) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for id, s := range t.drivers {
		if now.Sub(s.lastReport) > idle {
			delete(t.drivers, id)
			n++
		}
	}
	return n
}

func (t *tracker) size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.drivers)
}

// distanceMetres is the great-circle distance (haversine) between two points.
func distanceMetres(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadius = 6371000.0
	rad := func(d float64) float64 { return d * math.Pi / 180 }
	dLat, dLng := rad(lat2-lat1), rad(lng2-lng1)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(rad(lat1))*math.Cos(rad(lat2))*math.Sin(dLng/2)*math.Sin(dLng/2)
	return 2 * earthRadius * math.Asin(math.Min(1, math.Sqrt(a)))
}
